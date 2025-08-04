# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import collections
from configparser import ConfigParser
import json
import os.path
import tempfile
import urllib.parse
from contextlib import contextmanager
from datetime import datetime, timedelta

import pytest
import responses
from taskcluster.helper import TaskclusterConfig
from taskcluster.utils import stringDate

from libmozevent.phabricator import (
    PhabricatorActions,
    PhabricatorBuild,
    PhabricatorBuildState,
)

MOCK_DIR = os.path.join(os.path.dirname(__file__), "mocks")


@pytest.fixture
def QueueMock():
    class Mock:
        def __init__(self):
            self.created_tasks = []
            self.groups = collections.defaultdict(list)

        def add_task_in_group(self, group_id, **task):
            self.groups[group_id].append(task)

        async def status(self, task_id):
            for status in ["failed", "completed", "exception", "pending"]:
                if status in task_id:
                    return {"status": {"state": status}}
            assert False

        async def task(self, task_id):
            now = datetime.utcnow()

            if "retry:" in task_id:
                retry = int(task_id[task_id.index("retry:") + 6])
            else:
                retry = 3

            return {
                "created": stringDate(now),
                "deadline": stringDate(now + timedelta(hours=2)),
                "dependencies": [],
                "expires": stringDate(now + timedelta(hours=24)),
                "payload": {
                    "command": ["/bin/command"],
                    "env": {},
                    "image": "alpine",
                    "maxRunTime": 3600,
                },
                "priority": "lowest",
                "provisionerId": "aws-provisioner-v1",
                "requires": "all-completed",
                "retries": retry,
                "scopes": [],
                "routes": ["index.{}.latest".format(task_id)],
                "taskGroupId": "group-{}".format(task_id),
                "workerType": "niceWorker",
            }

        async def createTask(self, task_id, payload):
            self.created_tasks.append((task_id, payload))

    return Mock()


@pytest.fixture
def NotifyMock():
    class Mock:
        def __init__(self):
            self.email_obj = {}

        async def email(self, obj):
            self.email_obj.update(obj)

    return Mock()


@pytest.fixture
def HooksMock():
    class Mock:
        def __init__(self):
            self.obj = {}

        def triggerHook(self, group_id, hook_id, payload):
            self.obj = {"group_id": group_id, "hook_id": hook_id, "payload": payload}
            return {"status": {"taskId": "fake_task_id"}}

    return Mock()


@pytest.fixture
@contextmanager
def PhabricatorMock():
    """
    Mock phabricator authentication process
    """
    json_headers = {"Content-Type": "application/json"}

    def _response(name):
        path = os.path.join(MOCK_DIR, "phabricator", "{}.json".format(name))
        assert os.path.exists(path), "Missing mock {}".format(path)
        return open(path).read()

    def _phab_params(request):
        # What a weird way to send parameters
        return json.loads(urllib.parse.parse_qs(request.body)["params"][0])

    def _diff_search(request):
        params = _phab_params(request)
        assert "constraints" in params
        if "revisionPHIDs" in params["constraints"]:
            # Search from revision
            mock_name = "search-{}".format(params["constraints"]["revisionPHIDs"][0])
        elif "phids" in params["constraints"]:
            # Search from diffs
            diffs = "-".join(params["constraints"]["phids"])
            mock_name = "search-{}".format(diffs)
        else:
            raise Exception("Unsupported diff mock {}".format(params))
        return (200, json_headers, _response(mock_name))

    def _diff_raw(request):
        params = _phab_params(request)
        assert "diffID" in params
        return (200, json_headers, _response("raw-{}".format(params["diffID"])))

    def _edges(request):
        params = _phab_params(request)
        assert "sourcePHIDs" in params
        return (
            200,
            json_headers,
            _response("edges-{}".format(params["sourcePHIDs"][0])),
        )

    def _create_artifact(request):
        params = _phab_params(request)
        assert "buildTargetPHID" in params
        return (
            200,
            json_headers,
            _response("artifact-{}".format(params["buildTargetPHID"])),
        )

    def _send_message(request):
        params = _phab_params(request)
        assert "buildTargetPHID" in params
        name = "message-{}-{}".format(params["buildTargetPHID"], params["type"])
        if params["unit"]:
            name += "-unit"
        if params["lint"]:
            name += "-lint"
        return (200, json_headers, _response(name))

    def _project_search(request):
        params = _phab_params(request)
        assert "constraints" in params
        assert "slugs" in params["constraints"]
        return (200, json_headers, _response("projects"))

    def _revision_search(request):
        params = _phab_params(request)
        assert "constraints" in params
        assert "ids" in params["constraints"]
        assert "attachments" in params
        assert "projects" in params["attachments"]
        assert "reviewers" in params["attachments"]
        assert params["attachments"]["projects"]
        assert params["attachments"]["reviewers"]
        mock_name = "revision-search-{}".format(params["constraints"]["ids"][0])
        return (200, json_headers, _response(mock_name))

    def _user_search(request):
        params = _phab_params(request)
        assert "constraints" in params
        assert "phids" in params["constraints"]
        mock_name = "user-search-{}".format(params["constraints"]["phids"][0])
        return (200, json_headers, _response(mock_name))

    with responses.RequestsMock(assert_all_requests_are_fired=False) as resp:
        resp.add(
            responses.POST,
            "http://phabricator.test/api/user.whoami",
            body=_response("auth"),
            content_type="application/json",
        )

        resp.add_callback(
            responses.POST, "http://phabricator.test/api/edge.search", callback=_edges
        )

        resp.add_callback(
            responses.POST,
            "http://phabricator.test/api/differential.diff.search",
            callback=_diff_search,
        )

        resp.add_callback(
            responses.POST,
            "http://phabricator.test/api/differential.getrawdiff",
            callback=_diff_raw,
        )

        resp.add_callback(
            responses.POST,
            "http://phabricator.test/api/harbormaster.createartifact",
            callback=_create_artifact,
        )

        resp.add_callback(
            responses.POST,
            "http://phabricator.test/api/harbormaster.sendmessage",
            callback=_send_message,
        )

        resp.add(
            responses.POST,
            "http://phabricator.test/api/diffusion.repository.search",
            body=_response("repositories"),
            content_type="application/json",
        )

        resp.add_callback(
            responses.POST,
            "http://phabricator.test/api/project.search",
            callback=_project_search,
        )

        resp.add_callback(
            responses.POST,
            "http://phabricator.test/api/differential.revision.search",
            callback=_revision_search,
        )

        resp.add_callback(
            responses.POST,
            "http://phabricator.test/api/user.search",
            callback=_user_search,
        )

        actions = PhabricatorActions(
            url="http://phabricator.test/api/", api_key="deadbeef"
        )
        actions.mocks = resp  # used to assert in tests on callbacks
        yield actions


@pytest.fixture
def mock_taskcluster():
    """
    Mock Tasklcuster authentication
    """
    tc = TaskclusterConfig("http://taskcluster.test")

    # Force options to avoid auto proxy detection
    tc.auth("client", "token")
    tc.default_url = "http://taskcluster.test"
    tc.options["maxRetries"] = 1
    return tc


class MockBuild(PhabricatorBuild):
    def __init__(self, diff_id, repo_phid, revision_id, target_phid, diff):
        config_file = tempfile.NamedTemporaryFile()
        with open(config_file.name, "w") as f:
            custom_conf = ConfigParser()
            custom_conf.add_section("User-Agent")
            custom_conf.set("User-Agent", "name", "libmozdata")
            custom_conf.write(f)
            f.seek(0)
        from libmozdata import config

        config.set_config(config.ConfigIni(config_file.name))

        self.diff_id = diff_id
        self.repo_phid = repo_phid
        self.revision_id = revision_id
        self.target_phid = target_phid
        self.diff = diff
        self.stack = []
        self.state = PhabricatorBuildState.Public
        self.revision_url = None
        self.retries = 0
