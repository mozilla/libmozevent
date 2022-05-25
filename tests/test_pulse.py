# -*- coding: utf-8 -*-

from libmozevent.pulse import PulseListener


def test_matching_routes():
    """
    Test the queue detection for a pulse message
    """
    config = {
        "queue_A": [("exchange/x", ["#"])],
        "queue_B": [("exchange/x", ["prefix.#"]), ("exchange/y", ["#-gecko-#"])],
    }
    pulse = PulseListener(config, "user", "password")

    # Simple test case
    assert pulse.find_matching_queues("exchange/x", [b"whatever"]) == {"queue_A"}

    # Bad exchange
    assert not pulse.find_matching_queues("exchange/YY", [b"whatever"])

    # get A and B
    assert pulse.find_matching_queues("exchange/x", [b"prefix.sometask.XYZ"]) == {
        "queue_A",
        "queue_B",
    }

    # Only gecko
    assert pulse.find_matching_queues("exchange/y", [b"something-gecko-123"]) == {
        "queue_B"
    }


def test_bugbug_routes():
    """
    Test the bugbug routes match our expected configuration
    """
    config = {
        "bugbug_firefox": [
            ("exchange/taskcluster-queue/v1/task-completed", ["#.gecko-level-1.#"]),
            ("exchange/taskcluster-queue/v1/task-failed", ["#.gecko-level-1.#"]),
        ],
        "bugbug_community": [
            (
                "exchange/taskcluster-queue/v1/task-completed",
                ["route.project.relman.bugbug.test_select"],
            )
        ],
    }
    pulse = PulseListener(config, "user", "password")

    assert pulse.find_matching_queues(
        "exchange/taskcluster-queue/v1/task-completed",
        [
            b"primary.fUAKaIdkSF6K1NlOgx7-LA.0.aws.i-0a45c84b1709af6a7.gecko-t.t-win10-64.gecko-level-1.RHY-YSgBQ7KlTAaQ5ZWP5g._",
            b"route.tc-treeherder.v2.try.028980a035fb3e214f7645675a01a52234aad0fe.455891",
        ],
    ) == {"bugbug_firefox"}

    assert pulse.find_matching_queues(
        "exchange/taskcluster-queue/v1/task-completed",
        [
            b"primary.OhtlizLqT9ah2jVkUL-yvg.0.community-tc-workers-google.8155538221748661937.proj-relman.compute-large.-.OhtlizLqT9ah2jVkUL-yvg._",
            b"route.notify.email.release-mgmt-analysis@mozilla.com.on-failed",
            b"route.notify.irc-channel.#bugbug.on-failed",
            b"route.index.project.relman.bugbug.test_select.latest",
            b"route.index.project.relman.bugbug.test_select.diff.196676",
            b"route.project.relman.bugbug.test_select",
        ],
    ) == {"bugbug_community"}
