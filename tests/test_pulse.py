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
