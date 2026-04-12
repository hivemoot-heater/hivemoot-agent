"""Tests for MessagingTrigger allowlist behavior.

Regression coverage for the allowlist inversion bug where an unset
MESSAGING_ALLOWED_CHAT_IDS caused all inbound chats to be denied instead of
allowed.
"""

import os
import sys
import threading
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from hivemoot_agent.plugins.interfaces import Job, PluginConfig
from hivemoot_agent.plugins_builtin.messaging.trigger import MessagingTrigger


# ── Helpers ───────────────────────────────────────────────────────


class _FakeAdapter:
    """Adapter that yields one batch of messages then stops the trigger."""

    def __init__(self, messages: list, stop_event: threading.Event) -> None:
        self._messages = messages
        self._stop_event = stop_event
        self._called = False

    def poll(self, config, offset, timeout):
        if not self._called:
            self._called = True
            self._stop_event.set()  # stop after first poll
            return self._messages
        return []


class _RecordingDispatcher:
    """Dispatcher that records every dispatched job."""

    def __init__(self) -> None:
        self.jobs: list[Job] = []

    def dispatch(self, job: Job) -> bool:
        self.jobs.append(job)
        return True


def _make_msg(chat_id: str, text: str = "hello", update_id: int = 1) -> dict:
    return {"chat_id": chat_id, "text": text, "update_id": update_id}


def _run_trigger(messages: list, allowed_ids: str = "") -> list[Job]:
    """Run the trigger against a message batch and return dispatched jobs."""
    stop_event = threading.Event()
    adapter = _FakeAdapter(messages, stop_event)

    fake_plugin = MagicMock()
    fake_plugin.get_adapter.return_value = adapter

    trigger = MessagingTrigger(fake_plugin)
    trigger._stop_event = stop_event

    config: PluginConfig = {
        "MESSAGING_AGENT_ID": "bot",
        "TELEGRAM_POLL_TIMEOUT_SECS": "1",
    }
    if allowed_ids:
        config["MESSAGING_ALLOWED_CHAT_IDS"] = allowed_ids

    dispatcher = _RecordingDispatcher()
    trigger.start(config, dispatcher)
    return dispatcher.jobs


# ── Allowlist tests ───────────────────────────────────────────────


def test_no_allowlist_dispatches_all_chats():
    """Regression: unset MESSAGING_ALLOWED_CHAT_IDS must allow all chats.

    Before the fix, an empty allowed set caused `not allowed` to be True and
    every message was denied.
    """
    jobs = _run_trigger([_make_msg("111"), _make_msg("222", update_id=2)])
    dispatched_keys = {j.session_key for j in jobs}
    assert "tg:111" in dispatched_keys, "chat 111 must be dispatched with no allowlist"
    assert "tg:222" in dispatched_keys, "chat 222 must be dispatched with no allowlist"


def test_allowlist_passes_listed_chat():
    """A chat_id that appears in MESSAGING_ALLOWED_CHAT_IDS is dispatched."""
    jobs = _run_trigger([_make_msg("111")], allowed_ids="111,222")
    assert any(j.session_key == "tg:111" for j in jobs), "allowed chat must be dispatched"


def test_allowlist_denies_unlisted_chat():
    """A chat_id absent from MESSAGING_ALLOWED_CHAT_IDS is denied."""
    jobs = _run_trigger([_make_msg("999")], allowed_ids="111,222")
    assert not any(j.session_key == "tg:999" for j in jobs), "unlisted chat must be denied"


def test_allowlist_mixed_batch():
    """Only allowed chats are dispatched when allowlist is set."""
    messages = [
        _make_msg("111", update_id=1),
        _make_msg("999", update_id=2),  # not in allowlist
        _make_msg("222", update_id=3),
    ]
    jobs = _run_trigger(messages, allowed_ids="111,222")
    dispatched_keys = {j.session_key for j in jobs}
    assert "tg:111" in dispatched_keys
    assert "tg:222" in dispatched_keys
    assert "tg:999" not in dispatched_keys, "unlisted chat 999 must be denied"


if __name__ == "__main__":
    import inspect

    passed = 0
    failed = 0
    for name, func in sorted(
        inspect.getmembers(sys.modules[__name__], inspect.isfunction)
    ):
        if not name.startswith("test_"):
            continue
        try:
            func()
            print(f"  \u2713 {name}")
            passed += 1
        except Exception as exc:
            print(f"  \u2717 {name}: {exc}")
            failed += 1

    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
