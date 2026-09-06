from __future__ import annotations

import json
from io import BytesIO

from ouroboros import userio_secretary_subscriber as subscriber


class _Response:
    def __init__(self, payload):
        self._payload = json.dumps(payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, *_args, **_kwargs):
        return self._payload


def test_active_task_id_uses_root_task_id_from_tasks_api(monkeypatch):
    payload = {
        "tasks": [
            {
                "root_task_id": "active-secretary-123",
                "status": "running",
                "metadata": {"source": "userio-mcp2"},
                "description": "Event-driven personal secretary ingest",
            }
        ]
    }
    monkeypatch.setattr(subscriber.urllib.request, "urlopen", lambda *_a, **_k: _Response(payload))

    assert subscriber.active_task_id() == "active-secretary-123"


def test_wake_coalesces_when_tasks_api_exposes_only_root_task_id(monkeypatch):
    payload = {
        "tasks": [
            {
                "root_task_id": "scheduled-secretary-456",
                "status": "running",
                "metadata": {"schedule_id": "secretary-wake"},
                "description": "Работай по personal_information_secretary.md",
            }
        ]
    }
    calls = []

    def fake_urlopen(req, *args, **kwargs):
        calls.append(getattr(req, "method", None) or "GET")
        if len(calls) > 1:
            raise AssertionError("wake must not POST while secretary work is active")
        return _Response(payload)

    monkeypatch.setattr(subscriber.urllib.request, "urlopen", fake_urlopen)

    assert subscriber.wake() == "scheduled-secretary-456"
    assert calls == ["GET"]


def test_checkpoint_deduplicates_processed_pending_and_inflight(tmp_path, monkeypatch):
    checkpoint = tmp_path / "checkpoint.json"
    monkeypatch.setattr(subscriber, "STATE_PATH", checkpoint)
    state = subscriber._default_state()
    state["processed_ids"] = ["old"]
    state["pending_ids"] = ["pending"]
    state["in_flight"] = {"task_id": "t1", "message_ids": ["running"]}
    subscriber.save_state(state)

    current = subscriber.load_state()
    assert subscriber.collect_new_ids(current, ["old", "pending", "running", "new-a", "new-b"]) == ["new-a", "new-b"]
    assert subscriber.enqueue_snapshot(current, ["old", "pending", "running", "new-a", "new-b"]) == ["new-a", "new-b"]
    saved = subscriber.load_state()
    assert saved["pending_ids"] == ["pending", "new-a", "new-b"]


def test_reconcile_completed_inflight_moves_ids_to_processed(tmp_path, monkeypatch):
    checkpoint = tmp_path / "checkpoint.json"
    monkeypatch.setattr(subscriber, "STATE_PATH", checkpoint)
    state = subscriber._default_state()
    state["in_flight"] = {"task_id": "done-1", "message_ids": ["m1", "m2"]}
    subscriber.save_state(state)
    monkeypatch.setattr(subscriber, "task_status", lambda _task_id: "completed")

    result = subscriber.reconcile_state(subscriber.load_state())
    assert result["in_flight"] is None
    assert result["processed_ids"] == ["m1", "m2"]


def test_reconcile_failed_inflight_returns_ids_to_pending(tmp_path, monkeypatch):
    checkpoint = tmp_path / "checkpoint.json"
    monkeypatch.setattr(subscriber, "STATE_PATH", checkpoint)
    state = subscriber._default_state()
    state["in_flight"] = {"task_id": "failed-1", "message_ids": ["m1"]}
    subscriber.save_state(state)
    monkeypatch.setattr(subscriber, "task_status", lambda _task_id: "failed")

    result = subscriber.reconcile_state(subscriber.load_state())
    assert result["in_flight"] is None
    assert result["pending_ids"] == ["m1"]


def test_secretary_prompt_treats_userio_audio_transcript_as_canonical():
    prompt = subscriber.BASE_PROMPT
    assert "UserIO normalizes Telegram voice/audio at ingress" in prompt
    assert "treat that transcript as the canonical user content" in prompt
    assert "Do not build STT infrastructure" in prompt
    assert "request Whisper/API secrets" in prompt


def test_secretary_prompt_keeps_messaging_on_userio_data_plane():
    from ouroboros.userio_secretary_subscriber import BASE_PROMPT
    assert "UserIO is the canonical messaging data plane" in BASE_PROMPT
    assert "Do not switch to direct Telegram/WhatsApp/provider MCPs" in BASE_PROMPT
    assert "Do not use shell/run_script/VCS/repository-editing tools" in BASE_PROMPT
    assert "Those capabilities remain available to other tasks" in BASE_PROMPT
