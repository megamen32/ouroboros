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
