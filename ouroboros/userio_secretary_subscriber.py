"""Long-lived MCP2 UserIO subscriber that wakes the secretary on new inbox data."""
from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

SETTINGS = Path(os.environ.get("OUROBOROS_SETTINGS", "/home/roomhacker/Ouroboros/data/settings.json"))
OUROBOROS_URL = os.environ.get("OUROBOROS_URL", "http://127.0.0.1:8765").rstrip("/")
URI = "userio://inbox/unread"
STATE_PATH = Path(os.environ.get("OUROBOROS_USERIO_CHECKPOINT", "/home/roomhacker/Ouroboros/data/state/userio_secretary_checkpoint.json"))
MAX_PROCESSED_IDS = int(os.environ.get("OUROBOROS_USERIO_MAX_PROCESSED_IDS", "5000"))
BASE_PROMPT = '''Event-driven personal secretary ingest. New messages are available in Universal UserIO. Primary working language: Russian.

Follow personal_information_secretary.md as the source of truth. Old cron-secretary/curfew/digest/mandate rules are deprecated.
1. Read only the newly queued UserIO message IDs listed in this task, plus the minimum surrounding conversation/AFFiNE context needed to understand them.
2. UserIO normalizes Telegram voice/audio at ingress. When an audio or voice message has a transcript in its body/attachment metadata, treat that transcript as the canonical user content. Do not build STT infrastructure, create transcription skills, or request Whisper/API secrets from the owner during secretary work. If transcription is explicitly unavailable, preserve the audio reference and continue unless the owner specifically asked for transcription.
3. Prioritize owner's DMs, «ИИ Frontier», «ИИ бенчмарки», Artem Popov and Oleg Karpov when relevant, then other conversations.
4. Create/update useful AFFiNE artifacts: people profiles, meetings, projects, agreements, ideas and links. Preserve provenance and distinguish source facts from conclusions.
5. Create/update todo cards for explicit commitments, next steps, deadlines and reminders; deduplicate.
6. If a useful reply should be prepared, create/update a UserIO draft. Never approve/send automatically.
7. Direct AFFiNE create/update tools are allowed by the owner's current MCP policy. Do not rewrite MCP allowlists or schedules from this subscriber.
8. Prefer useful organization over infrastructure self-checks or self-reflection. If nothing useful remains after resolving the listed IDs, finish quietly.
'''


def cfg() -> tuple[str, str, str]:
    data = json.loads(SETTINGS.read_text())
    server = next(item for item in data.get("MCP_SERVERS", []) if item.get("id") == "userio")
    return server["url"], server.get("auth_header") or "Authorization", server.get("auth_token") or ""


def rpc(url: str, header: str, token: str, rid: int, method: str, params: dict[str, Any]) -> dict[str, Any]:
    req = urllib.request.Request(
        url,
        data=json.dumps({"jsonrpc": "2.0", "id": rid, "method": method, "params": params}).encode(),
        headers={header: token, "Content-Type": "application/json", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=15) as response:
        return json.load(response)


def _default_state() -> dict[str, Any]:
    return {"version": 1, "processed_ids": [], "pending_ids": [], "in_flight": None, "baseline_hash": "", "updated_at": ""}


def load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return _default_state()
    try:
        data = json.loads(STATE_PATH.read_text())
    except Exception:
        return _default_state()
    state = _default_state()
    state.update(data if isinstance(data, dict) else {})
    state["processed_ids"] = [str(x) for x in state.get("processed_ids") or [] if str(x)]
    state["pending_ids"] = [str(x) for x in state.get("pending_ids") or [] if str(x)]
    return state


def save_state(state: dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    state = dict(state)
    state["processed_ids"] = list(dict.fromkeys(state.get("processed_ids") or []))[-MAX_PROCESSED_IDS:]
    state["pending_ids"] = list(dict.fromkeys(state.get("pending_ids") or []))
    state["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    tmp = STATE_PATH.with_suffix(STATE_PATH.suffix + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n")
    os.chmod(tmp, 0o600)
    os.replace(tmp, STATE_PATH)


def unread_message_ids(url: str, header: str, token: str) -> list[str]:
    response = rpc(url, header, token, 11, "resources/read", {"uri": URI})
    contents = (response.get("result") or {}).get("contents") or []
    payload = json.loads((contents[0] if contents else {}).get("text") or "{}")
    ids: list[str] = []
    for message in payload.get("messages") or []:
        message_id = str(message.get("message_id") or message.get("id") or "").strip()
        if message_id:
            ids.append(message_id)
    return list(dict.fromkeys(ids))


def _snapshot_hash(ids: list[str]) -> str:
    return hashlib.sha256("\n".join(ids).encode()).hexdigest()


def active_task_id() -> str:
    req = urllib.request.Request(OUROBOROS_URL + "/api/tasks")
    with urllib.request.urlopen(req, timeout=15) as response:
        data = json.load(response)
    for task in data.get("tasks", []):
        if task.get("status") not in {"scheduled", "running", "queued"}:
            continue
        meta = task.get("metadata") or {}
        purpose = str(meta.get("purpose") or "")
        schedule_id = str(meta.get("schedule_id") or task.get("schedule_id") or "")
        source = str(meta.get("source") or task.get("source") or "")
        description = str(task.get("description") or "").lower()
        if source == "userio-mcp2" or schedule_id == "secretary-wake" or "personal_information_secretary" in purpose or "секретар" in description:
            return str(task.get("task_id") or task.get("root_task_id") or "")
    return ""


def task_status(task_id: str) -> str:
    if not task_id:
        return ""
    req = urllib.request.Request(OUROBOROS_URL + "/api/tasks")
    with urllib.request.urlopen(req, timeout=15) as response:
        data = json.load(response)
    for task in data.get("tasks", []):
        candidate = str(task.get("task_id") or task.get("root_task_id") or "")
        root = str(task.get("root_task_id") or "")
        if task_id in {candidate, root}:
            return str(task.get("status") or "")
    return "missing"


def reconcile_state(state: dict[str, Any]) -> dict[str, Any]:
    in_flight = state.get("in_flight") if isinstance(state.get("in_flight"), dict) else None
    if not in_flight:
        return state
    status = task_status(str(in_flight.get("task_id") or ""))
    ids = [str(x) for x in in_flight.get("message_ids") or [] if str(x)]
    if status in {"completed", "done", "succeeded"}:
        state["processed_ids"] = list(dict.fromkeys((state.get("processed_ids") or []) + ids))[-MAX_PROCESSED_IDS:]
        state["in_flight"] = None
        save_state(state)
    elif status in {"failed", "cancelled", "missing"}:
        state["pending_ids"] = list(dict.fromkeys(ids + (state.get("pending_ids") or [])))
        state["in_flight"] = None
        save_state(state)
    return state


def bootstrap_state(url: str, header: str, token: str) -> dict[str, Any]:
    state = load_state()
    if state.get("processed_ids") or state.get("pending_ids") or state.get("in_flight") or state.get("baseline_hash"):
        return state
    ids = unread_message_ids(url, header, token)
    state["processed_ids"] = ids[-MAX_PROCESSED_IDS:]
    state["baseline_hash"] = _snapshot_hash(ids)
    save_state(state)
    print(f"userio checkpoint baseline initialized with {len(ids)} unread id(s)", flush=True)
    return state


def collect_new_ids(state: dict[str, Any], unread_ids: list[str]) -> list[str]:
    processed = set(state.get("processed_ids") or [])
    pending = set(state.get("pending_ids") or [])
    in_flight = state.get("in_flight") if isinstance(state.get("in_flight"), dict) else {}
    active = set(in_flight.get("message_ids") or [])
    return [mid for mid in unread_ids if mid not in processed and mid not in pending and mid not in active]


def enqueue_snapshot(state: dict[str, Any], unread_ids: list[str]) -> list[str]:
    new_ids = collect_new_ids(state, unread_ids)
    if new_ids:
        state["pending_ids"] = list(dict.fromkeys((state.get("pending_ids") or []) + new_ids))
    state["baseline_hash"] = _snapshot_hash(unread_ids)
    save_state(state)
    return new_ids


def wake(message_ids: list[str] | None = None) -> str:
    existing = active_task_id()
    if existing:
        print(f"userio event coalesced into active task {existing}", flush=True)
        return existing
    message_ids = list(message_ids or [])
    listed = "\n".join(f"- {mid}" for mid in message_ids)
    prompt = BASE_PROMPT + f"\nNew UserIO message IDs for this cycle ({len(message_ids)}):\n{listed}\n"
    body = {
        "description": prompt,
        "source": "userio-mcp2",
        "metadata": {"source": "userio-mcp2", "event": "resources/updated", "uri": URI, "message_ids": message_ids},
        "memory_mode": "shared",
    }
    req = urllib.request.Request(
        OUROBOROS_URL + "/api/tasks",
        data=json.dumps(body, ensure_ascii=False).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as response:
        payload = json.load(response)
    return str(payload.get("task_id") or payload.get("root_task_id") or "")


def dispatch_pending(state: dict[str, Any]) -> str:
    if state.get("in_flight"):
        return ""
    pending = [str(x) for x in state.get("pending_ids") or [] if str(x)]
    if not pending:
        return ""
    if active_task_id():
        return ""
    task_id = wake(pending)
    if not task_id:
        return ""
    state["pending_ids"] = []
    state["in_flight"] = {"task_id": task_id, "message_ids": pending, "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    save_state(state)
    return task_id


def stream_once(url: str, header: str, token: str) -> bool:
    rpc(url, header, token, 1, "initialize", {"protocolVersion": "2026-07-28", "capabilities": {}, "clientInfo": {"name": "ouroboros-secretary-subscriber", "version": "2"}})
    rpc(url, header, token, 2, "resources/subscribe", {"uri": URI})
    print("userio subscribed " + URI, flush=True)
    req = urllib.request.Request(url, headers={header: token, "Accept": "text/event-stream"})
    with urllib.request.urlopen(req, timeout=45) as response:
        data: list[str] = []
        while True:
            raw = response.readline()
            if not raw:
                break
            line = raw.decode(errors="replace").rstrip("\r\n")
            if line.startswith("data:"):
                data.append(line[5:].strip())
            elif not line and data:
                try:
                    event = json.loads("\n".join(data))
                except Exception:
                    event = {}
                data = []
                method = event.get("method")
                if method and method != "userio/ready":
                    print("userio event " + str(method), flush=True)
                if method == "notifications/resources/updated" and (event.get("params") or {}).get("uri") == URI:
                    return True
    return False


def process_update(url: str, header: str, token: str) -> tuple[list[str], str]:
    state = reconcile_state(load_state())
    unread_ids = unread_message_ids(url, header, token)
    new_ids = enqueue_snapshot(state, unread_ids)
    state = load_state()
    task_id = dispatch_pending(state)
    return new_ids, task_id


def main() -> None:
    while True:
        try:
            url, header, token = cfg()
            state = bootstrap_state(url, header, token)
            state = reconcile_state(state)
            dispatch_pending(state)
            if stream_once(url, header, token):
                new_ids, task_id = process_update(url, header, token)
                if new_ids:
                    print(f"userio resource update: queued {len(new_ids)} new id(s)" + (f" -> task {task_id}" if task_id else ""), flush=True)
                else:
                    print("userio resource update: duplicate snapshot ignored", flush=True)
        except (OSError, ValueError, KeyError, urllib.error.URLError, urllib.error.HTTPError) as error:
            print(f"userio subscriber reconnect: {type(error).__name__}: {error}", flush=True)
            time.sleep(3)


if __name__ == "__main__":
    main()
