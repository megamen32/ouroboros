from pathlib import Path


RELAY = Path(__file__).resolve().parents[1] / "integrations" / "telegram-relay" / "relay.mjs"


def test_telegram_relay_uses_current_secretary_ssot_and_draft_only_reply_policy():
    source = RELAY.read_text(encoding="utf-8")
    assert "personal_information_secretary.md" in source
    assert "UserIO draft" in source
    assert "Никогда не отправляй Telegram-сообщение автоматически" in source
    assert "Секретарь · Правила ответов в Telegram" not in source
    assert "mcp_telegram__telegram-send-message" not in source
    assert "Ночью (22:00–09:00 МСК)" not in source
