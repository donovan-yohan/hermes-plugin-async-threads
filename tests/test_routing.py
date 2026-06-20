from async_threads.routing import send_metadata_for_source
from gateway.config import Platform
from gateway.session import SessionSource


def _telegram_dm_topic_source() -> SessionSource:
    return SessionSource(
        platform=Platform.TELEGRAM,
        chat_id="12345",
        chat_type="dm",
        thread_id="42",
        user_id="67890",
        message_id="99",
    )


def _telegram_dm_topic_metadata(reply_anchor: str = "99") -> dict[str, object]:
    return {
        "thread_id": "42",
        "telegram_dm_topic_reply_fallback": True,
        "direct_messages_topic_id": "42",
        "telegram_reply_to_message_id": reply_anchor,
    }


def test_send_metadata_for_source_discord_thread_uses_generic_thread_id():
    source = SessionSource(
        platform=Platform.DISCORD,
        chat_id="c1",
        chat_type="channel",
        thread_id="t1",
        user_id="u1",
    )

    assert send_metadata_for_source(source) == {"thread_id": "t1"}


def test_send_metadata_for_source_without_thread_returns_none():
    source = SessionSource(platform=Platform.DISCORD, chat_id="c1", chat_type="channel", user_id="u1")

    assert send_metadata_for_source(source) is None


def test_send_metadata_for_source_slack_thread_uses_generic_thread_id():
    source = SessionSource(
        platform=Platform.SLACK,
        chat_id="C123",
        chat_type="channel",
        thread_id="1718910000.000100",
        user_id="U123",
    )

    assert send_metadata_for_source(source) == {"thread_id": "1718910000.000100"}


def test_send_metadata_for_source_telegram_dm_topic_uses_platform_aware_fields():
    assert send_metadata_for_source(_telegram_dm_topic_source()) == _telegram_dm_topic_metadata()


def test_send_metadata_for_source_telegram_dm_topic_can_use_explicit_reply_anchor():
    metadata = send_metadata_for_source(_telegram_dm_topic_source(), reply_to_message_id="100")

    assert metadata == _telegram_dm_topic_metadata("100")
