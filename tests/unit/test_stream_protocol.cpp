#include "stream_protocol.h"
#include "test_support.h"

#include <cstring>
#include <string>

namespace {

using stream_protocol::ServerMessageType;

void test_server_message_classification() {
    CHECK(stream_protocol::classify_server_message("setupComplete") == ServerMessageType::SetupComplete);
    CHECK(stream_protocol::classify_server_message("serverContent") == ServerMessageType::ServerContent);
    CHECK(stream_protocol::classify_server_message("toolCall") == ServerMessageType::ToolCall);
    CHECK(stream_protocol::classify_server_message("toolCallCancellation") == ServerMessageType::ToolCallCancellation);
    CHECK(stream_protocol::classify_server_message("goAway") == ServerMessageType::GoAway);
    CHECK(stream_protocol::classify_server_message("error") == ServerMessageType::Error);
    // unknown top-level keys map to Unhandled so the raw payload is forwarded to subscribers
    CHECK(stream_protocol::classify_server_message("response.created") == ServerMessageType::Unhandled);
    CHECK(stream_protocol::classify_server_message("") == ServerMessageType::Unhandled);
    CHECK(stream_protocol::classify_server_message(nullptr) == ServerMessageType::Unhandled);
}

void test_json_depth_limit() {
    CHECK(!stream_protocol::json_depth_exceeded(R"({"value":[1,2,3]})", 2));
    CHECK(stream_protocol::json_depth_exceeded(R"({"value":[{"nested":true}]})", 2));
    CHECK(!stream_protocol::json_depth_exceeded(R"({"text":"[[[ {{{"})", 1));
    CHECK(!stream_protocol::json_depth_exceeded(R"({"text":"escaped quote: \" and brace: {"})", 1));
    CHECK(!stream_protocol::json_depth_exceeded("{}", 1));
    CHECK(stream_protocol::json_depth_exceeded("{}", 0));
    CHECK(stream_protocol::json_depth_exceeded(nullptr, 1));
    CHECK(stream_protocol::json_depth_exceeded("{}", -1));
}

void expect_valid_uri(const char *uri) {
    char destination[256] = {};
    CHECK(stream_protocol::validate_ws_uri(uri, destination, sizeof(destination)));
    CHECK(std::strcmp(uri, destination) == 0);
}

void expect_invalid_uri(const char *uri) {
    char destination[256] = "unchanged";
    CHECK(!stream_protocol::validate_ws_uri(uri, destination, sizeof(destination)));
    CHECK(std::strcmp(destination, "unchanged") == 0);
}

void test_valid_websocket_uris() {
    expect_valid_uri("ws://localhost");
    expect_valid_uri("ws://localhost:8080");
    expect_valid_uri("wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService."
                     "BidiGenerateContent?key=secret");
    expect_valid_uri("wss://example-host.test:1/path");
    expect_valid_uri("wss://example.test:65535?query=1");
    expect_valid_uri("ws://127.0.0.1:8080");
    expect_valid_uri("ws://[::1]:8080/path");
    expect_valid_uri("wss://[2001:db8::1]/path");
}

void test_invalid_websocket_uris() {
    expect_invalid_uri("");
    expect_invalid_uri("http://example.test");
    expect_invalid_uri("ws://");
    expect_invalid_uri("ws://user@example.test");
    expect_invalid_uri("ws://example.test:");
    expect_invalid_uri("ws://example.test:0");
    expect_invalid_uri("ws://example.test:65536");
    expect_invalid_uri("ws://example.test:not-a-port");
    expect_invalid_uri("ws://2001:db8::1");
    expect_invalid_uri("ws://[invalid::address]");

    char destination[5] = "keep";
    CHECK(!stream_protocol::validate_ws_uri("ws://localhost", destination, sizeof(destination)));
    CHECK(std::strcmp(destination, "keep") == 0);
    CHECK(!stream_protocol::validate_ws_uri(nullptr, destination, sizeof(destination)));
    CHECK(!stream_protocol::validate_ws_uri("ws://localhost", nullptr, sizeof(destination)));
}

void test_utf8_validation() {
    CHECK(stream_protocol::is_valid_utf8(""));
    CHECK(stream_protocol::is_valid_utf8("plain ASCII"));
    CHECK(stream_protocol::is_valid_utf8(u8"こんにちは"));
    CHECK(stream_protocol::is_valid_utf8(u8"\U00010348"));

    const char continuation_only[] = {static_cast<char>(0x80), '\0'};
    const char bad_continuation[] = {static_cast<char>(0xC2), 'A', '\0'};
    const char truncated_two_byte[] = {static_cast<char>(0xC2), '\0'};
    const char overlong_two_byte[] = {static_cast<char>(0xC0), static_cast<char>(0x80), '\0'};
    const char overlong_three_byte[] = {static_cast<char>(0xE0), static_cast<char>(0x80), static_cast<char>(0x80),
                                        '\0'};
    const char surrogate[] = {static_cast<char>(0xED), static_cast<char>(0xA0), static_cast<char>(0x80), '\0'};
    const char above_unicode_limit[] = {static_cast<char>(0xF4), static_cast<char>(0x90), static_cast<char>(0x80),
                                        static_cast<char>(0x80), '\0'};
    CHECK(!stream_protocol::is_valid_utf8(continuation_only));
    CHECK(!stream_protocol::is_valid_utf8(bad_continuation));
    CHECK(!stream_protocol::is_valid_utf8(truncated_two_byte));
    CHECK(!stream_protocol::is_valid_utf8(overlong_two_byte));
    CHECK(!stream_protocol::is_valid_utf8(overlong_three_byte));
    CHECK(!stream_protocol::is_valid_utf8(surrogate));
    CHECK(!stream_protocol::is_valid_utf8(above_unicode_limit));
    CHECK(!stream_protocol::is_valid_utf8(nullptr));
}

void test_realtime_input_json() {
    stream_protocol::RealtimeInputMessage literal(16000);
    CHECK(literal.format("AA==") == R"({"realtimeInput":{"audio":{"data":"AA==","mimeType":"audio/pcm;rate=16000"}}})");
    for (uint32_t rate : {8000u, 16000u, 24000u}) {
        stream_protocol::RealtimeInputMessage message(rate);
        const std::string suffix = "\",\"mimeType\":\"audio/pcm;rate=" + std::to_string(rate) + "\"}}}";
        for (const std::string& audio : {std::string("AAECA/8="), std::string(42668, 'A'),
                                         std::string("+/8="), std::string("AA==")}) {
            const std::string expected = "{\"realtimeInput\":{\"audio\":{\"data\":\"" + audio + suffix;
            CHECK(message.format(audio) == expected);
            CHECK(!stream_protocol::json_depth_exceeded(message.format(audio).c_str(), 128));
        }
    }
}

} // namespace

int main() {
    int failures = 0;
    failures += run_test("realtime input JSON and buffer reuse", test_realtime_input_json);
    failures += run_test("server message classification", test_server_message_classification);
    failures += run_test("JSON depth limit", test_json_depth_limit);
    failures += run_test("valid WebSocket URIs", test_valid_websocket_uris);
    failures += run_test("invalid WebSocket URIs", test_invalid_websocket_uris);
    failures += run_test("UTF-8 validation", test_utf8_validation);
    return failures == 0 ? 0 : 1;
}
