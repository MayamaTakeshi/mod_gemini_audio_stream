#ifndef STREAM_PROTOCOL_H
#define STREAM_PROTOCOL_H

#include <cstddef>

namespace stream_protocol {

// Gemini Live API server -> client messages are discriminated by their top-level
// JSON key. setupComplete and serverContent carry the session state and audio;
// turnComplete/interrupted are flags inside a serverContent payload.
enum class ServerMessageType {
    SetupComplete,
    ServerContent,
    ToolCall,
    ToolCallCancellation,
    GoAway,
    Error,
    Unhandled,
};

ServerMessageType classify_server_message(const char *top_level_key);
bool json_depth_exceeded(const char *json, int max_depth);
bool validate_ws_uri(const char *url, char *destination, std::size_t destination_size);
bool is_valid_utf8(const char *text);

} // namespace stream_protocol

#endif // STREAM_PROTOCOL_H