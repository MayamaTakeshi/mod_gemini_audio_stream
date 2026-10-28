#ifndef STREAM_PROTOCOL_H
#define STREAM_PROTOCOL_H

#include <cstddef>
#include <cstdint>
#include <string>

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

// Caller supplies codec-generated base64, whose alphabet needs no JSON escaping.
// The returned message remains valid until the next format() call.
class RealtimeInputMessage {
  public:
    explicit RealtimeInputMessage(uint32_t sample_rate);
    const std::string& format(const std::string& base64_audio);

  private:
    std::string m_suffix;
    std::string m_message;
};

ServerMessageType classify_server_message(const char *top_level_key);
bool json_depth_exceeded(const char *json, int max_depth);
bool validate_ws_uri(const char *url, char *destination, std::size_t destination_size);
bool is_valid_utf8(const char *text);

} // namespace stream_protocol

#endif // STREAM_PROTOCOL_H