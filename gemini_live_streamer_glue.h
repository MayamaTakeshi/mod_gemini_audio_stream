#ifndef GEMINI_LIVE_STREAMER_GLUE_H
#define GEMINI_LIVE_STREAMER_GLUE_H
#include "mod_gemini_audio_stream.h"

// Use C linkage when this header is included from C++, while keeping it valid for C callers.
#ifdef __cplusplus
extern "C" {
#endif

int validate_ws_uri(const char *url, char *wsUri);
switch_status_t is_valid_utf8(const char *str);
switch_status_t stream_session_send_json(switch_core_session_t *session, const char *json);
switch_status_t stream_session_pauseresume(switch_core_session_t *session, int pause);
switch_status_t stream_session_set_user_mute(switch_core_session_t *session, int mute);
switch_status_t stream_session_set_assistant_mute(switch_core_session_t *session, int mute);
switch_status_t stream_session_init(switch_core_session_t *session, responseHandler_t responseHandler,
                                    uint32_t samples_per_second, char *wsUri, int sampling, int playback_sampling,
                                    int channels, switch_bool_t start_muted, switch_bool_t force_raw_audio_mode,
                                    void **ppUserData);
switch_status_t stream_session_start(void *pUserData);
void stream_session_release(void *pUserData);
void *stream_session_lifecycle_lock(switch_core_session_t *session);
void stream_session_lifecycle_unlock(void *handle);
switch_bool_t stream_frame(switch_media_bug_t *bug);
switch_bool_t write_frame(switch_core_session_t *session, switch_media_bug_t *bug);
switch_status_t stream_session_cleanup(switch_core_session_t *session, char *text, int channelIsClosing);

#ifdef __cplusplus
}
#endif

#endif // GEMINI_LIVE_STREAMER_GLUE_H