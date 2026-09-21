#ifndef MOD_GEMINI_AUDIO_STREAM_H
#define MOD_GEMINI_AUDIO_STREAM_H

#include <switch.h>
#include <limits.h>
#include <speex/speex_resampler.h>

#define MY_BUG_NAME "gemini_audio_stream"
#define MAX_SESSION_ID (256)
#define MAX_WS_URI (4096)
#define STREAM_MIN_SAMPLING (8000)
#define STREAM_MAX_SAMPLING (48000)

#define EVENT_CONNECT "mod_gemini_audio_stream::connect"
#define EVENT_DISCONNECT "mod_gemini_audio_stream::disconnect"
#define EVENT_ERROR "mod_gemini_audio_stream::error"
#define EVENT_JSON "mod_gemini_audio_stream::json"
#define EVENT_PLAY "mod_gemini_audio_stream::play"
#define EVENT_SPEECH_STARTED "mod_gemini_audio_stream::speech_started"
#define EVENT_SPEECH_STOPPED "mod_gemini_audio_stream::speech_stopped"

typedef void (*responseHandler_t)(switch_core_session_t *session, const char *eventName, const char *json);

struct private_data {
    switch_mutex_t *mutex;
    char sessionId[MAX_SESSION_ID];
    SpeexResamplerState *resampler;
    void *pAudioStreamer;
    int sampling;
    int channels;
    switch_atomic_t audio_paused;
    switch_atomic_t user_audio_muted;
    switch_atomic_t assistant_audio_muted;
    switch_atomic_t close_requested;
    switch_buffer_t *sbuffer;
    int rtp_packets;
    switch_buffer_t *playback_buffer;
    void *stream_buffers;
};

typedef struct private_data private_t;

enum notifyEvent_t { CONNECT_SUCCESS, CONNECT_ERROR, CONNECTION_DROPPED, MESSAGE };

#endif // MOD_GEMINI_AUDIO_STREAM_H