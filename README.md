# mod_gemini_audio_stream

A FreeSWITCH module that streams L16 audio from a channel to the Google
[Gemini Live API](https://ai.google.dev/gemini-api/docs/live) over WebSocket and plays the
model's audio responses back into the channel in real time.

It is a provider-specific port of
[mod_openai_realtime](https://github.com/VoiSmart/mod_openai_realtime): the transport, media bug,
resampling, TLS, reconnection and event plumbing are shared, while the wire protocol is adapted to
Gemini's `BidiGenerateContent` framing (setup handshake, `realtimeInput` for caller audio and
`serverContent` for model audio).

## Why a separate module?

The Gemini Live API differs structurally from OpenAI's Realtime API:

| Aspect | OpenAI Realtime | Gemini Live API |
|---|---|---|
| Handshake | none; the server greets the client | the client must send a `setup` message as the **very first** frame and wait for `setupComplete` before any audio |
| Caller audio | `{"type":"input_audio_buffer.append","audio":B64}` | `{"realtimeInput":{"audio":{"data":B64,"mimeType":"audio/pcm;rate=16000"}}}` |
| Model audio | `response.output_audio.delta` (`delta` key) | `serverContent.modelTurn.parts[].inlineData.data` |
| Message discrimination | `type` field | top-level key (`setupComplete`, `serverContent`, `toolCall`, `goAway`, ...) |
| Sample rates | 24k input / 24k output | **16k input** / 24k output |
| Autentication | `Authorization: Bearer` header | `X-Goog-Api-Key` header or `?key=` in the URI |

Because of the handshake-gated audio path and the different framing, the two protocols cannot share
a code path cleanly, so they live in separate modules. Generic parameter handling was inherited from
the OpenAI module and remains identical.

## Build

Install FreeSWITCH development headers, OpenSSL, Zlib and SpeexDSP development packages, then:

```sh
cmake -S . -B build -DENABLE_LOCAL=ON   # ENABLE_LOCAL points pkg-config at /usr/local/freeswitch
cmake --build build -j
cmake --install build                    # installs mod_gemini_audio_stream.so into the FS modules dir
```

The build consumes the `ixwebsocket` library that is vendored under `libs/`.

## Quick start

Start the module in `modules.conf.xml` (or load it at runtime with `fs_cli -x "load
mod_gemini_audio_stream"`). A minimal dialplan usage is:

```xml
<extension name="gemini">
    <condition field="destination_number" expression="^999$">
        <action application="answer"/>
        <action application="set" data="STREAM_GEMINI_API_KEY=YOUR_API_KEY"/>
        <action application="set" data="stream_result=${uuid_gemini_audio_stream ${uuid} start wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=${STREAM_GEMINI_API_KEY} mono}"/>
        <action application="set" data="stream_result=${uuid_gemini_audio_stream ${uuid} send_json ZT...}"/>
        <action application="playback" data="silence_stream://-1//"/>
        <action application="set" data="stream_result=${uuid_gemini_audio_stream ${uuid} stop}"/>
    </condition>
</extension>
```

Notes:

- The API key can be supplied either as `?key=...` in the URI (the example above) or via the
  `STREAM_GEMINI_API_KEY` channel variable, which adds an `X-Goog-Api-Key` header.
- Gemini's Live API **requires** a `setup` message as the very first WebSocket frame. Send it with
  `uuid_gemini_audio_stream ... send_json <base64json>`. The module holds caller audio until the
  server acknowledges with `setupComplete`, so the ordering requirement is enforced automatically.
- The `playback` with `silence_stream://-1//` keeps the media flowing so model audio can be written
  to the channel. See [mod_audio_stream](https://github.com/amigniter/mod_audio_stream) issue
  patterns for details.
- The default caller audio rate is `16k` (Gemini input) and the default playback rate is `24k`
  (Gemini output). HTTP/1.1 is used; per-message deflate is available through
  `STREAM_MESSAGE_DEFLATE`.

## Gemini Live protocol mapping

The module performs no protocol logic of its own; it only transports audio and forwards the raw
JSON events to subscribers. The Gemini Live session is driven by whatever you send with
`send_json` (setup, contents, ...) and by server events delivered as the
`mod_gemini_audio_stream::json` events (toolCall, goAway, ...).

Handled locally:

- `setupComplete` — acknowledged internally to switch the caller-audio gate open; the raw message
  is still forwarded for visibility.
- `serverContent` — the `modelTurn.parts[].inlineData.data` base64 audio is decoded, resampled to
  the channel rate and queued for playback. Control-only payloads (`interrupted`, `turnComplete`) are
  forwarded as-is.
- `serverContent.interrupted` — clears the audio queue and the playback buffer (Gemini barge-in).
- `turnComplete` — marks the end of the current model turn so the `speech_stopped` event can fire
  once playback drains.

All other top-level frames (`toolCall`, `toolCallCancellation`, `goAway`, `error`, ...) are
forwarded unchanged to event subscribers.

## Channel variables

| Variable | Description | Default |
|---|---|---|
| `STREAM_GEMINI_API_KEY` | Google API key; adds the `X-Goog-Api-Key` header (merged with, and taking precedence over, `STREAM_EXTRA_HEADERS`) | none |
| `STREAM_EXTRA_HEADERS` | JSON object of extra WebSocket headers | none |
| `STREAM_NO_RECONNECT` | true/1 disables ixwebsocket automatic reconnection | false |
| `STREAM_HEART_BEAT` | WebSocket ping interval in seconds (1..3600) | none |
| `STREAM_TLS_CA_FILE` | Path to a CA bundle, or `NONE`/`SYSTEM` | system |
| `STREAM_TLS_KEY_FILE` / `STREAM_TLS_CERT_FILE` | Client TLS certificate | none |
| `STREAM_TLS_DISABLE_HOSTNAME_VALIDATION` | true disables hostname checks | false |
| `STREAM_MESSAGE_DEFLATE` | true disables per-message deflate | false |
| `STREAM_SUPPRESS_LOG` | true suppresses message logging | false |
| `STREAM_BUFFER_SIZE` | Capture aggregation window in ms (multiples of 20) | 20 |
| `STREAM_DISABLE_AUDIOFILES` | true disables writing debug `.wav` files | false |

## API

```
uuid_gemini_audio_stream <uuid> start <ws-uri> <mono | mixed | stereo> [<send-rate>] [<playback-rate>] [mute_user]
```

Attaches a media bug and starts streaming. `send-rate` defaults to `16k` and `playback-rate` to
`24k`, matching the Gemini Live API rates. `mute_user` delays forwarding caller audio until you
explicitly unmute.

```
uuid_gemini_audio_stream <uuid> send_json <base64json>
```

Sends a base64-encoded JSON object to the Gemini WebSocket endpoint. Base64 encoding avoids
whitespace/special-character issues in the fs_cli command parser.

```
uuid_gemini_audio_stream <uuid> stop [<base64json>]
uuid_gemini_audio_stream <uuid> pause
uuid_gemini_audio_stream <uuid> resume
uuid_gemini_audio_stream <uuid> mute [user | assistant | all]
uuid_gemini_audio_stream <uuid> unmute [user | assistant | all]
```

`pause`/`resume` halt both directions. `mute user` blocks caller audio from reaching Gemini;
`mute assistant` blocks model audio from reaching the channel. The raw PCM16-only entrypoint
`uuid_raw_audio_stream <uuid> start ...` is also provided for compliant binary backends.

## Events

Events are fired as FreeSWITCH custom events with the following subclasses:

- `mod_gemini_audio_stream::json`
- `mod_gemini_audio_stream::connect`
- `mod_gemini_audio_stream::disconnect`
- `mod_gemini_audio_stream::error`
- `mod_gemini_audio_stream::play`
- `mod_gemini_audio_stream::speech_started`
- `mod_gemini_audio_stream::speech_stopped`

`speech_started` fires when model audio actually starts playing to the channel; `speech_stopped`
fires when playback drains after `turnComplete`, or immediately when interrupted by barge-in.

## Tests

See [tests/README.md](tests/README.md). Unit tests run without FreeSWITCH; integration tests build
and load the module in a real FreeSWITCH process talking to a mock Gemini Live server
(`tests/integration/mock_gemini_server.py`) that enforces the setup-first handshake.