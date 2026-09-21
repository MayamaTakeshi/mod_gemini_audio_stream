#!/usr/bin/env python3
import base64
import json
import math
import socket
import subprocess
import sys
import threading
import time
import unittest
import wave
from array import array
from pathlib import Path


MOCK_URL = "ws://127.0.0.1:18080"
EVENT_LOG = Path("/tmp/mod-gemini-mock-events.jsonl")
PLAYBACK_DRAIN_MARGIN_SECONDS = 0.4
PLAYBACK_DURATION_TOLERANCE_SECONDS = 0.1
FS_EVENT_SOCKET_HOST = "127.0.0.1"
FS_EVENT_SOCKET_PORT = 8021
FS_EVENT_SOCKET_PASSWORD = "ClueCon"
EVENT_SPEECH_STARTED = "mod_gemini_audio_stream::speech_started"
EVENT_SPEECH_STOPPED = "mod_gemini_audio_stream::speech_stopped"


def api(command, timeout=10):
    result = subprocess.run(
        ["fs_cli", "-x", command],
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(f"fs_cli failed for {command!r}: {result.stderr.strip()}")
    return result.stdout.strip()


def assert_ok(test, command):
    result = api(command)
    test.assertTrue(result.startswith("+OK"), f"command failed: {command}\n{result}")
    return result


def assert_error(test, command):
    result = api(command)
    test.assertTrue(result.startswith("-ERR"), f"command unexpectedly succeeded: {command}\n{result}")
    return result


def mock_events():
    if not EVENT_LOG.exists():
        return []
    events = []
    for line in EVENT_LOG.read_text(encoding="utf-8").splitlines():
        if line:
            events.append(json.loads(line))
    return events


def wait_for_event(predicate, start_index=0, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        events = mock_events()
        for event in events[start_index:]:
            if predicate(event):
                return event
        time.sleep(0.05)
    return None


def encode_json(payload):
    compact = json.dumps(payload, separators=(",", ":")).encode()
    return base64.b64encode(compact).decode()


def read_mono_pcm16(path):
    with wave.open(str(path), "rb") as recording:
        channels = recording.getnchannels()
        sample_width = recording.getsampwidth()
        sample_rate = recording.getframerate()
        frames = recording.readframes(recording.getnframes())

    if sample_width != 2:
        raise AssertionError(f"expected PCM16 recording, got {sample_width * 8}-bit samples")

    samples = array("h")
    samples.frombytes(frames)
    if sys.byteorder == "big":
        samples.byteswap()
    if channels > 1:
        samples = array(
            "h",
            (
                sum(samples[index : index + channels]) // channels
                for index in range(0, len(samples), channels)
            ),
        )
    return sample_rate, samples


def goertzel_power(samples, sample_rate, frequency):
    coefficient = 2 * math.cos(2 * math.pi * frequency / sample_rate)
    previous = 0.0
    previous_previous = 0.0
    for sample in samples:
        current = sample + coefficient * previous - previous_previous
        previous_previous = previous
        previous = current
    return previous_previous**2 + previous**2 - coefficient * previous * previous_previous


def dominant_frequency(samples, sample_rate):
    frequencies = range(300, min(3000, sample_rate // 2), 25)
    return max(frequencies, key=lambda frequency: goertzel_power(samples, sample_rate, frequency))


def audible_duration(samples, sample_rate):
    window_size = max(sample_rate // 50, 1)
    active_samples = 0
    for offset in range(0, len(samples) - window_size + 1, window_size):
        window = samples[offset : offset + window_size]
        mean_square = sum(sample * sample for sample in window) / len(window)
        if mean_square >= 500**2:
            active_samples += len(window)
    return active_samples / sample_rate


def wait_for_realtime_playback(duration_seconds):
    # The mock queues audio immediately, but FreeSWITCH consumes it at media rate.
    time.sleep(duration_seconds + PLAYBACK_DRAIN_MARGIN_SECONDS)


class EventSocketListener:
    """Minimal FreeSWITCH event-socket (ESL) subscriber with only the stdlib.

    Listens for the module's CUSTOM events so integration tests can assert on
    mod_gemini_audio_stream::speech_started / speech_stopped, which are not
    visible to the mock WebSocket server.
    """

    def __init__(self, host=FS_EVENT_SOCKET_HOST, port=FS_EVENT_SOCKET_PORT, password=FS_EVENT_SOCKET_PASSWORD):
        self.host = host
        self.port = port
        self.password = password
        self._socket = None
        self._file = None
        self._thread = None
        self._events = []
        self._lock = threading.Lock()

    def start(self, attempts=30, gap=0.1):
        last_error = None
        for _ in range(attempts):
            try:
                self._socket = socket.create_connection((self.host, self.port), timeout=5)
                break
            except OSError as error:
                last_error = error
                time.sleep(gap)
        if self._socket is None:
            raise RuntimeError(
                f"could not connect to the FreeSWITCH event socket on {self.host}:{self.port}: {last_error}"
            )
        self._file = self._socket.makefile("r", encoding="utf-8", errors="replace", newline="\n")
        self._expect_ok(f"auth {self.password}")
        self._expect_ok("event plain CUSTOM")
        self._thread = threading.Thread(target=self._read_loop, name="esl-listener", daemon=True)
        self._thread.start()
        return self

    def close(self):
        if self._socket is not None:
            try:
                self._socket.close()
            except OSError:
                pass
            self._socket = None
        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None

    def _expect_ok(self, command, timeout=5):
        self._socket.sendall((command + "\n\n").encode("ascii"))
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            line = self._file.readline()
            if not line:
                raise RuntimeError(f"event socket closed while awaiting the reply to {command!r}")
            cleaned = line.strip()
            if cleaned.startswith("+OK"):
                return
            if cleaned.startswith("-ERR"):
                raise RuntimeError(f"event socket rejected {command!r}: {cleaned}")
        raise TimeoutError(f"no reply to the event socket command {command!r}")

    def _read_loop(self):
        while True:
            line = self._file.readline()
            if not line:
                return
            line = line.rstrip("\r\n")
            if not line:
                continue
            if line[:1].isspace():
                continue
            key, separator, value = line.partition(":")
            if not separator or not key.strip():
                continue
            with self._lock:
                self._events.append({key.strip(): value.strip()})

    def snapshot(self):
        with self._lock:
            return list(self._events)

    def wait_for_event(self, subclass, start_index=0, timeout=5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            events = self.snapshot()
            for index in range(start_index, len(events)):
                if events[index].get("Event-Subclass") == subclass:
                    return events[index], index
            time.sleep(0.05)
        return None


class ModuleIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.uuid = self.originate_call()
        assert_ok(self, f"uuid_setvar {self.uuid} STREAM_GEMINI_API_KEY integration-test-key")
        assert_ok(self, f"uuid_setvar {self.uuid} STREAM_DISABLE_AUDIOFILES true")
        assert_ok(self, f"uuid_setvar {self.uuid} STREAM_NO_RECONNECT true")

    def tearDown(self):
        if getattr(self, "uuid", None):
            api(f"uuid_kill {self.uuid}")
        recording = getattr(self, "recording", None)
        if recording:
            recording.unlink(missing_ok=True)
        self.assertTrue(api("status").startswith("UP"), "FreeSWITCH stopped during the test")

    def originate_call(self):
        result = api("originate null/+15555550100 &park()")
        self.assertTrue(result.startswith("+OK "), f"could not originate test channel: {result}")
        uuid = result.removeprefix("+OK ").strip()
        assert_ok(self, f"uuid_broadcast {uuid} silence_stream://-1 aleg")
        return uuid

    def start_stream(self, url=MOCK_URL, start_muted=True):
        before = len(mock_events())
        command = f"uuid_gemini_audio_stream {self.uuid} start {url} mono 16k 24k"
        if start_muted:
            command += " mute_user"
        assert_ok(self, command)
        connected = wait_for_event(lambda event: event.get("event") == "connected", before)
        self.assertIsNotNone(connected, "module did not connect to the mock WebSocket server")

    def send_setup(self, model="models/mock"):
        before = len(mock_events())
        setup = encode_json({"setup": {"config": {"model": model, "responseModalities": ["AUDIO"]}}})
        assert_ok(self, f"uuid_gemini_audio_stream {self.uuid} send_json {setup}")
        received = wait_for_event(lambda event: event.get("event") == "setup-received", before)
        self.assertIsNotNone(received, "mock did not receive the Gemini setup handshake")
        return received

    def stop_stream(self, final_payload=None):
        before = len(mock_events())
        command = f"uuid_gemini_audio_stream {self.uuid} stop"
        if final_payload is not None:
            command += f" {encode_json(final_payload)}"
        assert_ok(self, command)

        if final_payload is not None:
            delivered = wait_for_event(
                lambda event: event.get("event") == "message" and event.get("payload") == final_payload,
                before,
            )
            self.assertIsNotNone(delivered, "final stop payload was not delivered before disconnect")

        disconnected = wait_for_event(lambda event: event.get("event") == "disconnected", before)
        self.assertIsNotNone(disconnected, "WebSocket did not disconnect after stop")

    def restart_after_automatic_cleanup(self, timeout=5):
        deadline = time.monotonic() + timeout
        last_result = "no start attempt completed"
        while time.monotonic() < deadline:
            before = len(mock_events())
            command = f"uuid_gemini_audio_stream {self.uuid} start {MOCK_URL} mono 16k 24k mute_user"
            last_result = api(command)
            if last_result.startswith("+OK"):
                connected = wait_for_event(lambda event: event.get("event") == "connected", before)
                self.assertIsNotNone(connected, "restart succeeded but WebSocket did not reconnect")
                return
            time.sleep(0.05)
        self.fail(f"stream context was not cleaned up after peer disconnect: {last_result}")

    def assert_recording_contains_tone(self, expected_frequency, expected_duration_seconds):
        self.assertTrue(
            self.recording.exists(),
            "FreeSWITCH did not create the playback recording",
        )
        sample_rate, samples = read_mono_pcm16(self.recording)
        self.assertGreater(
            audible_duration(samples, sample_rate),
            expected_duration_seconds - PLAYBACK_DURATION_TOLERANCE_SECONDS,
            "recording contains less audible audio than the mock sent",
        )
        frequency = dominant_frequency(samples, sample_rate)
        self.assertAlmostEqual(frequency, expected_frequency, delta=75)

    def test_setup_gate_and_final_stop_payload(self):
        self.start_stream()
        # Capture audio flows from the wire immediately, so the module must hold back
        # every realtimeInput until the setup handshake completes.
        assert_ok(self, f"uuid_gemini_audio_stream {self.uuid} pause")
        assert_ok(self, f"uuid_gemini_audio_stream {self.uuid} resume")
        setup = self.send_setup()
        self.assertEqual(setup["model"], "models/mock")
        assert_ok(self, f"uuid_gemini_audio_stream {self.uuid} mute assistant")
        assert_ok(self, f"uuid_gemini_audio_stream {self.uuid} unmute assistant")

        self.stop_stream({"type": "integration.final", "marker": "stop-payload"})

    def test_caller_audio_reaches_websocket_after_setup(self):
        before = len(mock_events())
        self.start_stream(start_muted=False)
        self.send_setup()
        audio = wait_for_event(lambda event: event.get("event") == "audio-received", before)
        self.assertIsNotNone(audio, "module did not send caller audio to the WebSocket")
        self.assertGreater(audio["size"], 0, "module sent an empty audio payload")
        self.assertTrue(audio["sample_aligned"], "module sent a partial PCM16 sample")
        self.assertEqual(audio["mime_type"], "audio/pcm;rate=16000")

        # Gemini rejects caller audio before the setup handshake: nothing may reach the
        # server before setup, and the buffered pre-setup capture must land after it.
        events = mock_events()
        self.assertFalse(
            any(event.get("event") == "audio-before-setup" for event in events),
            "module sent audio before the Gemini setup handshake",
        )
        setup_index = next(
            (index for index, event in enumerate(events) if event.get("event") == "setup-received"),
            None,
        )
        first_audio = next(
            (index for index, event in enumerate(events) if event.get("event") == "audio-received"),
            None,
        )
        self.assertIsNotNone(setup_index, "setup was not received before any audio")
        self.assertGreater(first_audio, setup_index, "caller audio arrived before the setup handshake")
        self.stop_stream()

    def test_playback_audio_reaches_channel(self):
        self.start_stream()
        self.recording = Path(f"/tmp/mod-gemini-playback-{self.uuid}.wav")
        self.recording.unlink(missing_ok=True)
        assert_ok(self, f"uuid_record {self.uuid} start {self.recording}")
        self.send_setup()

        before = len(mock_events())
        request = encode_json({"type": "integration.response"})
        assert_ok(self, f"uuid_gemini_audio_stream {self.uuid} send_json {request}")
        sent = wait_for_event(lambda event: event.get("event") == "audio-response-sent", before)
        self.assertIsNotNone(sent, "mock serverContent tone was not delivered")
        wait_for_realtime_playback(sent["duration_seconds"])
        assert_ok(self, f"uuid_record {self.uuid} stop {self.recording}")

        self.assert_recording_contains_tone(sent["frequency"], sent["duration_seconds"])
        self.stop_stream()

    def test_completed_turn_fires_speech_events(self):
        # Regression guard: a finished spoken turn must free the "response audio
        # done" latch and emit speech_stopped once playback drains. The mock sends
        # turnComplete on the same frame as the last audio chunk, and intermediate
        # audio frames must not arm the latch early.
        listener = EventSocketListener().start()
        self.addCleanup(listener.close)
        self.start_stream()
        self.send_setup()

        before = len(listener.snapshot())
        sent_index = len(mock_events())
        response = encode_json({"type": "integration.response"})
        assert_ok(self, f"uuid_gemini_audio_stream {self.uuid} send_json {response}")
        sent = wait_for_event(lambda event: event.get("event") == "audio-response-sent", sent_index)
        self.assertIsNotNone(sent, "mock playback tone was not delivered")

        started = listener.wait_for_event(EVENT_SPEECH_STARTED, before)
        self.assertIsNotNone(started, "module did not emit speech_started when playback began")

        wait_for_realtime_playback(sent["duration_seconds"])
        stopped = listener.wait_for_event(EVENT_SPEECH_STOPPED, before)
        self.assertIsNotNone(stopped, "module did not emit speech_stopped after the completed turn drained")
        self.assertGreater(stopped[1], started[1], "speech_stopped was observed before speech_started")

        self.stop_stream()

    def test_bargein_interrupts_then_playback_recovers(self):
        self.start_stream()
        self.recording = Path(f"/tmp/mod-gemini-bargein-{self.uuid}.wav")
        self.recording.unlink(missing_ok=True)
        assert_ok(self, f"uuid_record {self.uuid} start {self.recording}")
        self.send_setup()

        before = len(mock_events())
        assert_ok(self, f"uuid_gemini_audio_stream {self.uuid} send_json {encode_json({'type': 'integration.response'})}")
        sent = wait_for_event(lambda event: event.get("event") == "audio-response-sent", before)
        self.assertIsNotNone(sent, "mock playback tone was not delivered")

        before = len(mock_events())
        assert_ok(self, f"uuid_gemini_audio_stream {self.uuid} send_json {encode_json({'type': 'integration.bargein'})}")
        barged = wait_for_event(lambda event: event.get("event") == "bargein-sent", before)
        self.assertIsNotNone(barged, "mock did not send the interrupted serverContent")
        wait_for_realtime_playback(0.1)

        before = len(mock_events())
        assert_ok(self, f"uuid_gemini_audio_stream {self.uuid} send_json {encode_json({'type': 'integration.response'})}")
        sent = wait_for_event(lambda event: event.get("event") == "audio-response-sent", before)
        self.assertIsNotNone(sent, "second mock playback tone was not delivered")
        wait_for_realtime_playback(sent["duration_seconds"])
        assert_ok(self, f"uuid_record {self.uuid} stop {self.recording}")

        self.assert_recording_contains_tone(sent["frequency"], sent["duration_seconds"])
        self.stop_stream()

    def test_double_start_is_rejected(self):
        self.start_stream()
        assert_error(self, f"uuid_gemini_audio_stream {self.uuid} start {MOCK_URL} mono 16k 24k mute_user")
        self.stop_stream()

    def test_rapid_restart(self):
        for _ in range(10):
            self.start_stream()
            self.stop_stream()

    def test_hangup_while_streaming(self):
        self.start_stream()
        before = len(mock_events())
        assert_ok(self, f"uuid_kill {self.uuid}")
        self.uuid = None
        disconnected = wait_for_event(lambda event: event.get("event") == "disconnected", before)
        self.assertIsNotNone(disconnected, "WebSocket did not disconnect after channel hangup")

    def test_immediate_peer_close_without_reconnect(self):
        before = len(mock_events())
        assert_ok(
            self,
            f"uuid_gemini_audio_stream {self.uuid} start {MOCK_URL}/close-immediately mono 16k 24k mute_user",
        )
        closed = wait_for_event(lambda event: event.get("event") == "closed", before)
        self.assertIsNotNone(closed, "mock peer did not close the WebSocket")
        disconnected = wait_for_event(lambda event: event.get("event") == "disconnected", before)
        self.assertIsNotNone(disconnected, "mock WebSocket handler did not finish after peer close")

        self.restart_after_automatic_cleanup()
        self.stop_stream()

    def test_invalid_start_arguments_are_rejected(self):
        assert_error(self, f"uuid_gemini_audio_stream {self.uuid} start http://example.test mono 16k 24k")
        assert_error(self, f"uuid_gemini_audio_stream {self.uuid} start {MOCK_URL} invalid 24k")
        assert_error(self, f"uuid_gemini_audio_stream {self.uuid} start {MOCK_URL} mono 12000")
        assert_error(self, f"uuid_gemini_audio_stream {self.uuid} start {MOCK_URL} mono 16k 24k unexpected")


if __name__ == "__main__":
    unittest.main(verbosity=2)