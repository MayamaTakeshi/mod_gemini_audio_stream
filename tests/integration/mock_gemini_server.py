#!/usr/bin/env python3
import argparse
import asyncio
import base64
import json
import math
import struct
from pathlib import Path

import websockets


def pcm16_tone(sample_rate, frequency, duration_seconds, amplitude=12000):
    sample_count = int(sample_rate * duration_seconds)
    samples = (
        int(amplitude * math.sin(2 * math.pi * frequency * index / sample_rate))
        for index in range(sample_count)
    )
    return struct.pack(f"<{sample_count}h", *samples)


class MockGeminiServer:
    def __init__(self, event_log: Path, ready_file: Path):
        self.event_log = event_log
        self.ready_file = ready_file
        self._lock = asyncio.Lock()

    async def record(self, event, **fields):
        payload = {"event": event, **fields}
        async with self._lock:
            with self.event_log.open("a", encoding="utf-8") as log:
                log.write(json.dumps(payload, sort_keys=True) + "\n")

    async def handle(self, websocket, path=None):
        path = path or getattr(websocket, "path", "/")
        await self.record("connected", path=path)

        try:
            if path == "/close-immediately":
                await websocket.close(code=1011, reason="intentional integration-test close")
                await self.record("closed", path=path)
                return

            # The Gemini Live API requires the client's first message to be the setup
            # handshake: call audio must not precede setupComplete. The module fulfills
            # this by buffering capture audio and flushing it only after setup.
            first = await asyncio.wait_for(websocket.recv(), timeout=10)
            if isinstance(first, bytes):
                await self.record("audio-before-setup", size=len(first))
                await websocket.close(code=1002, reason="binary audio sent before setup")
                return

            try:
                first_payload = json.loads(first)
            except json.JSONDecodeError:
                await self.record("invalid-json")
                await websocket.close(code=1002, reason="first message is not valid JSON")
                return

            if "setup" not in first_payload:
                await self.record("setup-missing", keys=sorted(first_payload.keys()))
                await websocket.close(code=1002, reason="first message is not a BidiGenerateContentSetup")
                return

            await self.record(
                "setup-received",
                model=(first_payload.get("setup", {}).get("config", {}).get("model", "")),
            )
            await websocket.send(json.dumps({"setupComplete": {}}))

            async for message in websocket:
                if isinstance(message, bytes):
                    await self.record("binary", size=len(message))
                    continue

                try:
                    payload = json.loads(message)
                except json.JSONDecodeError:
                    await self.record("invalid-json")
                    continue

                if "realtimeInput" in payload:
                    audio_obj = payload["realtimeInput"].get("audio", {})
                    try:
                        audio = base64.b64decode(audio_obj.get("data", ""), validate=True)
                    except (ValueError, TypeError):
                        await self.record("invalid-audio-message")
                    else:
                        await self.record(
                            "audio-received",
                            size=len(audio),
                            sample_aligned=len(audio) % 2 == 0,
                            mime_type=audio_obj.get("mimeType", ""),
                        )
                else:
                    await self.record("message", payload=payload)

                if "integration.response" in payload:
                    await self.send_audio_response(websocket)
                elif "integration.bargein" in payload:
                    await self.send_bargein(websocket)
        finally:
            await self.record("disconnected", path=path)

    async def send_audio_response(self, websocket):
        sample_rate = 24000
        frequency = 1000
        duration_seconds = 0.6
        audio = pcm16_tone(sample_rate, frequency, duration_seconds)
        chunk_sizes = (137, 521, 1003, 269, 1607)
        part_index = 0

        offset = 0
        while offset < len(audio):
            chunk_size = chunk_sizes[part_index % len(chunk_sizes)] * 2
            chunk = audio[offset : offset + chunk_size]
            server_content = {
                "modelTurn": {
                    "parts": [
                        {
                            "inlineData": {
                                "mimeType": "audio/pcm;rate=24000",
                                "data": base64.b64encode(chunk).decode("ascii"),
                            }
                        }
                    ]
                },
            }
            offset += len(chunk)
            part_index += 1
            if offset >= len(audio):
                server_content["turnComplete"] = True
            await websocket.send(json.dumps({"serverContent": server_content}))

        await self.record(
            "audio-response-sent",
            frequency=frequency,
            duration_seconds=duration_seconds,
        )

    async def send_bargein(self, websocket):
        # Mirrors Gemini barge-in detection: a control-only serverContent with no model
        # turn audio that tells the client to stop the current model playback.
        await websocket.send(json.dumps({"serverContent": {"interrupted": True}}))
        await self.record("bargein-sent")

    async def run(self, host, port):
        self.event_log.unlink(missing_ok=True)
        self.ready_file.unlink(missing_ok=True)
        async with websockets.serve(self.handle, host, port, max_size=16 * 1024 * 1024):
            self.ready_file.touch()
            await asyncio.Future()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--event-log", type=Path, required=True)
    parser.add_argument("--ready-file", type=Path, required=True)
    args = parser.parse_args()

    server = MockGeminiServer(args.event_log, args.ready_file)
    asyncio.run(server.run(args.host, args.port))


if __name__ == "__main__":
    main()