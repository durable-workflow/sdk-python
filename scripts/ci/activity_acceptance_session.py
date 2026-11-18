"""Bounded stdin/stdout exchange with the closed, already-owned consumer."""

from __future__ import annotations

import json
import os
import selectors
import subprocess
import time
from collections import deque


class Session:
    def __init__(self, identity, environment):
        self.process = subprocess.Popen(
            ["docker", "start", "-a", "-i", identity],
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        self.selector = selectors.DefaultSelector()
        os.set_blocking(self.process.stdin.fileno(), False)
        os.set_blocking(self.process.stdout.fileno(), False)
        self.selector.register(self.process.stdout, selectors.EVENT_READ)
        self.deadline = time.monotonic() + 240
        self.output = bytearray()
        self.pending = bytearray()
        self.lines = deque()
        self.eof = False

    def read_available(self):
        chunk = os.read(self.process.stdout.fileno(), 65536)
        if not chunk:
            self.eof = True
            self.selector.unregister(self.process.stdout)
            return
        self.output.extend(chunk)
        if len(self.output) > 2 * 1024 * 1024:
            raise ValueError("consumer output exceeds its bound")
        self.pending.extend(chunk)
        while b"\n" in self.pending:
            line, _, tail = self.pending.partition(b"\n")
            self.pending = bytearray(tail)
            self.lines.append(line.decode())

    def events(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("closed consumer deadline")
        return self.selector.select(min(remaining, 1))

    def send(self, value):
        data = json.dumps(value, separators=(",", ":")).encode() + b"\n"
        if len(data) > 64 * 1024 * 1024:
            raise ValueError("consumer setup exceeds its bound")
        self.selector.register(self.process.stdin, selectors.EVENT_WRITE)
        offset = 0
        try:
            while offset < len(data):
                for key, _ in self.events():
                    if key.fileobj is self.process.stdout:
                        self.read_available()
                    else:
                        offset += os.write(self.process.stdin.fileno(), data[offset : offset + 65536])
                if self.eof:
                    raise RuntimeError("consumer exited before receiving input")
        finally:
            self.selector.unregister(self.process.stdin)

    def receive(self, marker):
        while True:
            while self.lines:
                line = self.lines.popleft()
                if line.startswith(marker):
                    return json.loads(line[len(marker) :])
            if self.eof:
                raise RuntimeError("consumer exited without its bounded result")
            for _key, _ in self.events():
                self.read_available()

    def finish(self):
        self.process.stdin.close()
        while not self.eof:
            for _key, _ in self.events():
                self.read_available()
        return self.process.wait(timeout=10)

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=10)
        self.process.stdin.close()
        self.process.stdout.close()
        self.selector.close()
