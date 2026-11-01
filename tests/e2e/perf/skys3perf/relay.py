"""A TCP relay that delays what it forwards, for round trips without root.

The release run shapes links with tc netem (the target's link hooks, such
as tests/e2e/chaos/net.sh). Where netem is missing, as in CI's containers
and on loopback, the flush scenario puts this relay between the nodes and
the remote store instead: each byte arrives `delay` seconds after it was
sent, each way, so a request and its answer take one round trip more.
Bandwidth is not limited and nothing is lost, unlike netem; the result
records which shaper made each round trip.
"""

from __future__ import annotations

import asyncio
import threading
import time


class Relay:
    """Listens on 127.0.0.1 and forwards every connection to `upstream`
    (host, port), delaying each direction by `delay` seconds."""

    def __init__(self, upstream: tuple[str, int], delay: float) -> None:
        self.upstream = upstream
        self.delay = delay
        self.port = 0
        self.connections = 0
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._run, name="relay", daemon=True)
        self._server: asyncio.base_events.Server | None = None

    def __enter__(self) -> Relay:
        self._thread.start()
        if not self._ready.wait(10):
            raise RuntimeError("the relay did not start")
        return self

    def __exit__(self, *_exc) -> None:
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(10)

    @property
    def address(self) -> str:
        return f"127.0.0.1:{self.port}"

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._server = self._loop.run_until_complete(
            asyncio.start_server(self._accept, "127.0.0.1", 0))
        self.port = self._server.sockets[0].getsockname()[1]
        self._ready.set()
        self._loop.run_forever()
        self._server.close()
        for task in asyncio.all_tasks(self._loop):
            task.cancel()
        self._loop.run_until_complete(asyncio.sleep(0))
        self._loop.close()

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.connections += 1
        try:
            up_reader, up_writer = await asyncio.open_connection(*self.upstream)
        except OSError:
            writer.close()
            return
        await asyncio.gather(self._pipe(reader, up_writer), self._pipe(up_reader, writer),
                             return_exceptions=True)

    async def _pipe(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Forwards each piece `delay` after it arrived, in order: a queue
        of (due, data) drained by a writer, so pieces in flight overlap as
        packets on a long link do."""
        queue: asyncio.Queue = asyncio.Queue()

        async def send() -> None:
            while True:
                due, data = await queue.get()
                wait = due - time.monotonic()
                if wait > 0:
                    await asyncio.sleep(wait)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
            if writer.can_write_eof():
                writer.write_eof()

        sender = asyncio.ensure_future(send())
        try:
            while True:
                data = await reader.read(1 << 16)
                queue.put_nowait((time.monotonic() + self.delay, data))
                if not data:
                    break
            await sender
        except (OSError, asyncio.CancelledError):
            sender.cancel()
        finally:
            writer.close()
