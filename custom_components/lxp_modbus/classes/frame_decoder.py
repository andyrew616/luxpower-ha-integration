"""Stateful framing for LuxPower's length-prefixed TCP byte stream."""

from dataclasses import dataclass

from ..const import MAX_PACKET_SIZE
from .lxp_request_builder import LxpRequestBuilder
from .lxp_response import LxpResponse


@dataclass(frozen=True)
class FrameDecoderStats:
    """Cumulative decoder diagnostics for one connection generation."""

    frames: int
    discarded_bytes: int
    malformed_lengths: int
    buffered_bytes: int


class LuxFrameDecoder:
    """Assemble complete Lux frames across arbitrary TCP read boundaries."""

    _MIN_FRAME_SIZE = 8

    def __init__(self, *, max_frame_size: int = MAX_PACKET_SIZE) -> None:
        if max_frame_size < self._MIN_FRAME_SIZE:
            raise ValueError("max_frame_size is too small for a Lux frame")
        self._max_frame_size = max_frame_size
        self._buffer = bytearray()
        self._frames = 0
        self._discarded_bytes = 0
        self._malformed_lengths = 0

    def feed(self, data: bytes) -> tuple[bytes, ...]:
        """Add stream bytes and return every newly completed frame."""
        if data:
            self._buffer.extend(data)

        completed: list[bytes] = []
        prefix = LxpRequestBuilder.PREFIX

        while self._buffer:
            frame_start = self._buffer.find(prefix)
            if frame_start < 0:
                # Retain a possible first byte of a split A1 1A prefix.
                retained = 1 if self._buffer[-1:] == prefix[:1] else 0
                discarded = len(self._buffer) - retained
                self._discarded_bytes += discarded
                if retained:
                    self._buffer[:] = self._buffer[-1:]
                else:
                    self._buffer.clear()
                break

            if frame_start:
                del self._buffer[:frame_start]
                self._discarded_bytes += frame_start

            if len(self._buffer) < 6:
                break

            declared = int.from_bytes(self._buffer[4:6], "little") + 6
            if not self._MIN_FRAME_SIZE <= declared <= self._max_frame_size:
                # Drop one byte, not the whole buffer, so a following valid prefix
                # remains discoverable even after a corrupt length field.
                del self._buffer[0]
                self._discarded_bytes += 1
                self._malformed_lengths += 1
                continue

            if len(self._buffer) < declared:
                # A corrupt but plausible length can otherwise hold a complete
                # following frame hostage. Resynchronise only when a later
                # candidate is itself complete and accepted by the established
                # parser/CRC validation.
                later_start = self._find_valid_later_frame(prefix)
                if later_start is not None:
                    del self._buffer[:later_start]
                    self._discarded_bytes += later_start
                    self._malformed_lengths += 1
                    continue
                break

            completed.append(bytes(self._buffer[:declared]))
            del self._buffer[:declared]
            self._frames += 1

        return tuple(completed)

    def discard_partial(self) -> bool:
        """Discard a stale partial candidate after the session's bounded wait."""
        if not self._buffer:
            return False
        self._discarded_bytes += len(self._buffer)
        self._buffer.clear()
        self._malformed_lengths += 1
        return True

    def _find_valid_later_frame(self, prefix: bytes) -> int | None:
        search_from = 2
        while True:
            candidate = self._buffer.find(prefix, search_from)
            if candidate < 0 or len(self._buffer) - candidate < 6:
                return None
            candidate_length = (
                int.from_bytes(self._buffer[candidate + 4:candidate + 6], "little")
                + 6
            )
            if (
                self._MIN_FRAME_SIZE <= candidate_length <= self._max_frame_size
                and len(self._buffer) - candidate >= candidate_length
            ):
                response = LxpResponse(
                    bytes(self._buffer[candidate:candidate + candidate_length])
                )
                if (
                    not response.packet_error
                    and response.tcp_function == LxpRequestBuilder.TRANSLATED_DATA
                ):
                    return candidate
            search_from = candidate + 2

    def reset(self) -> None:
        """Discard connection-local leftovers before a reconnect."""
        self._buffer.clear()
        self._frames = 0
        self._discarded_bytes = 0
        self._malformed_lengths = 0

    def stats(self) -> FrameDecoderStats:
        """Return immutable decoder diagnostics."""
        return FrameDecoderStats(
            frames=self._frames,
            discarded_bytes=self._discarded_bytes,
            malformed_lengths=self._malformed_lengths,
            buffered_bytes=len(self._buffer),
        )
