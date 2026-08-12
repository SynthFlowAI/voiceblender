package wsmedia

import (
	"io"
	"sync"
)

// streamBuffer accepts variable-sized writes and provides blocking reads.
// The recv loop writes inbound PCM here; the mixer readLoop drains it into
// Participant.incoming. Capacity is bounded — writes that would exceed it
// discard the incoming bytes and increment a drop counter so the recv loop
// can record the loss.
//
// When playoutBytes > 0 the buffer acts as a fixed-delay WS jitter /
// playout buffer: Read blocks until the target lead is buffered, then
// returns frames as they become available. Empty after warm-up blocks
// rather than inventing silence/hold — the room mixer is the sole 20 ms
// clock (mixTick hold-last covers brief underruns). A second Sleep-based
// pace here used to phase-skew against mixTick and splice mid-word zeros.
//
// Adapted from internal/api/agent.go's streamBuffer with a fixed capacity
// for drop-on-overflow semantics.
type streamBuffer struct {
	mu      sync.Mutex
	cond    *sync.Cond
	buf     []byte
	cap     int
	dropped int64
	closed  bool

	// playoutBytes is the warm-up / target lead (0 = passthrough: block
	// until a full Read is available, matching historical behaviour).
	playoutBytes int
	warming      bool
}

func newStreamBuffer(capBytes int, frameMs int) *streamBuffer {
	return newStreamBufferPlayout(capBytes, frameMs, 0)
}

func newStreamBufferPlayout(capBytes int, frameMs int, playoutBytes int) *streamBuffer {
	_ = frameMs // retained for call-site compatibility; mixer owns pacing
	if playoutBytes < 0 {
		playoutBytes = 0
	}
	if playoutBytes > capBytes {
		playoutBytes = capBytes
	}
	sb := &streamBuffer{
		cap:          capBytes,
		playoutBytes: playoutBytes,
		warming:      playoutBytes > 0,
	}
	sb.cond = sync.NewCond(&sb.mu)
	return sb
}

// Write appends p to the buffer. If the buffer would exceed its capacity,
// the entire incoming write is dropped (drop-oldest would chop a frame in
// half and produce audio artifacts; whole-frame drop is the right call for
// 20ms audio frames). Always returns (len(p), nil) to satisfy io.Writer.
func (sb *streamBuffer) Write(p []byte) (int, error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	if sb.closed {
		return len(p), nil
	}
	if len(sb.buf)+len(p) > sb.cap {
		sb.dropped += int64(len(p))
		return len(p), nil
	}
	sb.buf = append(sb.buf, p...)
	sb.cond.Signal()
	return len(p), nil
}

// Read blocks until len(p) bytes are buffered or the buffer is closed.
// With playout enabled, the first successful read waits until the warm-up
// lead is buffered; after that it behaves like passthrough. Never invents
// silence or hold-last frames — that is the mixer's job.
func (sb *streamBuffer) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	sb.mu.Lock()
	defer sb.mu.Unlock()

	if sb.playoutBytes > 0 && sb.warming {
		for len(sb.buf) < sb.playoutBytes && !sb.closed {
			sb.cond.Wait()
		}
		if sb.closed && len(sb.buf) < sb.playoutBytes {
			sb.buf = sb.buf[:0]
			return 0, io.EOF
		}
		sb.warming = false
	}

	for len(sb.buf) < len(p) && !sb.closed {
		sb.cond.Wait()
	}
	if len(sb.buf) == 0 && sb.closed {
		return 0, io.EOF
	}
	if sb.closed && len(sb.buf) < len(p) {
		// Drop a trailing partial frame on close rather than returning a short read.
		sb.buf = sb.buf[:0]
		return 0, io.EOF
	}

	n := copy(p, sb.buf)
	remaining := copy(sb.buf, sb.buf[n:])
	sb.buf = sb.buf[:remaining]
	return n, nil
}

// Close signals the reader to return io.EOF and stops accepting writes.
func (sb *streamBuffer) Close() {
	sb.mu.Lock()
	sb.closed = true
	sb.cond.Broadcast()
	sb.mu.Unlock()
}

// Dropped returns the cumulative count of bytes discarded on overflow.
func (sb *streamBuffer) Dropped() int64 {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.dropped
}
