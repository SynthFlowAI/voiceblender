package wsmedia

import (
	"bytes"
	"io"
	"testing"
	"time"
)

func TestStreamBufferRoundTrip(t *testing.T) {
	sb := newStreamBuffer(1024, 20)
	in := bytes.Repeat([]byte{0xAB}, 320)
	n, err := sb.Write(in)
	if err != nil || n != 320 {
		t.Fatalf("write: n=%d err=%v", n, err)
	}
	out := make([]byte, 320)
	n, err = sb.Read(out)
	if err != nil || n != 320 {
		t.Fatalf("read: n=%d err=%v", n, err)
	}
	if !bytes.Equal(in, out) {
		t.Fatal("data mismatch")
	}
}

func TestStreamBufferDropsOnOverflow(t *testing.T) {
	sb := newStreamBuffer(640, 20) // 2 frames at 20ms@16kHz
	frame := bytes.Repeat([]byte{0xCD}, 640)
	// First write fits exactly.
	if _, err := sb.Write(frame); err != nil {
		t.Fatalf("write 1: %v", err)
	}
	// Second write would exceed capacity — must be dropped silently.
	if _, err := sb.Write(frame); err != nil {
		t.Fatalf("write 2: %v", err)
	}
	if got := sb.Dropped(); got != 640 {
		t.Fatalf("drops=%d, want 640", got)
	}
	// Read should still see only the first frame's worth.
	out := make([]byte, 640)
	if _, err := sb.Read(out); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(out, frame) {
		t.Fatal("first frame corrupted")
	}
}

func TestStreamBufferCloseUnblocksRead(t *testing.T) {
	sb := newStreamBuffer(1024, 20)
	out := make([]byte, 100)
	done := make(chan error, 1)
	go func() {
		_, err := sb.Read(out)
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	sb.Close()
	select {
	case err := <-done:
		if err != io.EOF {
			t.Fatalf("want io.EOF, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Read did not unblock after Close")
	}
}

func TestStreamBufferDoesNotPaceReads(t *testing.T) {
	// Mixer owns the 20 ms clock; streambuf must not Sleep between reads.
	sb := newStreamBuffer(4096, 20)
	frame := bytes.Repeat([]byte{1}, 100)
	for range 3 {
		if _, err := sb.Write(frame); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	out := make([]byte, 100)
	if _, err := sb.Read(out); err != nil {
		t.Fatalf("read 1: %v", err)
	}
	start := time.Now()
	if _, err := sb.Read(out); err != nil {
		t.Fatalf("read 2: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed >= 10*time.Millisecond {
		t.Fatalf("unexpected pacing sleep: %v", elapsed)
	}
}

func TestStreamBufferPlayoutWarmsThenReleases(t *testing.T) {
	const (
		frameBytes   = 320 // 20ms @ 16kHz s16le
		playoutBytes = 640 // 40ms lead
	)
	sb := newStreamBufferPlayout(4096, 20, playoutBytes)
	frameA := bytes.Repeat([]byte{0x11}, frameBytes)
	frameB := bytes.Repeat([]byte{0x22}, frameBytes)
	frameC := bytes.Repeat([]byte{0x33}, frameBytes)

	out := make([]byte, frameBytes)
	started := make(chan struct{})
	errC := make(chan error, 1)
	go func() {
		close(started)
		_, err := sb.Read(out)
		errC <- err
	}()
	<-started
	time.Sleep(20 * time.Millisecond) // still warming — Read must block
	select {
	case err := <-errC:
		t.Fatalf("warm Read returned early: %v", err)
	default:
	}

	if _, err := sb.Write(frameA); err != nil {
		t.Fatalf("write A: %v", err)
	}
	if _, err := sb.Write(frameB); err != nil {
		t.Fatalf("write B: %v", err)
	}
	select {
	case err := <-errC:
		if err != nil {
			t.Fatalf("post-warm read: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("post-warm Read blocked")
	}
	if !bytes.Equal(out, frameA) {
		t.Fatalf("want frame A after warm-up, got %x", out[:8])
	}

	if _, err := sb.Write(frameC); err != nil {
		t.Fatalf("write C: %v", err)
	}
	n, err := sb.Read(out)
	if err != nil || n != frameBytes {
		t.Fatalf("read B: n=%d err=%v", n, err)
	}
	if !bytes.Equal(out, frameB) {
		t.Fatalf("want frame B, got %x", out[:8])
	}
}

func TestStreamBufferPlayoutUnderrunBlocksUntilData(t *testing.T) {
	const frameBytes = 320
	sb := newStreamBufferPlayout(4096, 20, frameBytes) // 20ms lead
	frame := bytes.Repeat([]byte{0x44}, frameBytes)
	next := bytes.Repeat([]byte{0x55}, frameBytes)
	if _, err := sb.Write(frame); err != nil {
		t.Fatalf("write: %v", err)
	}
	out := make([]byte, frameBytes)
	if _, err := sb.Read(out); err != nil {
		t.Fatalf("warm release: %v", err)
	}
	if !bytes.Equal(out, frame) {
		t.Fatal("expected real frame after warm-up")
	}

	errC := make(chan error, 1)
	go func() {
		_, err := sb.Read(out)
		errC <- err
	}()
	time.Sleep(30 * time.Millisecond)
	select {
	case err := <-errC:
		t.Fatalf("underrun Read returned early (must block, not invent): %v", err)
	default:
	}
	if _, err := sb.Write(next); err != nil {
		t.Fatalf("write next: %v", err)
	}
	select {
	case err := <-errC:
		if err != nil {
			t.Fatalf("underrun read: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("underrun Read stayed blocked after write")
	}
	if !bytes.Equal(out, next) {
		t.Fatal("expected next real frame after underrun wait")
	}
}
