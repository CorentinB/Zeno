package queue

import "testing"

func TestNewHandover(t *testing.T) {
	handover := newHandoverChannel()
	if handover == nil {
		t.Fatal("expected handover channel to be created")
	}

	if handover.open.Load() {
		t.Fatal("expected handover channel to be closed")
	}
}

func TestHandoverTryOpen(t *testing.T) {
	handover := newHandoverChannel()
	if !handover.tryOpen(1) {
		t.Fatal("expected handover channel to be opened")
	}

	if !handover.open.Load() {
		t.Fatal("expected handover channel to be open")
	}

	if handover.tryOpen(1) {
		t.Fatal("expected handover channel to not be opened again")
	}
}

func TestHandoverTryClose(t *testing.T) {
	handover := newHandoverChannel()
	if handover.tryClose() {
		t.Fatal("expected handover channel to not be closed")
	}

	if !handover.tryOpen(1) {
		t.Fatal("expected handover channel to be opened")
	}

	if handover.tryClose() {
		t.Fatal("expected tryClose() to fail when handover is not drained")
	}

	if _, ok := handover.tryDrain(); ok {
		t.Fatal("expected handover channel to be drained")
	}

	if handover.tryClose() {
		t.Fatal("expected handover channel to not be closed again")
	}
}

func TestHandoverTryPut(t *testing.T) {
	handover := newHandoverChannel()
	if handover.tryPut(&handoverEncodedItem{}) {
		t.Fatal("expected handover channel to not be open")
	}

	if !handover.tryOpen(1) {
		t.Fatal("expected handover channel to be opened")
	}

	if !handover.tryPut(&handoverEncodedItem{}) {
		t.Fatal("expected handover channel to accept item")
	}

	if handover.tryPut(&handoverEncodedItem{}) {
		t.Fatal("expected handover channel to not accept item")
	}
}

func TestHandoverTryGet(t *testing.T) {
	handover := newHandoverChannel()
	if _, ok := handover.tryGet(); ok {
		t.Fatal("expected handover channel to not be open")
	}

	if !handover.tryOpen(1) {
		t.Fatal("expected handover channel to be opened")
	}

	if _, ok := handover.tryGet(); ok {
		t.Fatal("expected handover channel to not have any items")
	}

	if !handover.tryPut(&handoverEncodedItem{}) {
		t.Fatal("expected handover channel to accept item")
	}

	if _, ok := handover.tryGet(); !ok {
		t.Fatal("expected handover channel to have item")
	}

	if _, ok := handover.tryGet(); ok {
		t.Fatal("expected handover channel to not have any items")
	}
}
