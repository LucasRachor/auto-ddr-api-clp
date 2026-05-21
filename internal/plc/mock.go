package plc

import (
	"context"
	"sync"
)

// Mock é uma impl in-memory para testes do servidor gRPC sem CLP físico.
type Mock struct {
	mu     sync.RWMutex
	tags   map[string]TagValue
	status Status
}

func NewMock() *Mock {
	return &Mock{
		tags:   make(map[string]TagValue),
		status: Status{Mode: "PROGRAM"},
	}
}

func (m *Mock) Connect(context.Context) error { return nil }
func (m *Mock) Close() error                   { return nil }

func (m *Mock) Start(context.Context) error {
	m.mu.Lock()
	m.status.Mode = "RUN"
	m.mu.Unlock()
	return nil
}

func (m *Mock) Stop(context.Context) error {
	m.mu.Lock()
	m.status.Mode = "PROGRAM"
	m.mu.Unlock()
	return nil
}

func (m *Mock) Reset(context.Context) error {
	m.mu.Lock()
	m.status.Faulted = false
	m.status.Detail = ""
	m.mu.Unlock()
	return nil
}

func (m *Mock) ReadTag(_ context.Context, tag string) (TagValue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if v, ok := m.tags[tag]; ok {
		return v, nil
	}
	return TagValue{}, nil
}

func (m *Mock) WriteTag(_ context.Context, tag string, v TagValue) error {
	m.mu.Lock()
	m.tags[tag] = v
	m.mu.Unlock()
	return nil
}

func (m *Mock) Status(context.Context) (Status, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.status, nil
}
