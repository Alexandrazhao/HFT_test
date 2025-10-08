package orderbook

import (
	"net/http"
	"strings"
	"sync"
)

type Manager struct {
	mu     sync.Mutex
	client *http.Client
	window float64
	aggs   map[string]*Aggregator
}

func NewManager(client *http.Client, window float64) *Manager {
	return &Manager{
		client: client,
		window: window,
		aggs:   make(map[string]*Aggregator),
	}
}

func (m *Manager) key(symbol string, market Market) string {
	return strings.ToUpper(symbol) + ":" + string(market)
}

func (m *Manager) Get(symbol string, market Market) *Aggregator {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := m.key(symbol, market)
	if agg, ok := m.aggs[key]; ok {
		return agg
	}
	agg := NewAggregator(symbol, market, m.window, m.client)
	agg.Start()
	m.aggs[key] = agg
	return agg
}

func (m *Manager) Health() []map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	reports := make([]map[string]any, 0, len(m.aggs))
	for _, agg := range m.aggs {
		reports = append(reports, agg.Health())
	}
	return reports
}

// ResetIndicators resets indicator state for a given symbol/market (e.g., when OB#1 band changes)
func (m *Manager) ResetIndicators(symbol string, market Market) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := m.key(symbol, market)
	if agg, ok := m.aggs[key]; ok {
		agg.ResetIndicators()
	}
}
