package server

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"orderbook_turnover/internal/orderbook"
)

type Server struct {
	mux        *http.ServeMux
	aggManager *orderbook.Manager
}

func New() (*Server, error) {
	static, err := staticHandler()
	if err != nil {
		return nil, err
	}
	client := &http.Client{
		Timeout: 8 * time.Second,
		Transport: &http.Transport{
			TLSHandshakeTimeout:   5 * time.Second,
			ResponseHeaderTimeout: 5 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
	mgr := orderbook.NewManager(client, 1000)
	srv := &Server{
		mux:        http.NewServeMux(),
		aggManager: mgr,
	}
	srv.routes(static)
	return srv, nil
}

func (s *Server) routes(static http.Handler) {
	s.mux.Handle("/stream/turnover", http.HandlerFunc(s.handleTurnoverStream))
	s.mux.Handle("/api/defaults", http.HandlerFunc(s.handleDefaults))
	s.mux.Handle("/healthz", http.HandlerFunc(s.handleHealth))
	s.mux.Handle("/", static)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleTurnoverStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		log.Printf("/stream/turnover flusher unsupported: %T", w)
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	symbol, market := parseSymbolMarket(r)
	window := parseWindow(r, 1000)
	agg := s.aggManager.Get(symbol, market)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ctx := r.Context()
	id, ch := agg.RegisterIndicator(window, 60)
	defer agg.UnregisterIndicator(id)

    type payload struct {
        Timestamp       int64                       `json:"ts"`
        TurnoverRate    orderbook.TurnoverRate      `json:"turnoverRate"`
        TurnoverHistory []orderbook.TurnoverHistory `json:"turnoverHistory"`
        CumBins         []float64                   `json:"cumBins"`
    }

	for {
		select {
		case indicator, ok := <-ch:
			if !ok {
				return
			}
            data, err := json.Marshal(payload{
                Timestamp:       indicator.Timestamp,
                TurnoverRate:    indicator.TurnoverRate,
                TurnoverHistory: indicator.TurnoverHistory,
                CumBins:         indicator.CumBins,
            })
			if err != nil {
				continue
			}
			w.Write([]byte("event: turnover\n"))
			w.Write([]byte("data: "))
			w.Write(data)
			w.Write([]byte("\n\n"))
			flusher.Flush()
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"aggregators": s.aggManager.Health(),
		"time":        time.Now().UTC(),
	})
}

func (s *Server) handleDefaults(w http.ResponseWriter, r *http.Request) {
	sym := strings.ToUpper(strings.TrimSpace(os.Getenv("DEFAULT_SYMBOL")))
	if sym == "" {
		sym = "BTCUSDT"
	}
	raw := strings.ToLower(strings.TrimSpace(os.Getenv("DEFAULT_MARKET")))
	mkt := orderbook.Market(raw)
	switch mkt {
	case orderbook.MarketSpot, orderbook.MarketFutures:
	default:
		mkt = orderbook.MarketSpot
	}
	venue := orderbook.ActiveVenue()
	if venue == "us" {
		mkt = orderbook.MarketSpot
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"symbol": sym,
		"market": string(mkt),
		"venue":  venue,
	})
}

func parseSymbolMarket(r *http.Request) (string, orderbook.Market) {
	symbol := strings.ToUpper(r.URL.Query().Get("symbol"))
	if symbol == "" {
		if env := strings.TrimSpace(strings.ToUpper(os.Getenv("DEFAULT_SYMBOL"))); env != "" {
			symbol = env
		} else {
			symbol = "BTCUSDT"
		}
	}
	rawMarket := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("market")))
	if rawMarket == "" {
		rawMarket = strings.TrimSpace(strings.ToLower(os.Getenv("DEFAULT_MARKET")))
	}
	market := orderbook.Market(rawMarket)
	switch market {
	case orderbook.MarketSpot, orderbook.MarketFutures:
	default:
		market = orderbook.MarketSpot
	}
	if orderbook.ActiveVenue() == "us" {
		market = orderbook.MarketSpot
	}
	return symbol, market
}

func parseWindow(r *http.Request, def float64) float64 {
	window := def
	if raw := r.URL.Query().Get("window"); raw != "" {
		if v, err := strconv.ParseFloat(raw, 64); err == nil && v > 0 {
			window = v
		}
	}
	return window
}
