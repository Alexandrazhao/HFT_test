package orderbook

import (
	"fmt"
	"net/url"
	"os"
	"strings"
)

// EndpointConfig centralizes WS/HTTP base endpoints per market with env overrides.
// Env vars (optional):
//   - BINANCE_FUTURES_WS_BASE (default: wss://fstream.binance.com)
//   - BINANCE_FUTURES_HTTP_BASE (default: https://fapi.binance.com)
//   - BINANCE_SPOT_WS_BASE (default: wss://stream.binance.com:9443)
//   - BINANCE_SPOT_HTTP_BASE (default: https://api.binance.com)
type EndpointConfig struct {
	FuturesWSBase   string
	FuturesHTTPBase string
	SpotWSBase      string
	SpotHTTPBase    string
	Venue           string
}

func LoadEndpointConfig() EndpointConfig {
	get := func(key, def string) string {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			return v
		}
		return def
	}

	venue := strings.ToLower(strings.TrimSpace(os.Getenv("BINANCE_VENUE")))
	if venue == "" {
		venue = "us"
	}

	defaults := map[string]string{
		"FuturesWSBase":   "wss://fstream.binance.com",
		"FuturesHTTPBase": "https://fapi.binance.com",
		"SpotWSBase":      "wss://stream.binance.com:9443",
		"SpotHTTPBase":    "https://api.binance.com",
	}

	if venue == "us" || venue == "binanceus" || venue == "usdm" {
		defaults["SpotWSBase"] = "wss://stream.binance.us:9443"
		defaults["SpotHTTPBase"] = "https://api.binance.us"

		// Binance.US does not expose futures streams/snapshot. We keep global defaults
		// here so that futures access remains functional when required.
	}

	return EndpointConfig{
		FuturesWSBase:   get("BINANCE_FUTURES_WS_BASE", defaults["FuturesWSBase"]),
		FuturesHTTPBase: get("BINANCE_FUTURES_HTTP_BASE", defaults["FuturesHTTPBase"]),
		SpotWSBase:      get("BINANCE_SPOT_WS_BASE", defaults["SpotWSBase"]),
		SpotHTTPBase:    get("BINANCE_SPOT_HTTP_BASE", defaults["SpotHTTPBase"]),
		Venue:           venue,
	}
}

// ActiveVenue returns the normalized venue selector (global/us).
func ActiveVenue() string {
	return LoadEndpointConfig().Venue
}

// WSDepthPath returns the depth stream path for a symbol with explicit 100ms update speed.
// Both Spot and Futures support @depth@100ms.
func WSDepthPath(symbol string) string {
	return fmt.Sprintf("/ws/%s@depth@100ms", strings.ToLower(symbol))
}

// WSBookTickerPath returns the bookTicker stream path for a symbol.
func WSBookTickerPath(symbol string) string {
	return fmt.Sprintf("/ws/%s@bookTicker", strings.ToLower(symbol))
}

// HTTPDepthPath returns the HTTP depth endpoint path.
func HTTPDepthPath(market Market) string {
	switch market {
	case MarketSpot:
		return "/api/v3/depth"
	default:
		return "/fapi/v1/depth"
	}
}

// DepthStreamURLFor returns full WS URL for depth stream.
func DepthStreamURLFor(market Market, symbol string) string {
	cfg := LoadEndpointConfig()
	base := cfg.FuturesWSBase
	if market == MarketSpot {
		base = cfg.SpotWSBase
	}
	return base + WSDepthPath(symbol)
}

// BookTickerStreamURLFor returns full WS URL for bookTicker stream.
func BookTickerStreamURLFor(market Market, symbol string) string {
	cfg := LoadEndpointConfig()
	base := cfg.FuturesWSBase
	if market == MarketSpot {
		base = cfg.SpotWSBase
	}
	return base + WSBookTickerPath(symbol)
}

// DepthSnapshotURLFor returns full HTTP URL for depth snapshot (no query parameters).
func DepthSnapshotURLFor(market Market) string {
	cfg := LoadEndpointConfig()
	base := cfg.FuturesHTTPBase
	if market == MarketSpot {
		base = cfg.SpotHTTPBase
	}
	return base + HTTPDepthPath(market)
}

// BuildDepthSnapshotURLWithQuery returns the URL with query parameters applied.
func BuildDepthSnapshotURLWithQuery(market Market, symbol string, limit int) string {
	raw := DepthSnapshotURLFor(market)
	u, _ := url.Parse(raw)
	q := u.Query()
	q.Set("symbol", strings.ToUpper(symbol))
	if limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", limit))
	}
	u.RawQuery = q.Encode()
	return u.String()
}
