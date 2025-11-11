package orderbook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

    "nhooyr.io/websocket"
)

const (
	// Reorder/hold window for 100ms diff stream (t_buf)
	queueWindow   = 500 * time.Millisecond
	bridgeWait    = 750 * time.Millisecond
	maxBackoff    = 5 * time.Second
	minBackoff    = 500 * time.Millisecond
	priceHorizon  = time.Hour
	fanoutTickDur = 100 * time.Millisecond
	gapGrace      = 500 * time.Millisecond
)

// Market represents the upstream Binance venue.
type Market string

const (
	MarketFutures Market = "futures"
	MarketSpot    Market = "spot"
)

// DepthDiff mirrors Binance depth update payload.
type DepthDiff struct {
	EventType string     `json:"e"`
	EventTime int64      `json:"E"`
	Symbol    string     `json:"s"`
	FirstID   int64      `json:"U"`
	FinalID   int64      `json:"u"`
	PrevFinal *int64     `json:"pu"`
	Bids      [][]string `json:"b"`
	Asks      [][]string `json:"a"`
}

type bookTickerMessage struct {
	UpdateID  any    `json:"u"`
	EventTime any    `json:"E"`
	TradeTime any    `json:"T"`
	Symbol    string `json:"s"`
	BidPrice  string `json:"b"`
	BidQty    string `json:"B"`
	AskPrice  string `json:"a"`
	AskQty    string `json:"A"`
}

type depthSnapshot struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
}

// BookLevel is the compact representation pushed to the front-end.
type BookLevel struct {
	Side  string  `json:"side"`
	Price float64 `json:"price"`
	Qty   float64 `json:"qty"`
}

// Snapshot is served via HTTP for initial render.
type Snapshot struct {
	Timestamp int64       `json:"ts"`
	Mid       float64     `json:"mid"`
	Levels    []BookLevel `json:"levels"`
}

// BookPatch is emitted over SSE for incremental updates.
type BookPatch struct {
	Timestamp int64       `json:"ts"`
	Mid       float64     `json:"mid"`
	Patch     []BookLevel `json:"patch"`
}

// PricePoint captures a mid-price sample for the chart.
type PricePoint struct {
    Time int64   `json:"t"`
    Mid  float64 `json:"mid"`
}

// tradeBucket accumulates trade flow until a target quote volume is reached
type tradeBucket struct {
    startMS int64
    endMS   int64
    buyQ    float64 // quote volume
    sellQ   float64 // quote volume
    volQ    float64 // total quote vol
}

// IndicatorData represents the calculated indicators for orderbook analysis
type IndicatorData struct {
    Timestamp       int64             `json:"ts"`
    Imbalance       float64           `json:"imbalance"`
    VPIN            float64           `json:"vpin"`
    TurnoverRate    TurnoverRate      `json:"turnoverRate"`
    TurnoverHistory []TurnoverHistory `json:"turnoverHistory"`
    // Cumulative volume bins (100 contracts per bin) around mid
    // Each entry is the absolute buy/sell volume difference within the bin.
    CumBins []float64 `json:"cumBins"`
    // Debug fields
    VPINBucketCount int   `json:"vpinBucketCount"`
    VPINLastBucket  int64 `json:"vpinLastBucket"`
}

// TurnoverRate represents real-time turnover metrics
type TurnoverRate struct {
	BidPlacements int     `json:"bidPlacements"`
	AskPlacements int     `json:"askPlacements"`
	TotalChanges  int     `json:"totalChanges"`
	OrdersPerMin  float64 `json:"ordersPerMin"`
}

// TurnoverHistory represents historical turnover data for the chart
type TurnoverHistory struct {
	Timestamp   int64   `json:"t"`
	BidVolume   float64 `json:"bidVolume"`
	AskVolume   float64 `json:"askVolume"`
	TotalVolume float64 `json:"totalVolume"`
	OrdersCount int     `json:"ordersCount"`
	Imbalance   float64 `json:"imbalance"`
}

type minuteTurnover struct {
    bidPlacements int
    bidRemovals   int
    askPlacements int
    askRemovals   int
    ordersCount   int
}

type aggregatorState int

const (
	stateIdle aggregatorState = iota
	stateWarmingUp
	stateBridging
	stateReady
	stateResync
)

func (s aggregatorState) String() string {
	switch s {
	case stateIdle:
		return "IDLE"
	case stateWarmingUp:
		return "WARMING_UP"
	case stateBridging:
		return "BRIDGING"
	case stateReady:
		return "READY"
	case stateResync:
		return "RESYNC"
	default:
		return "UNKNOWN"
	}
}

type queuedDiff struct {
	diff    DepthDiff
	arrived time.Time
}

type bookSubscriber struct {
	ch     chan BookPatch
	window float64
}

type priceSubscriber struct {
	ch chan PricePoint
}

type indicatorSubscriber struct {
	ch         chan IndicatorData
	window     float64
	historyLen int
}

type bookKey struct {
	side  string
	price float64
}

type bookUpdate struct {
	ts      int64
	mid     float64
	changes []BookLevel
}

// Aggregator owns the order book state and streaming fan-out for one symbol.
type Aggregator struct {
	symbol string
	market Market
	window float64
	client *http.Client

	ctx    context.Context
	cancel context.CancelFunc

	stateMu sync.RWMutex
	state   aggregatorState

	mu        sync.RWMutex
	bids      map[float64]float64
	asks      map[float64]float64
	lastID    int64
	bookMid   float64
	tickerMid float64
	tickerTS  time.Time
	lastFrame time.Time
	lastSnap  time.Time

	queueMu sync.Mutex
	queue   []queuedDiff

	priceMu   sync.RWMutex
	priceRing []PricePoint

	turnoverMu    sync.RWMutex
	turnoverRing  []TurnoverHistory
	minuteTracker map[int64]*minuteTurnover

	subMu         sync.Mutex
	nextSubID     int
	bookSubs      map[int]*bookSubscriber
	priceSubs     map[int]*priceSubscriber
	indicatorSubs map[int]*indicatorSubscriber

	bookUpdates      chan bookUpdate
	pricePoints      chan PricePoint
    indicatorUpdates chan IndicatorData

    // trades / VPIN (trade-based)
    tradeMu         sync.Mutex
    tradeBuckets    []tradeBucket // rolling buckets (newest at end)
    currentBucket   tradeBucket
    vpinBucketQuote float64 // quote volume per bucket (e.g., 50000)
    vpinWindowMin   int     // minutes to average over
    vpinUseBase     bool    // if true, bucket by base qty; else quote notional
    vpinBucketBase  float64 // base units per bucket when in base mode

    // Butterworth low-pass filter on signed trade flow (pre-VPIN)
    butterEnabled bool
    butterCutoff  float64 // normalized cutoff (0<fc<0.5) at 1 Hz sampling
    // biquad coefficients/state
    bw_b0, bw_b1, bw_b2 float64
    bw_a1, bw_a2        float64
    bw_x1, bw_x2        float64
    bw_y1, bw_y2        float64
    // 1 Hz sampler for signed flow
    sampSecond int64   // unix second for current bin
    sampAccum  float64 // accumulated signed volume within current second

	logMu   sync.Mutex
	diffLog *os.File
	snapLog *os.File
	gapLog  *os.File
}

func NewAggregator(symbol string, market Market, window float64, client *http.Client) *Aggregator {
	ctx, cancel := context.WithCancel(context.Background())
	return &Aggregator{
		symbol:           strings.ToUpper(symbol),
		market:           market,
		window:           window,
		client:           client,
		ctx:              ctx,
		cancel:           cancel,
		bids:             make(map[float64]float64),
		asks:             make(map[float64]float64),
		priceRing:        make([]PricePoint, 0, 4096),
		turnoverRing:     make([]TurnoverHistory, 0, 60), // 60 minutes of history
		minuteTracker:    make(map[int64]*minuteTurnover),
		bookSubs:         make(map[int]*bookSubscriber),
		priceSubs:        make(map[int]*priceSubscriber),
		indicatorSubs:    make(map[int]*indicatorSubscriber),
		bookUpdates:      make(chan bookUpdate, 256),
		pricePoints:      make(chan PricePoint, 256),
        indicatorUpdates: make(chan IndicatorData, 256),
        vpinBucketQuote:  50000,
        vpinWindowMin:    60,
        vpinUseBase:      false,
        vpinBucketBase:   10, // default e.g., 10 base units
        butterEnabled:    false,
        butterCutoff:     0.05,
    }
}

func (a *Aggregator) Start() {
    go a.runDepth()
    go a.runTicker()
    go a.runAggTrades()
    go a.bookFanoutLoop()
    go a.priceFanoutLoop()
    go a.indicatorFanoutLoop()
}

func (a *Aggregator) Stop() {
    a.cancel()
    // flush any pending 1 Hz sample through the filter when stopping
    a.tradeMu.Lock()
    if a.butterEnabled && a.sampSecond != 0 {
        filtered := a.butterworthStep(a.sampAccum)
        if filtered >= 0 {
            a.appendTrade(a.sampSecond*1000, filtered, false)
        } else {
            a.appendTrade(a.sampSecond*1000, -filtered, true)
        }
        a.sampSecond = 0
        a.sampAccum = 0
    }
    a.tradeMu.Unlock()
    a.closeLogs()
}

// SetVPINBucketQuote sets the target quote volume per VPIN bucket.
func (a *Aggregator) SetVPINBucketQuote(q float64) {
    if q <= 0 {
        return
    }
    a.tradeMu.Lock()
    a.vpinBucketQuote = q
    a.tradeMu.Unlock()
}

// SetVPINWindowMin sets the averaging window for trade VPIN in minutes.
func (a *Aggregator) SetVPINWindowMin(m int) {
    if m <= 0 {
        return
    }
    a.tradeMu.Lock()
    a.vpinWindowMin = m
    a.tradeMu.Unlock()
}

// SetVPINUseBase toggles VPIN bucket mode between base (true) and quote (false).
func (a *Aggregator) SetVPINUseBase(useBase bool) {
    a.tradeMu.Lock()
    a.vpinUseBase = useBase
    a.tradeMu.Unlock()
}

// SetVPINBucketBase sets the base-volume bucket size when in base mode.
func (a *Aggregator) SetVPINBucketBase(b float64) {
    if b <= 0 {
        return
    }
    a.tradeMu.Lock()
    a.vpinBucketBase = b
    a.tradeMu.Unlock()
}

// EnableButterworth enables a 2nd-order Butterworth LPF on signed trade flow.
// cutoff is normalized to 1 Hz sample (0<cutoff<0.5). When disabled, raw trades are used.
func (a *Aggregator) EnableButterworth(enable bool, cutoff float64) {
    a.tradeMu.Lock()
    defer a.tradeMu.Unlock()
    a.butterEnabled = enable
    if cutoff <= 0 || cutoff >= 0.5 {
        cutoff = 0.05
    }
    a.butterCutoff = cutoff
    a.initButterCoeffs()
}

func (a *Aggregator) initButterCoeffs() {
    // 2nd-order Butterworth LPF via bilinear transform; normalized sample rate 1 Hz
    fc := a.butterCutoff
    if fc <= 0 { fc = 0.05 }
    wc := math.Tan(math.Pi * fc)
    k1 := math.Sqrt2 * wc
    k2 := wc * wc
    den := 1 + k1 + k2
    a.bw_b0 = k2 / den
    a.bw_b1 = 2 * a.bw_b0
    a.bw_b2 = a.bw_b0
    a.bw_a1 = 2 * (k2 - 1) / den
    a.bw_a2 = (1 - k1 + k2) / den
    // reset state
    a.bw_x1, a.bw_x2 = 0, 0
    a.bw_y1, a.bw_y2 = 0, 0
}

func (a *Aggregator) butterworthStep(x float64) float64 {
    // Direct Form I biquad
    y := a.bw_b0*x + a.bw_b1*a.bw_x1 + a.bw_b2*a.bw_x2 - a.bw_a1*a.bw_y1 - a.bw_a2*a.bw_y2
    a.bw_x2 = a.bw_x1
    a.bw_x1 = x
    a.bw_y2 = a.bw_y1
    a.bw_y1 = y
    return y
}

func (a *Aggregator) runDepth() {
	backoff := minBackoff
	for {
		a.setState(stateWarmingUp)
		if err := a.syncLoop(); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			fmt.Printf("aggregator(%s): %v\n", a.symbol, err)
		}
		select {
		case <-a.ctx.Done():
			return
		case <-time.After(addJitter(backoff)):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func (a *Aggregator) runTicker() {
	backoff := minBackoff
	for {
		if err := a.tickerLoop(); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			fmt.Printf("aggregator(%s) ticker: %v\n", a.symbol, err)
		}
		select {
		case <-a.ctx.Done():
			return
		case <-time.After(addJitter(backoff)):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// runAggTrades connects to Binance aggTrades stream and accumulates trade-based volume buckets
func (a *Aggregator) runAggTrades() {
    backoff := minBackoff
    for {
        if err := a.aggTradesLoop(); err != nil {
            if errors.Is(err, context.Canceled) {
                return
            }
            fmt.Printf("aggregator(%s) aggTrades: %v\n", a.symbol, err)
        }
        select {
        case <-a.ctx.Done():
            return
        case <-time.After(addJitter(backoff)):
        }
        if backoff < maxBackoff {
            backoff *= 2
            if backoff > maxBackoff {
                backoff = maxBackoff
            }
        }
    }
}

func (a *Aggregator) aggTradesLoop() error {
    ctx, cancel := context.WithCancel(a.ctx)
    defer cancel()
    streamURL := AggTradesStreamURLFor(a.market, a.symbol)
    ws, _, err := websocket.Dial(ctx, streamURL, nil)
    if err != nil {
        return err
    }
    ws.SetReadLimit(1 << 20)
    defer ws.Close(websocket.StatusNormalClosure, "shutdown")

    type aggTrade struct {
        Price string `json:"p"`
        Qty   string `json:"q"`
        Time  int64  `json:"T"`
        Maker bool   `json:"m"`
    }

    for {
        _, data, err := ws.Read(ctx)
        if err != nil {
            if websocket.CloseStatus(err) == websocket.StatusNormalClosure || errors.Is(err, context.Canceled) {
                return nil
            }
            return err
        }
        var t aggTrade
        if err := json.Unmarshal(data, &t); err != nil {
            continue
        }
        price, _ := strconv.ParseFloat(t.Price, 64)
        qty, _ := strconv.ParseFloat(t.Qty, 64)
        if t.Time == 0 || qty <= 0 || price <= 0 {
            continue
        }
        // Use base or quote per mode
        var vol float64
        a.tradeMu.Lock()
        useBase := a.vpinUseBase
        butter := a.butterEnabled
        a.tradeMu.Unlock()
        if useBase {
            vol = qty
        } else {
            vol = price * qty
        }
        // Signed flow: buy positive, sell negative (maker=true means sell)
        signed := vol
        if t.Maker { signed = -vol }
        if butter {
            // Fixed 1 Hz sampling: accumulate signed flow within the trade's second,
            // on second change, run one Butterworth step with the bin sum.
            sec := t.Time / 1000
            a.tradeMu.Lock()
            if a.sampSecond == 0 {
                a.sampSecond = sec
            }
            if sec != a.sampSecond {
                // emit filtered value for previous second
                filtered := a.butterworthStep(a.sampAccum)
                // route to buckets preserving sign
                if filtered >= 0 {
                    a.appendTrade(a.sampSecond*1000, filtered, false)
                } else {
                    a.appendTrade(a.sampSecond*1000, -filtered, true)
                }
                // reset for new second
                a.sampSecond = sec
                a.sampAccum = 0
            }
            a.sampAccum += signed
            a.tradeMu.Unlock()
        } else {
            a.appendTrade(t.Time, vol, t.Maker)
        }
    }
}

func (a *Aggregator) appendTrade(tsMS int64, vol float64, maker bool) {
    a.tradeMu.Lock()
    defer a.tradeMu.Unlock()
    // init bucket start
    if a.currentBucket.startMS == 0 {
        a.currentBucket.startMS = tsMS
    }
    if maker {
        // seller-initiated => sell volume
        a.currentBucket.sellQ += vol
    } else {
        a.currentBucket.buyQ += vol
    }
    // Track bucketed volume in the correct unit per mode
    // When using base mode, vol is base units; otherwise vol is quote notional
    a.currentBucket.volQ += vol
    a.currentBucket.endMS = tsMS

    // pick threshold depending on mode (same unit as `vol` above)
    threshold := a.vpinBucketQuote
    if a.vpinUseBase && a.vpinBucketBase > 0 {
        threshold = a.vpinBucketBase
    }
    if a.currentBucket.volQ >= threshold && threshold > 0 {
        // finalize bucket
        done := a.currentBucket
        a.tradeBuckets = append(a.tradeBuckets, done)
        // cap buckets to window (approx minutes -> assume target avg bucket duration variable; keep last N buckets ~= window*60s)
        if len(a.tradeBuckets) > 600 { // safety cap
            a.tradeBuckets = a.tradeBuckets[len(a.tradeBuckets)-600:]
        }
        // reset
        a.currentBucket = tradeBucket{}
    }
}

func (a *Aggregator) calculateTradeVPIN() float64 {
    a.tradeMu.Lock()
    defer a.tradeMu.Unlock()
    if len(a.tradeBuckets) == 0 {
        return 0
    }
    // Consider only buckets whose midpoint time is within the last vpinWindowMin minutes
    cutoff := time.Now().Add(-time.Duration(a.vpinWindowMin) * time.Minute).UnixMilli()
    var sum, count float64
    for i := len(a.tradeBuckets) - 1; i >= 0; i-- {
        b := a.tradeBuckets[i]
        mid := b.endMS
        if mid < cutoff {
            break
        }
        // Normalize by actual filled bucket volume to ensure <= 1
        denom := b.volQ
        if denom <= 0 {
            continue
        }
        v := math.Abs(b.buyQ-b.sellQ) / denom
        if v < 0 { v = 0 }
        if v > 1 { v = 1 }
        sum += v
        count++
    }
    if count == 0 {
        return 0
    }
    avg := sum / count
    if avg < 0 { avg = 0 }
    if avg > 1 { avg = 1 }
    return avg
}

func (a *Aggregator) syncLoop() error {
	const maxRetries = 3
	const initialBackoff = 1 * time.Second

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(attempt) * initialBackoff
			fmt.Printf("aggregator(%s) sync retry %d/%d in %v\n", a.symbol, attempt+1, maxRetries, backoff)
			select {
			case <-a.ctx.Done():
				return a.ctx.Err()
			case <-time.After(backoff):
			}
		}

		ctx, cancel := context.WithCancel(a.ctx)
		defer cancel()

		streamURL := a.depthStreamURL()
		ws, _, err := websocket.Dial(ctx, streamURL, nil)
		if err != nil {
			lastErr = fmt.Errorf("dial stream (attempt %d/%d): %w", attempt+1, maxRetries, err)
			fmt.Printf("aggregator(%s) stream dial failed: %v\n", a.symbol, err)
			continue
		}
		ws.SetReadLimit(1048576) // 1MB
		defer ws.Close(websocket.StatusNormalClosure, "shutdown")

		diffCh := make(chan DepthDiff, 256)
		recvErr := make(chan error, 1)
		go func() {
			recvErr <- a.consumeStream(ctx, ws, diffCh)
		}()

		// Fetch HTTP snapshot to initialize order book
		fmt.Printf("aggregator(%s) fetching HTTP snapshot\n", a.symbol)
		if err := a.fetchSnapshot(ctx); err != nil {
			lastErr = fmt.Errorf("fetch snapshot failed: %w", err)
			fmt.Printf("aggregator(%s) snapshot fetch failed: %v\n", a.symbol, err)
			continue
		}
		a.setState(stateBridging)

		bridgeTimer := time.NewTimer(bridgeWait)
		defer bridgeTimer.Stop()

		for {
			select {
			case diff := <-diffCh:
				a.enqueue(diff)
				if a.currentState() == stateBridging {
					if a.tryBridge() {
						a.setState(stateReady)
						if err := a.drainReadyQueue(); err != nil {
							a.setState(stateResync)
							return err
						}
						fmt.Printf("aggregator(%s) READY lastUpdateId=%d\n", a.symbol, a.getLastID())
						if !bridgeTimer.Stop() {
							select {
							case <-bridgeTimer.C:
							default:
							}
						}
					} else {
						if !bridgeTimer.Stop() {
							select {
							case <-bridgeTimer.C:
							default:
							}
						}
						bridgeTimer.Reset(bridgeWait)
					}
				} else if a.currentState() == stateReady {
					if err := a.drainReadyQueue(); err != nil {
						a.setState(stateResync)
						return err
					}
				}
			case <-bridgeTimer.C:
				if a.currentState() == stateBridging {
					if a.tryBridge() {
						a.setState(stateReady)
						if err := a.drainReadyQueue(); err != nil {
							a.setState(stateResync)
							return err
						}
						fmt.Printf("aggregator(%s) READY lastUpdateId=%d\n", a.symbol, a.getLastID())
					} else {
						a.setState(stateResync)
						return errors.New("bridge timeout")
					}
				}
			case err := <-recvErr:
				if err != nil {
					fmt.Printf("aggregator(%s) stream error: %v\n", a.symbol, err)
					// For stream errors, break to retry unless it's the last attempt
					if attempt < maxRetries-1 {
						break
					}
					return err
				}
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	// All attempts failed
	return fmt.Errorf("sync loop failed after %d attempts, last error: %w", maxRetries, lastErr)
}

func (a *Aggregator) tickerLoop() error {
	const maxRetries = 3
	const initialBackoff = 1 * time.Second

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(attempt) * initialBackoff
			fmt.Printf("aggregator(%s) ticker retry %d/%d in %v\n", a.symbol, attempt+1, maxRetries, backoff)
			select {
			case <-a.ctx.Done():
				return a.ctx.Err()
			case <-time.After(backoff):
			}
		}

		ctx, cancel := context.WithCancel(a.ctx)
		defer cancel()

		streamURL := a.bookTickerStreamURL()
		ws, _, err := websocket.Dial(ctx, streamURL, nil)
		if err != nil {
			lastErr = fmt.Errorf("dial bookTicker (attempt %d/%d): %w", attempt+1, maxRetries, err)
			fmt.Printf("aggregator(%s) ticker dial failed: %v\n", a.symbol, err)
			continue
		}
		ws.SetReadLimit(1048576) // 1MB
		defer ws.Close(websocket.StatusNormalClosure, "shutdown")

		fmt.Printf("aggregator(%s) ticker stream connected\n", a.symbol)

		// Read messages until error or context cancellation
		for {
			_, data, err := ws.Read(ctx)
			if err != nil {
				lastErr = fmt.Errorf("ticker read: %w", err)
				if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
					return nil // Normal closure
				}
				fmt.Printf("aggregator(%s) ticker read error: %v\n", a.symbol, err)
				break // Break to retry
			}

			var msg bookTickerMessage
			if err := json.Unmarshal(data, &msg); err != nil {
				fmt.Printf("aggregator(%s) bookTicker unmarshal: %v\n", a.symbol, err)
				continue
			}
			if err := a.onTicker(msg); err != nil {
				fmt.Printf("aggregator(%s) bookTicker parse: %v\n", a.symbol, err)
			}
		}
	}

	// All attempts failed
	return fmt.Errorf("ticker loop failed after %d attempts, last error: %w", maxRetries, lastErr)
}

func (a *Aggregator) consumeStream(ctx context.Context, ws *websocket.Conn, out chan<- DepthDiff) error {
	defer close(out)
	fmt.Printf("aggregator(%s) depth stream connected\n", a.symbol)

	for {
		msgType, data, err := ws.Read(ctx)
		if err != nil {
			if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
				return nil // Normal closure
			}
			fmt.Printf("aggregator(%s) websocket read error: %v\n", a.symbol, err)
			return fmt.Errorf("read: %w", err)
		}

		if msgType != websocket.MessageText {
			fmt.Printf("aggregator(%s) received non-text message type: %v\n", a.symbol, msgType)
			continue
		}

		var diff DepthDiff
		if err := json.Unmarshal(data, &diff); err != nil {
			fmt.Printf("aggregator(%s) unmarshal diff: %v\n", a.symbol, err)
			continue
		}

		out <- diff
	}
}

func (a *Aggregator) enqueue(diff DepthDiff) {
	a.queueMu.Lock()
	defer a.queueMu.Unlock()
	now := time.Now()
	a.queue = append(a.queue, queuedDiff{diff: diff, arrived: now})
	cutoff := now.Add(-queueWindow)
	idx := 0
	for idx < len(a.queue) && a.queue[idx].arrived.Before(cutoff) {
		idx++
	}
	if idx > 0 {
		a.queue = a.queue[idx:]
	}
}

func (a *Aggregator) tryBridge() bool {
	if !a.hasSnapshot() {
		return false
	}
	a.queueMu.Lock()
	defer a.queueMu.Unlock()
	if len(a.queue) == 0 {
		return false
	}

	target := a.getLastID() + 1
	cut := 0
	for cut < len(a.queue) && a.queue[cut].diff.FinalID < target {
		cut++
	}
	if cut > 0 {
		a.queue = a.queue[cut:]
	}
	if len(a.queue) == 0 {
		return false
	}

	first := a.queue[0]
	if first.diff.FirstID > target {
		return false
	}
	if !(first.diff.FirstID <= target && target <= first.diff.FinalID) {
		return false
	}
	// apply first diff in bridge mode (no strict pu check)
	if err := a.applyDiff(first.diff, applyOptions{strictPU: false, bridgeMode: true}); err != nil {
		return false
	}
	a.queue = a.queue[1:]
	for len(a.queue) > 0 {
		if err := a.applyDiff(a.queue[0].diff, applyOptions{strictPU: false, bridgeMode: true}); err != nil {
			break
		}
		a.queue = a.queue[1:]
	}
	return true
}

func (a *Aggregator) drainReadyQueue() error {
	for {
		a.queueMu.Lock()
		if len(a.queue) == 0 {
			a.queueMu.Unlock()
			return nil
		}
		next := a.queue[0]
		target := a.getLastID() + 1
		if next.diff.FinalID < target {
			a.queue = a.queue[1:]
			a.queueMu.Unlock()
			continue
		}
		if next.diff.FirstID > target {
			// If Binance provides pu and it matches our lastID, accept continuity
			if next.diff.PrevFinal != nil && *next.diff.PrevFinal == a.getLastID() {
				// keep holding the lock; we'll unlock once below before apply
			} else {
				age := time.Since(next.arrived)
				fmt.Printf("aggregator(%s) waiting for gap: U=%d target=%d u=%d age=%s\n", a.symbol, next.diff.FirstID, target, next.diff.FinalID, age)
				if age < gapGrace {
					a.queueMu.Unlock()
					time.Sleep(50 * time.Millisecond)
					continue
				}
				a.queueMu.Unlock()
				a.logGap("queue-gap", next.diff)
				return fmt.Errorf("diff window gap U=%d target=%d u=%d age=%s", next.diff.FirstID, target, next.diff.FinalID, age)
			}
		}
		a.queueMu.Unlock()

		if err := a.applyDiff(next.diff, applyOptions{strictPU: true, bridgeMode: false}); err != nil {
			return err
		}
		a.queueMu.Lock()
		if len(a.queue) > 0 {
			a.queue = a.queue[1:]
		}
		a.queueMu.Unlock()
	}
}

type applyOptions struct {
	strictPU   bool
	bridgeMode bool
}

func (a *Aggregator) applyDiff(diff DepthDiff, opts applyOptions) error {
	a.mu.Lock()

	if a.lastID == 0 {
		a.mu.Unlock()
		return nil
	}
	target := a.lastID + 1

	// If pu is present (futures stream), require exact continuity and accept regardless of U window
	if opts.strictPU && diff.PrevFinal != nil {
		if *diff.PrevFinal != a.lastID {
			a.mu.Unlock()
			a.logGap("pu-mismatch", diff)
			return fmt.Errorf("pu mismatch pu=%d last=%d", *diff.PrevFinal, a.lastID)
		}
		// ok to continue; skip U-window checks
	} else {
		if diff.FinalID < target {
			a.mu.Unlock()
			return nil
		}
		if diff.FirstID > target {
			a.mu.Unlock()
			a.logGap("apply-gap", diff)
			return fmt.Errorf("diff window gap U=%d target=%d u=%d", diff.FirstID, target, diff.FinalID)
		}
	}

	changes, err := mergeLevels(diff.Bids, a.bids, "bid")
	if err != nil {
		a.mu.Unlock()
		return err
	}
	askChanges, err := mergeLevels(diff.Asks, a.asks, "ask")
	if err != nil {
		a.mu.Unlock()
		return err
	}
	changes = append(changes, askChanges...)

	a.lastID = diff.FinalID
	ts := diff.EventTime
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}

	mid := a.computeMidLocked()
	a.bookMid = mid
	a.lastFrame = time.UnixMilli(ts)

	a.mu.Unlock()

	// Track turnover after applying changes using computed mid
	a.trackTurnover(diff, mid)

	a.queueBookUpdate(ts, a.currentMid(mid), changes)
	// Use currentMid (prefers bookTicker mid) to center imbalance window
	a.queueIndicatorUpdate(ts, a.currentMid(mid))
	a.logDiff("apply", diff)
	return nil
}

func mergeLevels(raw [][]string, book map[float64]float64, side string) ([]BookLevel, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	changes := make([]BookLevel, 0, len(raw))
	for _, lvl := range raw {
		if len(lvl) < 2 {
			continue
		}
		price, err := strconv.ParseFloat(lvl[0], 64)
		if err != nil {
			return nil, err
		}
		qty, err := strconv.ParseFloat(lvl[1], 64)
		if err != nil {
			return nil, err
		}
		if qty == 0 {
			delete(book, price)
		} else {
			book[price] = qty
		}
		changes = append(changes, BookLevel{Side: side, Price: price, Qty: qty})
	}
	return changes, nil
}

func (a *Aggregator) trackTurnover(diff DepthDiff, mid float64) {
	// Calculate minute key for the current time (use event time from Binance)
	eventTime := time.UnixMilli(diff.EventTime)
	minuteKey := eventTime.Truncate(time.Minute).Unix()

	a.turnoverMu.Lock()

	// Initialize minute tracker if not exists
	if _, exists := a.minuteTracker[minuteKey]; !exists {
		a.minuteTracker[minuteKey] = &minuteTurnover{}
	}

	minute := a.minuteTracker[minuteKey]

	// Track bid changes - distinguish between placements and removals
	for _, bid := range diff.Bids {
		if len(bid) >= 2 {
			price, _ := strconv.ParseFloat(bid[0], 64)
			qty, _ := strconv.ParseFloat(bid[1], 64)

			// Check if this is within the ±1000 range (or custom window) for the first orderbook view
			window := 1000.0
			if a.window > 0 {
				window = a.window
			}

			if price >= mid-window && price <= mid+window {
				if qty > 0 {
					// Placement (new or increased order)
					minute.bidPlacements++
				} else {
					// Removal (order canceled or decreased to zero)
					minute.bidRemovals++
				}
			}
		}
	}

	// Track ask changes - distinguish between placements and removals
	for _, ask := range diff.Asks {
		if len(ask) >= 2 {
			price, _ := strconv.ParseFloat(ask[0], 64)
			qty, _ := strconv.ParseFloat(ask[1], 64)

			// Check if this is within the ±1000 range (or custom window) for the first orderbook view
			window := 1000.0
			if a.window > 0 {
				window = a.window
			}

			if price >= mid-window && price <= mid+window {
				if qty > 0 {
					// Placement (new or increased order)
					minute.askPlacements++
				} else {
					// Removal (order canceled or decreased to zero)
					minute.askRemovals++
				}
			}
		}
	}

	// Count total number of changes (orders)
	minute.ordersCount += len(diff.Bids) + len(diff.Asks)

	// Clean up old minute trackers (older than 1 hour), based on latest event time
	// minuteKey is Unix seconds; subtract 3600s to keep only the last hour
	cutoff := minuteKey - int64(time.Hour.Seconds())
	for key := range a.minuteTracker {
		if key < cutoff {
			delete(a.minuteTracker, key)
		}
	}

	a.turnoverMu.Unlock()
}

func (a *Aggregator) calculateImbalance(mid float64) float64 {
	if mid == 0 {
		return 0
	}

	a.mu.RLock()
	defer a.mu.RUnlock()

	// Calculate bid/ask volume within configured window
	window := 1000.0
	if a.window > 0 {
		window = a.window
	}

	low := mid - window
	high := mid + window

	bidVolume := 0.0
	askVolume := 0.0

	for price, qty := range a.bids {
		if price >= low && price <= high {
			bidVolume += qty
		}
	}

	for price, qty := range a.asks {
		if price >= low && price <= high {
			askVolume += qty
		}
	}

	if askVolume == 0 {
		return 1.0
	}

	return bidVolume / askVolume
}

// calculateImbalanceWithWindow computes imbalance using the reconstructed orderbook
// within [mid - window, mid + window]. It does not rely on bookTicker data for volumes,
// only for centering via mid.
func (a *Aggregator) calculateImbalanceWithWindow(mid float64, window float64) float64 {
	if mid == 0 {
		return 0
	}
	if window <= 0 {
		window = 1000
	}
	a.mu.RLock()
	defer a.mu.RUnlock()

	low := mid - window
	high := mid + window

	bidVolume := 0.0
	askVolume := 0.0

	for price, qty := range a.bids {
		if price >= low && price <= high {
			bidVolume += qty
		}
	}

	for price, qty := range a.asks {
		if price >= low && price <= high {
			askVolume += qty
		}
	}

	if askVolume == 0 {
		return 1.0
	}
	return bidVolume / askVolume
}

// calculateCumulativeBins builds cumulative volume bins of fixed size (contracts per bin)
// centered around the provided mid. It walks outward on both sides aggregating
// buy and sell volumes into bins and returns the absolute buy-sell difference per bin.
// For now, we produce 20 bins (10 on each side) and a bin size of `contractsPerBin`.
func (a *Aggregator) calculateCumulativeBins(mid float64, contractsPerBin float64) []float64 {
    if mid == 0 || contractsPerBin <= 0 {
        return nil
    }

    a.mu.RLock()
    defer a.mu.RUnlock()

    // Collect levels within the window first
    window := a.window
    if window <= 0 {
        window = 1000
    }
    low := mid - window
    high := mid + window

    // Extract and sort bids (desc) and asks (asc)
    bidLevels := make([]BookLevel, 0)
    for price, qty := range a.bids {
        if price >= low && price <= high && qty > 0 {
            bidLevels = append(bidLevels, BookLevel{Side: "bid", Price: price, Qty: qty})
        }
    }
    sort.Slice(bidLevels, func(i, j int) bool { return bidLevels[i].Price > bidLevels[j].Price })

    askLevels := make([]BookLevel, 0)
    for price, qty := range a.asks {
        if price >= low && price <= high && qty > 0 {
            askLevels = append(askLevels, BookLevel{Side: "ask", Price: price, Qty: qty})
        }
    }
    sort.Slice(askLevels, func(i, j int) bool { return askLevels[i].Price < askLevels[j].Price })

    // Walk outward from best bid and best ask accumulating into fixed-size bins
    // We'll build 10 bins per side by default.
    const binsPerSide = 10
    bins := make([]float64, 0, binsPerSide*2)

    // Helper to fill bins from a side
    fillBins := func(levels []BookLevel) []float64 {
        out := make([]float64, 0, binsPerSide)
        remaining := 0.0
        idx := 0
        for b := 0; b < binsPerSide; b++ {
            target := contractsPerBin
            acc := 0.0
            if remaining > 0 {
                use := remaining
                if use > target {
                    use = target
                }
                acc += use
                remaining -= use
            }
            for acc < target && idx < len(levels) {
                take := levels[idx].Qty
                need := target - acc
                if take > need {
                    acc += need
                    remaining = take - need
                    // reduce current level for future bins
                    levels[idx].Qty = remaining
                } else {
                    acc += take
                    remaining = 0
                    idx++
                }
            }
            out = append(out, acc)
        }
        return out
    }

    bidBins := fillBins(bidLevels)
    askBins := fillBins(askLevels)

    // Compute absolute difference per bin (buy vs sell)
    for i := 0; i < binsPerSide; i++ {
        diff := bidBins[i]
        if i < len(askBins) {
            diff = math.Abs(bidBins[i] - askBins[i])
        }
        bins = append(bins, diff)
    }
    // Then the far-side bins (remaining of the longer side)
    for i := 0; i < binsPerSide; i++ {
        // For symmetry, we again pair the i-th ask vs i-th bid; already covered above
        // Fill with abs difference; if one side shorter, treat missing as 0
        var bv, av float64
        if i < len(bidBins) {
            bv = bidBins[i]
        }
        if i < len(askBins) {
            av = askBins[i]
        }
        bins = append(bins, math.Abs(bv-av))
    }

    return bins
}

func (a *Aggregator) calculateTurnoverRate() TurnoverRate {
	// Expose ONLY the last fully completed minute, anchored by the
	// latest Binance diff event time bucket present in minuteTracker.
	a.turnoverMu.RLock()
	defer a.turnoverMu.RUnlock()

	var latest int64
	for k := range a.minuteTracker {
		if k > latest {
			latest = k
		}
	}
	if latest == 0 {
		return TurnoverRate{}
	}
	// Keys are per-minute Unix seconds truncated to the minute; subtract 60s to get previous bucket
	completed := latest - 60
	prev := a.minuteTracker[completed]
	if prev == nil {
		return TurnoverRate{}
	}
	return TurnoverRate{
		BidPlacements: prev.bidPlacements,
		AskPlacements: prev.askPlacements,
		TotalChanges:  prev.ordersCount,
		OrdersPerMin:  float64(prev.ordersCount),
	}
}

// calculateTurnoverImbalance computes bid/ask change ratio over the last windowSec seconds
// using the minuteTracker (placements + removals per side). Falls back to 1.0 if ask==0.
func (a *Aggregator) calculateTurnoverImbalance(windowSec int) float64 {
    if windowSec <= 0 {
        windowSec = 60
    }
    a.turnoverMu.RLock()
    defer a.turnoverMu.RUnlock()

    // Find latest minute key we have (event-time truncated to minute)
    var latest int64
    for k := range a.minuteTracker {
        if k > latest {
            latest = k
        }
    }
    if latest == 0 {
        return 1.0
    }

    // Sum across buckets overlapping [latest - windowSec, latest)
    // minute keys are second-level epoch truncated to minute boundaries.
    start := latest - int64(windowSec)
    var bidSum, askSum int
    for mk, m := range a.minuteTracker {
        if mk >= start && mk <= latest {
            bidSum += m.bidPlacements + m.bidRemovals
            askSum += m.askPlacements + m.askRemovals
        }
    }
    if askSum == 0 {
        return 1.0
    }
    return float64(bidSum) / float64(askSum)
}

// calculateVPIN computes a simplified VPIN (Volume-Synchronized PIN) proxy using
// turnover placements/removals as volume proxies. It forms equally sized
// volume buckets (bucketVol) by walking back through completed minute bins and
// accumulates bid and ask activity until the bucket threshold is met. VPIN is
// the average absolute order-imbalance per bucket over a rolling window.
// bucketVol is in "order events" units here (since we do not have trade size).
// windowMin controls how many minutes to include (completed minutes only).
// Deprecated: turnover-proxy VPIN using order-change counts. Kept for reference but not used.
func (a *Aggregator) calculateVPIN(bucketVol int, windowMin int) float64 {
    if bucketVol <= 0 {
        bucketVol = 50
    }
    if windowMin <= 0 {
        windowMin = 60
    }

    a.turnoverMu.RLock()
    defer a.turnoverMu.RUnlock()

    // Find latest completed minute key
    var latest int64
    for k := range a.minuteTracker {
        if k > latest {
            latest = k
        }
    }
    if latest == 0 {
        return 0
    }
    latestCompleted := latest - 60

    // Walk back windowMin minutes collecting minute bins newest->oldest
    bins := make([]*minuteTurnover, 0, windowMin)
    for i := 0; i < windowMin; i++ {
        mk := latestCompleted - int64(60*i)
        if m := a.minuteTracker[mk]; m != nil {
            bins = append(bins, m)
        } else {
            // include empty minute (no activity)
            bins = append(bins, &minuteTurnover{})
        }
    }

    // Build volume-synchronized buckets
    type bucket struct{ b, a int }
    var cur bucket
    var buckets []bucket
    push := func() {
        if cur.b == 0 && cur.a == 0 {
            return
        }
        buckets = append(buckets, cur)
        cur = bucket{}
    }

    for _, m := range bins {
        // Approximate bid/ask volume for the minute as total changes per side
        b := m.bidPlacements + m.bidRemovals
        a := m.askPlacements + m.askRemovals
        sideTotal := b + a
        if sideTotal == 0 {
            continue
        }
        // Fill the current bucket with this minute's activity
        remaining := sideTotal
        // Simple split by side proportions
        for remaining > 0 {
            need := bucketVol - (cur.b + cur.a)
            if need <= 0 {
                push()
                continue
            }
            take := need
            if take > remaining {
                take = remaining
            }
            // Proportionally allocate to b/a for this chunk
            var tb, ta int
            if b+a > 0 {
                tb = int(math.Round(float64(take) * float64(b) / float64(b+a)))
                if tb > take {
                    tb = take
                }
                ta = take - tb
            } else {
                ta = take
            }
            cur.b += tb
            cur.a += ta
            remaining -= take
            // If filled, push bucket
            if cur.b+cur.a >= bucketVol {
                push()
            }
        }
    }
    // push tail if partially filled; classical VPIN often excludes last partial bucket,
    // but we include it to keep the metric responsive.
    push()

    if len(buckets) == 0 {
        return 0
    }
    // VPIN = average(|B - A|) / bucketVol
    var sum float64
    for _, bk := range buckets {
        sum += math.Abs(float64(bk.b-bk.a)) / float64(bucketVol)
    }
    return sum / float64(len(buckets))
}

func (a *Aggregator) getTurnoverHistory(historyLen int) []TurnoverHistory {
	if historyLen <= 0 {
		historyLen = 60 // Default to 60 minutes
	}

	a.turnoverMu.RLock()
	defer a.turnoverMu.RUnlock()

	// Anchor history to the latest event-time minute we have
	var latest int64
	for k := range a.minuteTracker {
		if k > latest {
			latest = k
		}
	}
	if latest == 0 {
		return make([]TurnoverHistory, 0)
	}

	// Only include completed minutes: latestCompleted = latest - 60s
	latestCompleted := latest - 60
	history := make([]TurnoverHistory, 0, historyLen)
	for i := historyLen - 1; i >= 0; i-- {
		// Step backwards in 60s increments to get per-minute buckets
		minuteKey := latestCompleted - int64(60*i)
		minuteTime := time.Unix(minuteKey, 0)
		minute := a.minuteTracker[minuteKey]
		if minute == nil {
			history = append(history, TurnoverHistory{
				Timestamp:   minuteTime.UnixMilli(),
				BidVolume:   0,
				AskVolume:   0,
				TotalVolume: 0,
				OrdersCount: 0,
				Imbalance:   1.0,
			})
			continue
		}
		totalVolume := float64(minute.bidPlacements + minute.bidRemovals + minute.askPlacements + minute.askRemovals)
		imbalance := 1.0
		if minute.askPlacements+minute.askRemovals > 0 {
			imbalance = float64(minute.bidPlacements+minute.bidRemovals) / float64(minute.askPlacements+minute.askRemovals)
		}
		history = append(history, TurnoverHistory{
			Timestamp:   minuteTime.UnixMilli(),
			BidVolume:   float64(minute.bidPlacements + minute.bidRemovals),
			AskVolume:   float64(minute.askPlacements + minute.askRemovals),
			TotalVolume: totalVolume,
			OrdersCount: minute.ordersCount,
			Imbalance:   imbalance,
		})
	}
	return history
}

func (a *Aggregator) queueIndicatorUpdate(ts int64, mid float64) {
    if ts == 0 {
        ts = time.Now().UnixMilli()
    }

    // Use turnover-based imbalance over the last 60 seconds
    imbalance := a.calculateTurnoverImbalance(60)
    turnoverRate := a.calculateTurnoverRate()
    turnoverHistory := a.getTurnoverHistory(60) // 1 hour of history
    cumBins := a.calculateCumulativeBins(mid, 100.0) // 100 contracts per bin

    indicator := IndicatorData{
        Timestamp:       ts,
        Imbalance:       imbalance,
        TurnoverRate:    turnoverRate,
        TurnoverHistory: turnoverHistory,
        CumBins:         cumBins,
    }

	select {
	case a.indicatorUpdates <- indicator:
	default:
	}
}

func (a *Aggregator) fetchSnapshot(ctx context.Context) error {
	const maxRetries = 3
	const initialBackoff = 1 * time.Second

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * initialBackoff):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.depthSnapshotURL(), nil)
		if err != nil {
			return err
		}

		// Add headers to avoid 418 error
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Accept-Language", "en-US,en;q=0.9")
		req.Header.Set("Cache-Control", "no-cache")
		req.Header.Set("Pragma", "no-cache")

		// Build query via helper for clarity; symbol uppercased and limit applied
		req.URL.RawQuery = ""
		req.URL, _ = url.Parse(BuildDepthSnapshotURLWithQuery(a.market, a.symbol, 1000))

		resp, err := a.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("HTTP request failed (attempt %d/%d): %w", attempt+1, maxRetries, err)
			fmt.Printf("aggregator(%s) snapshot request failed: %v\n", a.symbol, err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			lastErr = fmt.Errorf("snapshot status %s (attempt %d/%d): %s", resp.Status, attempt+1, maxRetries, string(body))
			fmt.Printf("aggregator(%s) snapshot HTTP error: %s\n", a.symbol, lastErr)
			continue
		}

		var snap depthSnapshot
		if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
			lastErr = fmt.Errorf("JSON decode failed (attempt %d/%d): %w", attempt+1, maxRetries, err)
			fmt.Printf("aggregator(%s) snapshot decode error: %v\n", a.symbol, err)
			continue
		}

		// Success - reset lastErr and proceed with processing
		lastErr = nil
		bids := make(map[float64]float64, len(snap.Bids))
		asks := make(map[float64]float64, len(snap.Asks))
		for _, lvl := range snap.Bids {
			if len(lvl) < 2 {
				continue
			}
			price, err := strconv.ParseFloat(lvl[0], 64)
			if err != nil {
				return fmt.Errorf("bid price parse: %w", err)
			}
			qty, err := strconv.ParseFloat(lvl[1], 64)
			if err != nil {
				return fmt.Errorf("bid qty parse: %w", err)
			}
			if qty > 0 {
				bids[price] = qty
			}
		}
		for _, lvl := range snap.Asks {
			if len(lvl) < 2 {
				continue
			}
			price, err := strconv.ParseFloat(lvl[0], 64)
			if err != nil {
				return fmt.Errorf("ask price parse: %w", err)
			}
			qty, err := strconv.ParseFloat(lvl[1], 64)
			if err != nil {
				return fmt.Errorf("ask qty parse: %w", err)
			}
			if qty > 0 {
				asks[price] = qty
			}
		}

		a.mu.Lock()
		a.bids = bids
		a.asks = asks
		a.lastID = snap.LastUpdateID
		mid := a.computeMidLocked()
		a.bookMid = mid
		a.lastSnap = time.Now()
		a.mu.Unlock()

		a.logSnapshot(snap)
		return nil
	}

	// All attempts failed
	return fmt.Errorf("snapshot fetch failed after %d attempts, last error: %w", maxRetries, lastErr)
}

func (a *Aggregator) computeMidLocked() float64 {
	bestBid := 0.0
	for price := range a.bids {
		if price > bestBid {
			bestBid = price
		}
	}
	bestAsk := math.MaxFloat64
	for price := range a.asks {
		if price < bestAsk {
			bestAsk = price
		}
	}
	if bestBid == 0 || bestAsk == math.MaxFloat64 {
		return 0
	}
	return (bestBid + bestAsk) / 2
}

func (a *Aggregator) queueBookUpdate(ts int64, mid float64, changes []BookLevel) {
	if len(changes) == 0 {
		return
	}
	select {
	case a.bookUpdates <- bookUpdate{ts: ts, mid: mid, changes: changes}:
	default:
	}
}

func (a *Aggregator) broadcastBook(ts int64, mid float64, changes []BookLevel) {
	if len(changes) == 0 {
		return
	}
	a.subMu.Lock()
	for id, sub := range a.bookSubs {
		patch := make([]BookLevel, 0, len(changes))
		for _, lvl := range changes {
			if sub.window > 0 {
				low := mid - sub.window
				high := mid + sub.window
				if lvl.Price < low || lvl.Price > high {
					if lvl.Qty == 0 {
						patch = append(patch, lvl)
					} else {
						patch = append(patch, BookLevel{Side: lvl.Side, Price: lvl.Price, Qty: 0})
					}
					continue
				}
			}
			patch = append(patch, lvl)
		}
		if len(patch) == 0 {
			continue
		}
		select {
		case sub.ch <- BookPatch{Timestamp: ts, Mid: mid, Patch: patch}:
		default:
			close(sub.ch)
			delete(a.bookSubs, id)
		}
	}
	a.subMu.Unlock()
}

func (a *Aggregator) broadcastPrice(point PricePoint) {
	a.subMu.Lock()
	for id, sub := range a.priceSubs {
		select {
		case sub.ch <- point:
		default:
			close(sub.ch)
			delete(a.priceSubs, id)
		}
	}
	a.subMu.Unlock()
}

func (a *Aggregator) broadcastIndicator(indicator IndicatorData) {
	a.subMu.Lock()
	// Read current BBO mid to center imbalance calculation
	a.mu.RLock()
	mid := a.tickerMid
	// Fallback to bookMid if ticker is unavailable
	if mid == 0 {
		mid = a.bookMid
		if mid == 0 {
			mid = a.computeMidLocked()
		}
	}
	a.mu.RUnlock()

	for id, sub := range a.indicatorSubs {
		// Customize imbalance per-subscriber using their requested window around BBO mid
		custom := indicator
        if mid != 0 {
            // Keep instantaneous imbalance per-subscriber if needed later; now use turnover-based 60s
            custom.Imbalance = a.calculateTurnoverImbalance(60)
            // Use trade-based VPIN exclusively; do not fall back to turnover proxy
            custom.VPIN = a.calculateTradeVPIN()
            // Debug: include bucket stats
            a.tradeMu.Lock()
            custom.VPINBucketCount = len(a.tradeBuckets)
            if len(a.tradeBuckets) > 0 {
                custom.VPINLastBucket = a.tradeBuckets[len(a.tradeBuckets)-1].endMS
            }
            a.tradeMu.Unlock()
            // Recompute bins per subscriber centered at current mid
            custom.CumBins = a.calculateCumulativeBins(mid, 100.0)
        }
		select {
		case sub.ch <- custom:
		default:
			close(sub.ch)
			delete(a.indicatorSubs, id)
		}
	}
	a.subMu.Unlock()
}

func (a *Aggregator) bookFanoutLoop() {
	ticker := time.NewTicker(fanoutTickDur)
	defer ticker.Stop()

	pending := make(map[bookKey]BookLevel)
	var mid float64
	var ts int64

	for {
		select {
		case <-a.ctx.Done():
			return
		case upd := <-a.bookUpdates:
			if len(upd.changes) == 0 {
				continue
			}
			for _, lvl := range upd.changes {
				pending[bookKey{side: lvl.Side, price: lvl.Price}] = lvl
			}
			if upd.mid != 0 {
				mid = upd.mid
			}
			if upd.ts != 0 {
				ts = upd.ts
			}
		case <-ticker.C:
			if len(pending) == 0 {
				continue
			}
			patch := make([]BookLevel, 0, len(pending))
			for _, lvl := range pending {
				patch = append(patch, lvl)
			}
			pending = make(map[bookKey]BookLevel)
			useMid := a.currentMid(mid)
			useTS := ts
			if useTS == 0 {
				useTS = time.Now().UnixMilli()
			}
			a.broadcastBook(useTS, useMid, patch)
			mid = 0
			ts = 0
		}
	}
}

func (a *Aggregator) priceFanoutLoop() {
	ticker := time.NewTicker(fanoutTickDur)
	defer ticker.Stop()

	var last PricePoint
	var pending bool

	for {
		select {
		case <-a.ctx.Done():
			return
		case pt := <-a.pricePoints:
			last = pt
			pending = true
		case <-ticker.C:
			if !pending {
				continue
			}
			a.broadcastPrice(last)
			pending = false
		}
	}
}

func (a *Aggregator) indicatorFanoutLoop() {
	ticker := time.NewTicker(fanoutTickDur)
	defer ticker.Stop()

	var last IndicatorData
	var pending bool

	for {
		select {
		case <-a.ctx.Done():
			return
		case indicator := <-a.indicatorUpdates:
			last = indicator
			pending = true
		case <-ticker.C:
			if !pending {
				continue
			}
			a.broadcastIndicator(last)
			pending = false
		}
	}
}

func (a *Aggregator) recordPrice(ts int64, mid float64) {
	if mid == 0 {
		return
	}
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	point := PricePoint{Time: ts, Mid: mid}
	a.priceMu.Lock()
	a.priceRing = append(a.priceRing, point)
	cutoff := ts - priceHorizon.Milliseconds()
	idx := 0
	for idx < len(a.priceRing) && a.priceRing[idx].Time < cutoff {
		idx++
	}
	if idx > 0 {
		a.priceRing = a.priceRing[idx:]
	}
	a.priceMu.Unlock()

	select {
	case a.pricePoints <- point:
	default:
	}
}

func (a *Aggregator) onTicker(msg bookTickerMessage) error {
	bid, err := strconv.ParseFloat(msg.BidPrice, 64)
	if err != nil {
		return err
	}
	ask, err := strconv.ParseFloat(msg.AskPrice, 64)
	if err != nil {
		return err
	}
	if bid == 0 && ask == 0 {
		return nil
	}
	mid := bid
	if ask != 0 {
		if bid != 0 {
			mid = (bid + ask) / 2
		} else {
			mid = ask
		}
	}
	ts := parseAnyInt64(msg.EventTime)
	if ts == 0 {
		ts = parseAnyInt64(msg.TradeTime)
	}
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	a.updateTicker(mid, ts)
	return nil
}

func (a *Aggregator) updateTicker(mid float64, ts int64) {
	if mid == 0 {
		return
	}
	a.mu.Lock()
	a.tickerMid = mid
	a.tickerTS = time.UnixMilli(ts)
	a.mu.Unlock()

	a.recordPrice(ts, mid)
}

func parseAnyInt64(v any) int64 {
	switch val := v.(type) {
	case float64:
		return int64(val)
	case int64:
		return val
	case json.Number:
		parsed, _ := val.Int64()
		return parsed
	case string:
		parsed, _ := strconv.ParseInt(val, 10, 64)
		return parsed
	default:
		return 0
	}
}

func (a *Aggregator) currentMid(fallback float64) float64 {
	a.mu.RLock()
	mid := a.tickerMid
	if mid == 0 {
		mid = fallback
		if mid == 0 {
			mid = a.bookMid
		}
	}
	a.mu.RUnlock()
	return mid
}

func (a *Aggregator) Snapshot(window float64) Snapshot {
	if window <= 0 {
		window = a.window
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	mid := a.tickerMid
	if mid == 0 {
		mid = a.bookMid
		if mid == 0 {
			mid = a.computeMidLocked()
		}
	}
	levels := a.collectWindowLocked(mid, window)

	return Snapshot{
		Timestamp: time.Now().UnixMilli(),
		Mid:       mid,
		Levels:    levels,
	}
}

// createMockSnapshot creates mock orderbook data for testing
func (a *Aggregator) createMockSnapshot(window float64) Snapshot {
	mockMid := 45000.0 // Mock BTC price
	levels := make([]BookLevel, 0)

	// Generate mock bid levels (below mid price)
	for i := 0; i < 10; i++ {
		price := mockMid - float64(i+1)*10
		qty := float64(10+i) + (float64(i%3) * 5)
		levels = append(levels, BookLevel{
			Side:  "bid",
			Price: price,
			Qty:   qty,
		})
	}

	// Generate mock ask levels (above mid price)
	for i := 0; i < 10; i++ {
		price := mockMid + float64(i+1)*10
		qty := float64(8+i) + (float64(i%4) * 3)
		levels = append(levels, BookLevel{
			Side:  "ask",
			Price: price,
			Qty:   qty,
		})
	}

	return Snapshot{
		Timestamp: time.Now().UnixMilli(),
		Mid:       mockMid,
		Levels:    levels,
	}
}

func (a *Aggregator) collectWindowLocked(mid, window float64) []BookLevel {
	if window <= 0 {
		window = a.window
	}
	low := mid - window
	high := mid + window
	bids := make([]BookLevel, 0)
	asks := make([]BookLevel, 0)
	for price, qty := range a.bids {
		if window == 0 || (price >= low && price <= high) {
			bids = append(bids, BookLevel{Side: "bid", Price: price, Qty: qty})
		}
	}
	for price, qty := range a.asks {
		if window == 0 || (price >= low && price <= high) {
			asks = append(asks, BookLevel{Side: "ask", Price: price, Qty: qty})
		}
	}
	sort.Slice(bids, func(i, j int) bool { return bids[i].Price > bids[j].Price })
	sort.Slice(asks, func(i, j int) bool { return asks[i].Price < asks[j].Price })
	return append(bids, asks...)
}

func (a *Aggregator) History() []PricePoint {
	a.priceMu.RLock()
	defer a.priceMu.RUnlock()
	out := make([]PricePoint, len(a.priceRing))
	copy(out, a.priceRing)
	return out
}

func (a *Aggregator) closeLogs() {
	a.logMu.Lock()
	defer a.logMu.Unlock()
	if a.diffLog != nil {
		a.diffLog.Close()
		a.diffLog = nil
	}
	if a.snapLog != nil {
		a.snapLog.Close()
		a.snapLog = nil
	}
	if a.gapLog != nil {
		a.gapLog.Close()
		a.gapLog = nil
	}
}

func (a *Aggregator) appendLog(file **os.File, filename string, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		fmt.Printf("aggregator(%s) log marshal: %v\n", a.symbol, err)
		return
	}
	a.logMu.Lock()
	defer a.logMu.Unlock()
	if *file == nil {
		dir := filepath.Join("logs", strings.ToUpper(a.symbol))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			fmt.Printf("aggregator(%s) log mkdir: %v\n", a.symbol, err)
			return
		}
		path := filepath.Join(dir, filename)
		f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			fmt.Printf("aggregator(%s) log open %s: %v\n", a.symbol, filename, err)
			return
		}
		*file = f
	}
	if _, err := (*file).Write(append(data, '\n')); err != nil {
		fmt.Printf("aggregator(%s) log write %s: %v\n", a.symbol, (*file).Name(), err)
	}
}

func (a *Aggregator) logDiff(stage string, diff DepthDiff) {
	a.appendLog(&a.diffLog, "diff.log", map[string]any{
		"time":  time.Now().UTC().Format(time.RFC3339Nano),
		"stage": stage,
		"U":     diff.FirstID,
		"u":     diff.FinalID,
		"pu":    diff.PrevFinal,
		"bids":  diff.Bids,
		"asks":  diff.Asks,
	})
}

func (a *Aggregator) logSnapshot(snap depthSnapshot) {
	a.appendLog(&a.snapLog, "snapshot.log", map[string]any{
		"time":         time.Now().UTC().Format(time.RFC3339Nano),
		"lastUpdateId": snap.LastUpdateID,
		"bids":         snap.Bids,
		"asks":         snap.Asks,
	})
}

func (a *Aggregator) logGap(stage string, diff DepthDiff) {
	a.appendLog(&a.gapLog, "gap.log", map[string]any{
		"time":  time.Now().UTC().Format(time.RFC3339Nano),
		"stage": stage,
		"U":     diff.FirstID,
		"u":     diff.FinalID,
		"pu":    diff.PrevFinal,
		"bids":  diff.Bids,
		"asks":  diff.Asks,
	})
}

func (a *Aggregator) RegisterBook(window float64) (int, <-chan BookPatch) {
	if window <= 0 {
		window = a.window
	}
	ch := make(chan BookPatch, 64)
	a.subMu.Lock()
	id := a.nextSubID
	a.nextSubID++
	a.bookSubs[id] = &bookSubscriber{ch: ch, window: window}
	a.subMu.Unlock()
	return id, ch
}

func (a *Aggregator) UnregisterBook(id int) {
	a.subMu.Lock()
	if sub, ok := a.bookSubs[id]; ok {
		close(sub.ch)
		delete(a.bookSubs, id)
	}
	a.subMu.Unlock()
}

func (a *Aggregator) RegisterPrice() (int, <-chan PricePoint) {
	ch := make(chan PricePoint, 64)
	a.subMu.Lock()
	id := a.nextSubID
	a.nextSubID++
	a.priceSubs[id] = &priceSubscriber{ch: ch}
	a.subMu.Unlock()
	return id, ch
}

func (a *Aggregator) UnregisterPrice(id int) {
	a.subMu.Lock()
	if sub, ok := a.priceSubs[id]; ok {
		close(sub.ch)
		delete(a.priceSubs, id)
	}
	a.subMu.Unlock()
}

func (a *Aggregator) RegisterIndicator(window float64, historyLen int) (int, <-chan IndicatorData) {
	if window <= 0 {
		window = a.window
	}
	if historyLen <= 0 {
		historyLen = 60 // Default to 60 minutes
	}
	ch := make(chan IndicatorData, 64)
	a.subMu.Lock()
	id := a.nextSubID
	a.nextSubID++
	a.indicatorSubs[id] = &indicatorSubscriber{ch: ch, window: window, historyLen: historyLen}
	a.subMu.Unlock()
	return id, ch
}

func (a *Aggregator) UnregisterIndicator(id int) {
	a.subMu.Lock()
	if sub, ok := a.indicatorSubs[id]; ok {
		close(sub.ch)
		delete(a.indicatorSubs, id)
	}
	a.subMu.Unlock()
}

// ResetIndicators clears turnover history and minute tracker; called when OB#1 band changes
func (a *Aggregator) ResetIndicators() {
	a.turnoverMu.Lock()
	a.turnoverRing = a.turnoverRing[:0]
	a.minuteTracker = make(map[int64]*minuteTurnover)
	a.turnoverMu.Unlock()
}

func (a *Aggregator) hasSnapshot() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.lastID != 0
}

func (a *Aggregator) getLastID() int64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.lastID
}

func (a *Aggregator) currentState() aggregatorState {
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	return a.state
}

func (a *Aggregator) setState(st aggregatorState) {
	a.stateMu.Lock()
	a.state = st
	a.stateMu.Unlock()
}

func (a *Aggregator) depthStreamURL() string      { return DepthStreamURLFor(a.market, a.symbol) }
func (a *Aggregator) depthSnapshotURL() string    { return DepthSnapshotURLFor(a.market) }
func (a *Aggregator) bookTickerStreamURL() string { return BookTickerStreamURLFor(a.market, a.symbol) }

func (a *Aggregator) Health() map[string]any {
	a.mu.RLock()
	bookState := map[string]any{
		"lastUpdateId": a.lastID,
		"bookMid":      a.bookMid,
		"tickerMid":    a.tickerMid,
		"tickerTime":   a.tickerTS,
		"lastFrame":    a.lastFrame,
		"lastSnapshot": a.lastSnap,
	}
	a.mu.RUnlock()

	a.queueMu.Lock()
	qLen := len(a.queue)
	a.queueMu.Unlock()

	return map[string]any{
		"state":      a.currentState().String(),
		"symbol":     a.symbol,
		"market":     a.market,
		"queueDepth": qLen,
		"book":       bookState,
	}
}

func addJitter(d time.Duration) time.Duration {
	jitter := time.Duration((randFloat64() - 0.5) * float64(200*time.Millisecond))
	return d + jitter
}

var rndMu sync.Mutex
var rnd = time.Now().UnixNano()

func randFloat64() float64 {
	rndMu.Lock()
	rnd = rnd*1664525 + 1013904223
	v := rnd
	rndMu.Unlock()
	return float64(v%1_000_000) / 1_000_000
}
