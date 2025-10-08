# Orderbook Turnover Rates

A trimmed-down version of the visual order book project focused solely on streaming and visualising Binance.US orderbook turnover statistics by default. The Go backend streams turnover metrics over Server-Sent Events, while the bundled dashboard renders the live rate alongside a one-hour history chart.

## Project layout

- `cmd/app` – Go entrypoint wrapping the HTTP server
- `internal/orderbook` – aggregator logic copied from the original project (unchanged core behaviour)
- `internal/server` – thin server exposing `/stream/turnover`, `/api/defaults`, static assets, and `/healthz`
- `web` – Vite/TypeScript source for the dashboard (optional for further tweaks)
- `internal/server/dist` – pre-built static assets embedded by the Go server

## Running the app

1. Ensure Go 1.22+ is installed.
2. From this directory run:

   ```bash
   APP_PORT=33277 go run ./cmd/app
   ```

   The server logs the loopback URL once ready. Open the printed address in a browser to view turnover rates.

The embedded assets are served immediately; no Node build step is required just to run the packaged dashboard.

## Developing the frontend

The `web` folder contains the TypeScript source. To iterate with hot reload you can:

1. Install Node 18+.
2. From `web/`, install dependencies and start Vite:

   ```bash
   npm install
   npm run dev
   ```

3. In another terminal, run the Go server with a fixed port so the Vite proxy can forward requests:

   ```bash
   APP_PORT=33277 go run ./cmd/app
   ```

4. Edit files under `web/src`. When you are ready to refresh the embedded assets, run `npm run build` to regenerate `internal/server/dist`.

## Configuration

Environment variables can adjust the data source:

- `DEFAULT_SYMBOL` – trading pair symbol (default `BTCUSDT`)
- `DEFAULT_MARKET` – defaults to `spot` (Binance.US only exposes spot); set to `futures` if you also switch the venue
- `BINANCE_VENUE` – defaults to `us`; set to `global` for the worldwide endpoints
- Other `BINANCE_*` overrides – inherit the endpoint controls from the original project when required

The turnover stream also accepts optional `symbol`, `market`, and `window` query parameters when invoking `/stream/turnover` directly.
