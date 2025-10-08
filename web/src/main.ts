type TurnoverRate = {
  bidPlacements: number;
  askPlacements: number;
  totalChanges: number;
  ordersPerMin: number;
};

type TurnoverHistoryPoint = {
  t: number;
  bidVolume: number;
  askVolume: number;
  totalVolume: number;
  ordersCount: number;
  imbalance: number;
};

type TurnoverPayload = {
  ts: number;
  turnoverRate: TurnoverRate;
  turnoverHistory: TurnoverHistoryPoint[];
};

type DefaultsResponse = {
  symbol: string;
  market: string;
  venue: string;
};

const statusEl = document.getElementById("status");
const runtimeEl = document.getElementById("runtime-config");
const hintEl = document.getElementById("empty-hint");
const chartCanvas = document.getElementById("turnover-chart") as HTMLCanvasElement | null;

const bidEl = document.getElementById("bid-placements");
const askEl = document.getElementById("ask-placements");
const totalEl = document.getElementById("total-changes");
const ordersPerMinuteEl = document.getElementById("orders-per-minute");

let currentHistory: TurnoverHistoryPoint[] = [];
let currentDefaults: DefaultsResponse | null = null;
let source: EventSource | null = null;

function setStatus(text: string, state: "ok" | "connecting" | "info" | "error") {
  if (!statusEl) return;
  statusEl.textContent = text;
  statusEl.setAttribute("data-state", state);
}

function setRuntimeConfigText(defaults: DefaultsResponse) {
  if (!runtimeEl) return;
  const venueLabel = defaults.venue === "us" ? "Binance.US" : "Binance";
  runtimeEl.textContent = `${defaults.symbol} · ${defaults.market.toUpperCase()} · ${venueLabel}`;
}

function formatNumber(value: number, fractionDigits = 0) {
  return value.toLocaleString(undefined, {
    maximumFractionDigits: fractionDigits,
    minimumFractionDigits: fractionDigits,
  });
}

function updateMetricCards(rate: TurnoverRate) {
  bidEl && (bidEl.textContent = formatNumber(rate.bidPlacements));
  askEl && (askEl.textContent = formatNumber(rate.askPlacements));
  totalEl && (totalEl.textContent = formatNumber(rate.totalChanges));
  ordersPerMinuteEl && (ordersPerMinuteEl.textContent = formatNumber(rate.ordersPerMin, 1));
}

function configureCanvasSize() {
  if (!chartCanvas) return;
  const dpr = window.devicePixelRatio || 1;
  const rect = chartCanvas.getBoundingClientRect();
  const width = rect.width || 900;
  const height = rect.height || 360;
  chartCanvas.width = Math.round(width * dpr);
  chartCanvas.height = Math.round(height * dpr);
}

function drawChart() {
  if (!chartCanvas) return;
  const ctx = chartCanvas.getContext("2d");
  if (!ctx) return;

  ctx.setTransform(1, 0, 0, 1, 0, 0);
  ctx.clearRect(0, 0, chartCanvas.width, chartCanvas.height);

  const rect = chartCanvas.getBoundingClientRect();
  const width = rect.width || 900;
  const height = rect.height || 360;

  ctx.save();
  ctx.scale(chartCanvas.width / width, chartCanvas.height / height);

  ctx.fillStyle = "rgba(15, 23, 42, 0.6)";
  ctx.fillRect(0, 0, width, height);

  if (!currentHistory.length) {
    ctx.restore();
    if (hintEl) hintEl.style.display = "block";
    return;
  }
  if (hintEl) hintEl.style.display = "none";

  const padding = 32;
  const chartWidth = width - padding * 2;
  const chartHeight = height - padding * 2;

  const maxVolume = Math.max(...currentHistory.map((point) => point.totalVolume), 1);

  ctx.strokeStyle = "rgba(148, 163, 184, 0.22)";
  ctx.lineWidth = 1;
  ctx.setLineDash([4, 6]);
  const gridLines = 4;
  for (let i = 0; i <= gridLines; i += 1) {
    const y = padding + (chartHeight / gridLines) * i;
    ctx.beginPath();
    ctx.moveTo(padding, y);
    ctx.lineTo(padding + chartWidth, y);
    ctx.stroke();
  }
  ctx.setLineDash([]);

  function plotLine(color: string, accessor: (point: TurnoverHistoryPoint) => number) {
    ctx.beginPath();
    ctx.strokeStyle = color;
    ctx.lineWidth = 2.2;
    currentHistory.forEach((point, index) => {
      const x = padding + (chartWidth * index) / Math.max(currentHistory.length - 1, 1);
      const value = accessor(point);
      const y = padding + chartHeight - (chartHeight * value) / maxVolume;
      if (index === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.stroke();
  }

  plotLine("#22d3ee", (point) => point.bidVolume);
  plotLine("#fbbf24", (point) => point.askVolume);

  ctx.restore();
}

function connectToStream(defaults: DefaultsResponse) {
  if (source) {
    try { source.close(); } catch { /* noop */ }
  }
  const params = new URLSearchParams();
  params.set("symbol", defaults.symbol);
  params.set("market", defaults.market);

  source = new EventSource(`/stream/turnover?${params.toString()}`);
  setStatus("Connecting…", "connecting");

  source.onopen = () => setStatus("Live", "ok");
  source.onerror = () => setStatus("Reconnecting…", "error");

  source.addEventListener("turnover", (event) => {
    const data = JSON.parse((event as MessageEvent<string>).data) as TurnoverPayload;
    currentHistory = data.turnoverHistory.slice(-60);
    updateMetricCards(data.turnoverRate);
    drawChart();
  });
}

async function bootstrap() {
  setStatus("Loading…", "info");
  try {
    const res = await fetch("/api/defaults");
    if (!res.ok) throw new Error(`defaults status ${res.status}`);
    currentDefaults = (await res.json()) as DefaultsResponse;
    setRuntimeConfigText(currentDefaults);
    configureCanvasSize();
    connectToStream(currentDefaults);
  } catch (err) {
    console.error("failed to load defaults", err);
    setStatus("Defaults unavailable", "error");
  }
}

window.addEventListener("DOMContentLoaded", bootstrap);
window.addEventListener("resize", () => {
  configureCanvasSize();
  drawChart();
});
