const statusEl = document.getElementById("status");
const runtimeEl = document.getElementById("runtime-config");
const hintEl = document.getElementById("empty-hint");
const chartCanvas = document.getElementById("turnover-chart");

const bidEl = document.getElementById("bid-placements");
const askEl = document.getElementById("ask-placements");
const totalEl = document.getElementById("total-changes");
const ordersPerMinuteEl = document.getElementById("orders-per-minute");
const imbalanceBar = document.getElementById("imbalance-bar");
const imbalanceLabel = document.getElementById("imbalance-label");

let currentHistory = [];
let source = null;

function setStatus(text, state) {
  if (!statusEl) return;
  statusEl.textContent = text;
  statusEl.setAttribute("data-state", state);
}

function setRuntimeConfigText(defaults) {
  if (!runtimeEl) return;
  const venueLabel = defaults.venue === "us" ? "Binance.US" : "Binance";
  runtimeEl.textContent = `${defaults.symbol} · ${defaults.market.toUpperCase()} · ${venueLabel}`;
}

function formatNumber(value, fractionDigits = 0) {
  return value.toLocaleString(undefined, {
    maximumFractionDigits: fractionDigits,
    minimumFractionDigits: fractionDigits,
  });
}

function updateMetricCards(rate) {
  if (bidEl) bidEl.textContent = formatNumber(rate.bidPlacements);
  if (askEl) askEl.textContent = formatNumber(rate.askPlacements);
  if (totalEl) totalEl.textContent = formatNumber(rate.totalChanges);
  if (ordersPerMinuteEl) ordersPerMinuteEl.textContent = formatNumber(rate.ordersPerMin, 1);
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

  const plotLine = (color, accessor) => {
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
  };

  plotLine("#22d3ee", (point) => point.bidVolume);
  plotLine("#fbbf24", (point) => point.askVolume);

  ctx.restore();
}

function connectToStream(defaults) {
  if (source) {
    try { source.close(); } catch (err) { console.debug("close stream", err); }
  }
  const params = new URLSearchParams();
  params.set("symbol", defaults.symbol);
  params.set("market", defaults.market);

  source = new EventSource(`/stream/turnover?${params.toString()}`);
  setStatus("Connecting…", "connecting");

  source.onopen = () => setStatus("Live", "ok");
  source.onerror = () => setStatus("Reconnecting…", "error");

  source.addEventListener("turnover", (event) => {
    const data = JSON.parse(event.data);
    currentHistory = data.turnoverHistory.slice(-60);
    updateMetricCards(data.turnoverRate);
    drawChart();
    renderImbalance(data.imbalance);
  });
}

async function bootstrap() {
  setStatus("Loading…", "info");
  try {
    const res = await fetch("/api/defaults");
    if (!res.ok) throw new Error(`defaults status ${res.status}`);
    const defaults = await res.json();
    setRuntimeConfigText(defaults);
    configureCanvasSize();
    connectToStream(defaults);
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

function renderImbalance(value) {
  if (!imbalanceBar || !imbalanceLabel) return;
  const ratio = typeof value === "number" && isFinite(value) ? value : 1;
  // Visual representation: center at 50%. If ratio > 1, fill to the right; if < 1, fill to the left.
  const containerWidth = imbalanceBar.clientWidth || imbalanceBar.getBoundingClientRect().width || 300;
  const center = 0.5 * containerWidth;
  // Map ratio to a bounded fill length. Use atan to keep extremes controlled.
  const scaled = Math.atan((ratio - 1) * 2) / (Math.PI / 2); // [-1,1]
  const fillHalf = Math.abs(scaled) * center;
  // Clear previous
  imbalanceBar.innerHTML = "";
  const fill = document.createElement("div");
  fill.style.position = "absolute";
  fill.style.top = "0";
  fill.style.bottom = "0";
  if (ratio >= 1) {
    // Right fill (amber)
    fill.style.left = `${center}px`;
    fill.style.width = `${fillHalf}px`;
    fill.style.background = "linear-gradient(90deg, rgba(251,191,36,0.35), rgba(251,191,36,0.75))";
  } else {
    // Left fill (cyan)
    fill.style.left = `${center - fillHalf}px`;
    fill.style.width = `${fillHalf}px`;
    fill.style.background = "linear-gradient(90deg, rgba(34,211,238,0.75), rgba(34,211,238,0.35))";
  }
  imbalanceBar.appendChild(fill);
  // Center marker
  const midLine = document.createElement("div");
  midLine.style.position = "absolute";
  midLine.style.left = `${center - 1}px`;
  midLine.style.top = "0";
  midLine.style.bottom = "0";
  midLine.style.width = "2px";
  midLine.style.background = "rgba(148,163,184,0.5)";
  imbalanceBar.appendChild(midLine);
  // Label
  imbalanceLabel.textContent = `${ratio.toFixed(2)}× (bid/ask)`;
}
