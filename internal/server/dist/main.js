const statusEl = document.getElementById("status");
const runtimeEl = document.getElementById("runtime-config");
const hintEl = document.getElementById("empty-hint");
const chartCanvas = document.getElementById("turnover-chart");
const vpinChartCanvas = document.getElementById("vpin-chart");
const vpinFilteredChartCanvas = document.getElementById("vpin-filtered-chart");

const bidEl = document.getElementById("bid-placements");
const askEl = document.getElementById("ask-placements");
const totalEl = document.getElementById("total-changes");
const ordersPerMinuteEl = document.getElementById("orders-per-minute");
const vpinEl = document.getElementById("vpin");
const vpinSpark = document.getElementById("vpin-spark");
const vpinBucketInput = document.getElementById("vpin-bucket");
const vpinBucketBaseInput = document.getElementById("vpin-bucket-base");
const vpinModeSelect = document.getElementById("vpin-mode");
const vpinWindowInput = document.getElementById("vpin-window");
const applyVpinBtn = document.getElementById("apply-vpin");
const imbalanceBar = document.getElementById("imbalance-bar");
const imbalanceLabel = document.getElementById("imbalance-label");

let currentHistory = [];
let source = null;
let vpinHistory = [];
let vpinFilteredHistory = [];
let VPIN_EMA_ALPHA = parseFloat(localStorage.getItem('vpinAlpha') || '0.20');
if (isNaN(VPIN_EMA_ALPHA) || VPIN_EMA_ALPHA <= 0 || VPIN_EMA_ALPHA >= 1) VPIN_EMA_ALPHA = 0.20;
let currentDefaults = null;

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

function updateVPIN(vpin) {
  if (!vpinEl) return;
  if (typeof vpin === "number" && isFinite(vpin)) {
    vpinEl.textContent = vpin.toFixed(2);
    vpinHistory.push(vpin);
    if (vpinHistory.length > 120) vpinHistory = vpinHistory.slice(-120);
    // Update EMA filtered series
    if (vpinFilteredHistory.length === 0) {
      vpinFilteredHistory.push(vpin);
    } else {
      const prev = vpinFilteredHistory[vpinFilteredHistory.length - 1];
      const next = VPIN_EMA_ALPHA * vpin + (1 - VPIN_EMA_ALPHA) * prev;
      vpinFilteredHistory.push(next);
    }
    if (vpinFilteredHistory.length > 120) vpinFilteredHistory = vpinFilteredHistory.slice(-120);
    drawVPINSpark();
  }
}

function drawVPINSpark() {
  if (!vpinSpark) return;
  const ctx = vpinSpark.getContext("2d");
  if (!ctx) return;
  const width = vpinSpark.width;
  const height = vpinSpark.height;
  ctx.setTransform(1,0,0,1,0,0);
  ctx.clearRect(0,0,width,height);
  // background
  ctx.fillStyle = "rgba(15,23,42,0.5)";
  ctx.fillRect(0,0,width,height);
  if (!vpinHistory.length) return;
  const maxV = Math.max(...vpinHistory, 1);
  const minV = Math.min(...vpinHistory, 0);
  const range = Math.max(maxV - minV, 0.05);
  ctx.beginPath();
  ctx.strokeStyle = "#93c5fd";
  ctx.lineWidth = 1.5;
  for (let i = 0; i < vpinHistory.length; i++) {
    const x = (i / Math.max(vpinHistory.length - 1, 1)) * (width - 2) + 1;
    const y = height - 1 - ((vpinHistory[i] - minV) / range) * (height - 2);
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  }
  ctx.stroke();
}

function configureCanvasSize() {
  if (!chartCanvas) return;
  const dpr = window.devicePixelRatio || 1;
  const rect = chartCanvas.getBoundingClientRect();
  const width = rect.width || 900;
  const height = rect.height || 360;
  chartCanvas.width = Math.round(width * dpr);
  chartCanvas.height = Math.round(height * dpr);
  // VPIN canvas sizing
  if (vpinChartCanvas) {
    const rect2 = vpinChartCanvas.getBoundingClientRect();
    const width2 = rect2.width || 900;
    const height2 = rect2.height || 180;
    vpinChartCanvas.width = Math.round(width2 * dpr);
    vpinChartCanvas.height = Math.round(height2 * dpr);
  }
  if (vpinFilteredChartCanvas) {
    const rect3 = vpinFilteredChartCanvas.getBoundingClientRect();
    const width3 = rect3.width || 900;
    const height3 = rect3.height || 180;
    vpinFilteredChartCanvas.width = Math.round(width3 * dpr);
    vpinFilteredChartCanvas.height = Math.round(height3 * dpr);
  }
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
  // Include VPIN config if present
  const storedBucket = Number(localStorage.getItem("vpinBucket")) || 50000;
  const storedWindow = Number(localStorage.getItem("vpinWindow")) || 60;
  const storedMode = localStorage.getItem("vpinMode") || "quote";
  const storedBucketBase = Number(localStorage.getItem("vpinBucketBase")) || 10;
  params.set("vpinBucket", String(storedBucket));
  params.set("vpinWindow", String(storedWindow));
  params.set("vpinMode", storedMode);
  params.set("vpinBucketBase", String(storedBucketBase));
  // Butterworth options
  const butterOn = (localStorage.getItem("butterOn") || "off") === "on";
  const butterCut = Number(localStorage.getItem("butterCutoff") || 0.05);
  if (butterOn) params.set("butter", "1");
  if (butterCut > 0) params.set("butterCutoff", String(butterCut));

  source = new EventSource(`/stream/turnover?${params.toString()}`);
  setStatus("Connecting…", "connecting");

  source.onopen = () => setStatus("Live", "ok");
  source.onerror = () => setStatus("Reconnecting…", "error");

  source.addEventListener("turnover", (event) => {
    const data = JSON.parse(event.data);
    currentHistory = data.turnoverHistory.slice(-60);
    updateMetricCards(data.turnoverRate);
    updateVPIN(data.vpin);
    // Optional: log VPIN bucket debug to console for troubleshooting
    if (typeof data.vpinBucketCount === 'number') {
      console.debug('VPIN buckets:', data.vpinBucketCount, 'last end ms:', data.vpinLastBucket);
    }
    drawChart();
    renderImbalance(data.imbalance);
    drawVPINChart();
    drawVPINFilteredChart();
  });
}

async function bootstrap() {
  setStatus("Loading…", "info");
  try {
    const res = await fetch("/api/defaults");
    if (!res.ok) throw new Error(`defaults status ${res.status}`);
    const defaults = await res.json();
    currentDefaults = defaults;
    setRuntimeConfigText(defaults);
    configureCanvasSize();
    connectToStream(defaults);
    // Seed inputs from storage
    const storedBucket = Number(localStorage.getItem("vpinBucket")) || 50000;
    const storedBucketBase = Number(localStorage.getItem("vpinBucketBase")) || 10;
    const storedMode = localStorage.getItem("vpinMode") || "quote";
    const storedWindow = Number(localStorage.getItem("vpinWindow")) || 60;
    const storedButterOn = (localStorage.getItem("butterOn") || "off") === "on";
    const storedButterCut = Number(localStorage.getItem("butterCutoff") || 0.05);
    if (vpinBucketInput) vpinBucketInput.value = String(storedBucket);
    if (vpinBucketBaseInput) vpinBucketBaseInput.value = String(storedBucketBase);
    if (vpinModeSelect) vpinModeSelect.value = storedMode;
    if (vpinWindowInput) vpinWindowInput.value = String(storedWindow);
    if (applyVpinBtn) {
      applyVpinBtn.addEventListener("click", () => {
        const bucket = Math.max(1, Number(vpinBucketInput?.value || 50000));
        const bucketBase = Math.max(0.00000001, Number(vpinBucketBaseInput?.value || 10));
        const mode = (vpinModeSelect?.value || "quote").toLowerCase() === "base" ? "base" : "quote";
        const windowMin = Math.max(1, Number(vpinWindowInput?.value || 60));
        localStorage.setItem("vpinBucket", String(bucket));
        localStorage.setItem("vpinBucketBase", String(bucketBase));
        localStorage.setItem("vpinMode", mode);
        localStorage.setItem("vpinWindow", String(windowMin));
        const butterToggle = document.getElementById('butter-toggle');
        const butterCutoff = document.getElementById('butter-cutoff');
        const on = butterToggle && butterToggle.checked;
        const fc = Math.max(0.01, Math.min(0.49, Number(butterCutoff?.value || 0.05)));
        localStorage.setItem("butterOn", on ? "on" : "off");
        localStorage.setItem("butterCutoff", String(fc));
        vpinHistory = [];
        if (currentDefaults) connectToStream(currentDefaults);
      });
    }
    const bt = document.getElementById('butter-toggle');
    const bc = document.getElementById('butter-cutoff');
    if (bt) bt.checked = storedButterOn;
    if (bc) bc.value = String(storedButterCut);
    // Overlay toggle wiring
    const overlayToggle = document.getElementById('vpin-overlay-toggle');
    if (overlayToggle) {
      const stored = localStorage.getItem('vpinOverlay') || 'off';
      overlayToggle.checked = stored === 'on';
      overlayToggle.addEventListener('change', () => {
        localStorage.setItem('vpinOverlay', overlayToggle.checked ? 'on' : 'off');
        drawVPINChart();
      });
    }
    // Initialize EMA alpha controls
    const alphaSlider = document.getElementById('vpin-alpha');
    const alphaReadout = document.getElementById('vpin-alpha-readout');
    const alphaLabel = document.getElementById('vpin-alpha-label');
    if (alphaSlider && alphaReadout) {
      alphaSlider.value = VPIN_EMA_ALPHA.toFixed(2);
      alphaReadout.textContent = VPIN_EMA_ALPHA.toFixed(2);
      if (alphaLabel) alphaLabel.textContent = VPIN_EMA_ALPHA.toFixed(2);
      alphaSlider.addEventListener('input', (e) => {
        const val = parseFloat(e.target.value);
        if (!isNaN(val) && val > 0 && val < 1) {
          VPIN_EMA_ALPHA = val;
          localStorage.setItem('vpinAlpha', VPIN_EMA_ALPHA.toFixed(2));
          alphaReadout.textContent = VPIN_EMA_ALPHA.toFixed(2);
          if (alphaLabel) alphaLabel.textContent = VPIN_EMA_ALPHA.toFixed(2);
          // Rebuild filtered history from raw history
          vpinFilteredHistory = [];
          for (let i = 0; i < vpinHistory.length; i++) {
            const x = vpinHistory[i];
            const prev = vpinFilteredHistory.length ? vpinFilteredHistory[vpinFilteredHistory.length - 1] : x;
            vpinFilteredHistory.push(VPIN_EMA_ALPHA * x + (1 - VPIN_EMA_ALPHA) * prev);
          }
          drawVPINFilteredChart();
          drawVPINSpark();
        }
      });
    }
  } catch (err) {
    console.error("failed to load defaults", err);
    setStatus("Defaults unavailable", "error");
  }
}

window.addEventListener("DOMContentLoaded", bootstrap);
window.addEventListener("resize", () => {
  configureCanvasSize();
  drawChart();
  drawVPINChart();
  drawVPINFilteredChart();
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

// Full-width VPIN time-series chart
function drawVPINChart() {
  if (!vpinChartCanvas) return;
  const ctx = vpinChartCanvas.getContext("2d");
  if (!ctx) return;
  ctx.setTransform(1,0,0,1,0,0);
  ctx.clearRect(0,0,vpinChartCanvas.width,vpinChartCanvas.height);

  const rect = vpinChartCanvas.getBoundingClientRect();
  const width = rect.width || 900;
  const height = rect.height || 180;
  ctx.save();
  ctx.scale(vpinChartCanvas.width / width, vpinChartCanvas.height / height);

  // background
  ctx.fillStyle = "rgba(15, 23, 42, 0.6)";
  ctx.fillRect(0, 0, width, height);

  const padding = 28;
  const chartWidth = width - padding * 2;
  const chartHeight = height - padding * 2;

  // grid
  ctx.strokeStyle = "rgba(148, 163, 184, 0.22)";
  ctx.lineWidth = 1;
  ctx.setLineDash([4,6]);
  const gridLines = 3;
  for (let i = 0; i <= gridLines; i++) {
    const y = padding + (chartHeight / gridLines) * i;
    ctx.beginPath();
    ctx.moveTo(padding, y);
    ctx.lineTo(padding + chartWidth, y);
    ctx.stroke();
  }
  ctx.setLineDash([]);

  if (!vpinHistory.length) { ctx.restore(); return; }

  const maxV = Math.max(...vpinHistory, 1);
  const minV = Math.min(...vpinHistory, 0);
  const range = Math.max(maxV - minV, 0.05);

  // area under curve (raw)
  ctx.beginPath();
  for (let i = 0; i < vpinHistory.length; i++) {
    const x = padding + (chartWidth * i) / Math.max(vpinHistory.length - 1, 1);
    const y = padding + chartHeight - ((vpinHistory[i] - minV) / range) * chartHeight;
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  }
  ctx.lineTo(padding + chartWidth, padding + chartHeight);
  ctx.lineTo(padding, padding + chartHeight);
  ctx.closePath();
  ctx.fillStyle = "rgba(147, 197, 253, 0.18)"; // blue-300 alpha
  ctx.fill();

  // line (raw)
  ctx.beginPath();
  ctx.strokeStyle = "#93c5fd";
  ctx.lineWidth = 2;
  for (let i = 0; i < vpinHistory.length; i++) {
    const x = padding + (chartWidth * i) / Math.max(vpinHistory.length - 1, 1);
    const y = padding + chartHeight - ((vpinHistory[i] - minV) / range) * chartHeight;
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  }
  ctx.stroke();

  // optional overlay: filtered line on raw chart
  const overlayOn = (localStorage.getItem('vpinOverlay') || 'off') === 'on';
  if (overlayOn && vpinFilteredHistory.length) {
    ctx.beginPath();
    ctx.strokeStyle = "#34d399"; // emerald filtered
    ctx.lineWidth = 2;
    for (let i = 0; i < vpinFilteredHistory.length; i++) {
      const x = padding + (chartWidth * i) / Math.max(vpinFilteredHistory.length - 1, 1);
      const y = padding + chartHeight - ((vpinFilteredHistory[i] - minV) / range) * chartHeight;
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.stroke();
  }

  // y-axis labels (min/avg/max)
  ctx.fillStyle = "rgba(203,213,225,0.7)";
  ctx.font = "12px ui-sans-serif, system-ui, -apple-system, sans-serif";
  const avg = vpinHistory.reduce((a,b)=>a+b,0) / vpinHistory.length;
  const labels = [
    {v: maxV, y: padding, text: maxV.toFixed(2)},
    {v: avg, y: padding + chartHeight - ((avg - minV)/range)*chartHeight, text: avg.toFixed(2)},
    {v: minV, y: padding + chartHeight, text: minV.toFixed(2)},
  ];
  labels.forEach(l => {
    ctx.fillText(l.text, padding + chartWidth - 36, Math.min(height - 6, Math.max(12, l.y)));
  });

  ctx.restore();
}

// Filtered VPIN time-series chart (EMA)
function drawVPINFilteredChart() {
  if (!vpinFilteredChartCanvas) return;
  const ctx = vpinFilteredChartCanvas.getContext("2d");
  if (!ctx) return;
  ctx.setTransform(1,0,0,1,0,0);
  ctx.clearRect(0,0,vpinFilteredChartCanvas.width,vpinFilteredChartCanvas.height);

  const rect = vpinFilteredChartCanvas.getBoundingClientRect();
  const width = rect.width || 900;
  const height = rect.height || 180;
  ctx.save();
  ctx.scale(vpinFilteredChartCanvas.width / width, vpinFilteredChartCanvas.height / height);

  // background
  ctx.fillStyle = "rgba(15, 23, 42, 0.6)";
  ctx.fillRect(0, 0, width, height);

  const padding = 28;
  const chartWidth = width - padding * 2;
  const chartHeight = height - padding * 2;

  // grid
  ctx.strokeStyle = "rgba(148, 163, 184, 0.22)";
  ctx.lineWidth = 1;
  ctx.setLineDash([4,6]);
  const gridLines = 3;
  for (let i = 0; i <= gridLines; i++) {
    const y = padding + (chartHeight / gridLines) * i;
    ctx.beginPath();
    ctx.moveTo(padding, y);
    ctx.lineTo(padding + chartWidth, y);
    ctx.stroke();
  }
  ctx.setLineDash([]);

  if (!vpinFilteredHistory.length) { ctx.restore(); return; }

  const maxV = Math.max(...vpinFilteredHistory, 1);
  const minV = Math.min(...vpinFilteredHistory, 0);
  const range = Math.max(maxV - minV, 0.05);

  // area under curve
  ctx.beginPath();
  for (let i = 0; i < vpinFilteredHistory.length; i++) {
    const x = padding + (chartWidth * i) / Math.max(vpinFilteredHistory.length - 1, 1);
    const y = padding + chartHeight - ((vpinFilteredHistory[i] - minV) / range) * chartHeight;
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  }
  ctx.lineTo(padding + chartWidth, padding + chartHeight);
  ctx.lineTo(padding, padding + chartHeight);
  ctx.closePath();
  ctx.fillStyle = "rgba(110, 231, 183, 0.18)"; // emerald-ish area
  ctx.fill();

  // line
  ctx.beginPath();
  ctx.strokeStyle = "#34d399"; // emerald-400
  ctx.lineWidth = 2;
  for (let i = 0; i < vpinFilteredHistory.length; i++) {
    const x = padding + (chartWidth * i) / Math.max(vpinFilteredHistory.length - 1, 1);
    const y = padding + chartHeight - ((vpinFilteredHistory[i] - minV) / range) * chartHeight;
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  }
  ctx.stroke();

  // y-axis labels (min/avg/max)
  ctx.fillStyle = "rgba(203,213,225,0.7)";
  ctx.font = "12px ui-sans-serif, system-ui, -apple-system, sans-serif";
  const avg = vpinFilteredHistory.reduce((a,b)=>a+b,0) / vpinFilteredHistory.length;
  const labels = [
    {v: maxV, y: padding, text: maxV.toFixed(2)},
    {v: avg, y: padding + chartHeight - ((avg - minV)/range)*chartHeight, text: avg.toFixed(2)},
    {v: minV, y: padding + chartHeight, text: minV.toFixed(2)},
  ];
  labels.forEach(l => {
    ctx.fillText(l.text, padding + chartWidth - 36, Math.min(height - 6, Math.max(12, l.y)));
  });

  ctx.restore();
}
