const basePath = (window.__EDEC_BASE_PATH__ || "").replace(/\/$/, "");
const budgetPresetsBase = [1, 2, 5, 10, 15, 20];
const capitalPresetsBase = [50, 100, 500, 1000, 5000];
const chartWindows = [
  { id: "1m", label: "1M", durationMs: 60 * 1000 },
  { id: "3m", label: "3M", durationMs: 3 * 60 * 1000 },
  { id: "10m", label: "10M", durationMs: 10 * 60 * 1000 },
  { id: "max", label: "MAX", durationMs: null },
];

const state = {
  snapshot: null,
  focusCoin: null,
  series: {},
  ws: null,
  reconnectTimer: null,
  fallbackTimer: null,
  connection: "connecting",
  lastUpdateAt: null,
  pendingAction: false,
  confirmAction: null,
  chartWindow: "10m",
};

const els = {
  connection: document.getElementById("pill-connection"),
  mode: document.getElementById("pill-mode"),
  run: document.getElementById("pill-run"),
  updated: document.getElementById("pill-updated"),
  focusTitle: document.getElementById("focus-title"),
  focusSubtitle: document.getElementById("focus-subtitle"),
  focusChooser: document.getElementById("focus-chooser"),
  focusStats: document.getElementById("focus-stats"),
  chartWindowRow: document.getElementById("chart-window-row"),
  chartStatus: document.getElementById("chart-status"),
  chart: document.getElementById("focus-chart"),
  resolutionRail: document.getElementById("resolution-rail"),
  marketBoard: document.getElementById("market-board"),
  recentTrades: document.getElementById("recent-trades"),
  systemList: document.getElementById("system-list"),
  budgetRow: document.getElementById("budget-row"),
  capitalRow: document.getElementById("capital-row"),
  toastStack: document.getElementById("toast-stack"),
  confirmModal: document.getElementById("confirm-modal"),
  confirmTitle: document.getElementById("confirm-title"),
  confirmBody: document.getElementById("confirm-body"),
  confirmApprove: document.getElementById("confirm-approve"),
  confirmCancel: document.getElementById("confirm-cancel"),
  metricBalance: document.getElementById("metric-balance"),
  metricCapital: document.getElementById("metric-capital"),
  metricPnl: document.getElementById("metric-pnl"),
  metricWinrate: document.getElementById("metric-winrate"),
  metricOpen: document.getElementById("metric-open"),
  metricTrades: document.getElementById("metric-trades"),
  metricBudget: document.getElementById("metric-budget"),
  metricState: document.getElementById("metric-state"),
};

window.addEventListener("resize", () => drawChart());
window.setInterval(() => {
  if (!state.snapshot) return;
  renderHeader();
  renderChartStatus();
}, 1000);

els.confirmCancel.addEventListener("click", closeConfirm);
els.confirmApprove.addEventListener("click", async () => {
  if (!state.confirmAction) return;
  const action = state.confirmAction;
  closeConfirm();
  await action();
});

document.querySelectorAll("[data-action]").forEach((button) => {
  button.addEventListener("click", async () => {
    const action = button.dataset.action;
    if (action === "kill") {
      openConfirm(
        "Activate kill switch?",
        "This immediately halts scanning and enables the safety stop.",
        () => runAction("/api/actions/kill"),
      );
      return;
    }
    if (action === "reset-stats") {
      openConfirm(
        "Reset paper stats?",
        "This clears displayed paper metrics and risk counters for the current session.",
        () => runAction("/api/actions/reset-stats"),
      );
      return;
    }
    if (action === "start") {
      await runAction("/api/actions/start");
      return;
    }
    if (action === "stop") {
      await runAction("/api/actions/stop");
    }
  });
});

boot().catch((error) => {
  console.error(error);
  showToast(error.message || "Dashboard failed to load", "error");
});

async function boot() {
  await hydrateInitialState();
  connectWebSocket();
}

async function hydrateInitialState() {
  const response = await fetch(apiUrl("/api/state?history=1"));
  if (!response.ok) {
    throw new Error(`State request failed (${response.status})`);
  }
  const snapshot = await response.json();
  applySnapshot(snapshot, { replaceSeries: true });
  render();
}

function connectWebSocket() {
  const url = new URL(apiUrl("/api/ws"), window.location.origin);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";

  state.connection = "connecting";
  renderConnection();

  const ws = new WebSocket(url);
  state.ws = ws;

  ws.addEventListener("open", () => {
    state.connection = "live";
    stopFallbackPolling();
    renderConnection();
    renderChartStatus();
  });

  ws.addEventListener("message", (event) => {
    try {
      const payload = JSON.parse(event.data);
      handleSocketPayload(payload);
    } catch (error) {
      console.error("Bad websocket payload", error);
    }
  });

  ws.addEventListener("close", () => scheduleReconnect());
  ws.addEventListener("error", () => scheduleReconnect());
}

function scheduleReconnect() {
  if (state.ws) {
    state.ws = null;
  }
  state.connection = "retrying";
  renderConnection();
  renderChartStatus();
  startFallbackPolling();

  if (state.reconnectTimer) {
    return;
  }

  state.reconnectTimer = window.setTimeout(() => {
    state.reconnectTimer = null;
    connectWebSocket();
  }, 2000);
}

function startFallbackPolling() {
  if (state.fallbackTimer) return;
  state.fallbackTimer = window.setInterval(async () => {
    try {
      const response = await fetch(apiUrl("/api/state?history=1"));
      if (!response.ok) return;
      const snapshot = await response.json();
      applySnapshot(snapshot, { replaceSeries: true });
      render();
    } catch (error) {
      console.error("Fallback state refresh failed", error);
    }
  }, 2000);
}

function stopFallbackPolling() {
  if (!state.fallbackTimer) return;
  window.clearInterval(state.fallbackTimer);
  state.fallbackTimer = null;
}

function handleSocketPayload(payload) {
  if (!payload || !payload.type) return;
  if (payload.type === "snapshot") {
    applySnapshot(payload.data, { replaceSeries: true });
    render();
    return;
  }
  if (payload.type === "patch" || payload.type === "ack") {
    applySnapshot(payload.data, { replaceSeries: false });
    render();
    return;
  }
  if (payload.type === "error") {
    showToast(payload.message || "Dashboard stream error", "error");
  }
}

function applySnapshot(snapshot, { replaceSeries }) {
  if (!snapshot) return;
  state.snapshot = snapshot;
  state.lastUpdateAt = Date.now();

  if (replaceSeries) {
    const nextSeries = {};
    const incomingSeries = snapshot.series || {};
    for (const [coin, series] of Object.entries(incomingSeries)) {
      nextSeries[coin] = Array.isArray(series) ? series.slice(-600) : [];
    }
    for (const coinEntry of snapshot.coins || []) {
      if (!nextSeries[coinEntry.coin] && Array.isArray(coinEntry.series)) {
        nextSeries[coinEntry.coin] = coinEntry.series.slice(-600);
      }
    }
    state.series = nextSeries;
  } else {
    for (const coinEntry of snapshot.coins || []) {
      if (!state.series[coinEntry.coin]) {
        state.series[coinEntry.coin] = [];
      }
      appendSeriesPoint(coinEntry.coin, coinEntry);
    }
  }

  if (!state.focusCoin) {
    state.focusCoin = snapshot.coins?.[0]?.coin || null;
  }
  if (state.focusCoin && !snapshot.coins?.find((coin) => coin.coin === state.focusCoin)) {
    state.focusCoin = snapshot.coins?.[0]?.coin || null;
  }
}

function appendSeriesPoint(coin, coinEntry) {
  const series = state.series[coin];
  const point = {
    ts: state.snapshot.generated_at,
    market_slug: coinEntry.market?.slug || null,
    reference_price: coinEntry.market?.reference_price ?? null,
    spot_price: coinEntry.price?.spot ?? null,
    up_ask: coinEntry.book?.up?.best_ask ?? null,
    down_ask: coinEntry.book?.down?.best_ask ?? null,
  };
  const last = series[series.length - 1];
  if (last && last.ts === point.ts) {
    series[series.length - 1] = point;
  } else {
    series.push(point);
    if (series.length > 600) {
      series.shift();
    }
  }
}

function render() {
  if (!state.snapshot) return;
  renderConnection();
  renderHeader();
  renderFocusChooser();
  renderFocusStats();
  renderChartWindowChips();
  renderMetrics();
  renderBudgetChips();
  renderCapitalChips();
  renderResolutionRail();
  renderMarketBoard();
  renderTrades();
  renderSystem();
  renderChartStatus();
  drawChart();
}

function renderConnection() {
  const map = {
    connecting: ["Connecting", ""],
    live: ["Live WebSocket", "online"],
    retrying: ["Fallback Polling", "retrying"],
  };
  const [text, klass] = map[state.connection] || ["Offline", "danger"];
  els.connection.textContent = text;
  els.connection.className = `status-pill ${klass}`.trim();
}

function renderHeader() {
  const bot = state.snapshot.bot || {};
  els.mode.textContent = bot.mode_label || "Unknown";
  els.run.textContent = bot.dry_run ? "Dry Run" : "Wet Run";
  els.run.className = `status-pill ${bot.dry_run ? "dry" : "wet"}`.trim();
  els.updated.textContent = state.lastUpdateAt ? `Updated ${formatAgo(state.lastUpdateAt)}` : "Waiting for data";
}

function renderFocusChooser() {
  const coins = state.snapshot.coins || [];
  els.focusChooser.innerHTML = "";
  for (const coin of coins) {
    const button = document.createElement("button");
    button.className = `coin-focus-btn ${state.focusCoin === coin.coin ? "active" : ""}`.trim();
    button.textContent = coin.coin.toUpperCase();
    button.addEventListener("click", () => {
      state.focusCoin = coin.coin;
      render();
    });
    els.focusChooser.appendChild(button);
  }

  const focus = getFocusCoin();
  els.focusTitle.textContent = focus ? `${focus.coin.toUpperCase()} ${focus.market?.label || "Live Market"}` : "Market Overview";
  els.focusSubtitle.textContent = focus ? describeFocusSubtitle(focus) : "Waiting for market context";
}

function renderFocusStats() {
  const focus = getFocusCoin();
  if (!focus) {
    els.focusStats.innerHTML = "";
    return;
  }

  const spot = focus.price?.spot;
  const ref = focus.market?.reference_price;
  const upAsk = focus.book?.up?.best_ask;
  const downAsk = focus.book?.down?.best_ask;
  const vel30 = focus.price?.velocity_30s;
  const vel60 = focus.price?.velocity_60s;
  const drift = calculateReferenceDrift(spot, ref);
  const cards = [
    {
      label: "Spot",
      value: formatUsd(spot),
      note: Number.isFinite(ref) ? `Ref ${formatUsd(ref)} - ${formatReferenceDrift(drift)}` : "Reference pending",
      className: classForNumber(drift?.usd),
    },
    {
      label: "UP Ask",
      value: formatProb(upAsk),
      note: describeBookDepth(focus.book?.up?.ask_depth_usd),
      className: focus.signal?.side === "up" ? "positive" : "",
    },
    {
      label: "DOWN Ask",
      value: formatProb(downAsk),
      note: describeBookDepth(focus.book?.down?.ask_depth_usd),
      className: focus.signal?.side === "down" ? "negative" : "",
    },
    {
      label: "Momentum",
      value: formatSignedPercent(vel30),
      note: `60s ${formatSignedPercent(vel60)} - ${focus.signal?.label || "No signal"}`,
      className: classForNumber(vel30),
    },
    {
      label: "Market Clock",
      value: formatDuration(focus.market?.seconds_remaining),
      note: focus.market?.accepting_orders ? "Accepting orders" : "Awaiting market",
      className: "",
    },
  ];

  els.focusStats.innerHTML = cards.map((card) => `
    <article class="focus-card">
      <p class="focus-label">${card.label}</p>
      <p class="focus-value ${card.className || ""}">${card.value}</p>
      <p class="focus-note">${card.note}</p>
    </article>
  `).join("");
}

function renderChartWindowChips() {
  els.chartWindowRow.innerHTML = "";
  for (const option of chartWindows) {
    const button = document.createElement("button");
    button.className = `chip-btn ${state.chartWindow === option.id ? "active" : ""}`.trim();
    button.innerHTML = `<span class="window-label">${option.label}</span>`;
    button.addEventListener("click", () => {
      state.chartWindow = option.id;
      renderChartWindowChips();
      renderChartStatus();
      drawChart();
      renderMarketBoard();
    });
    els.chartWindowRow.appendChild(button);
  }
}

function renderMetrics() {
  const paper = state.snapshot.stats?.paper || {};
  const bot = state.snapshot.bot || {};
  els.metricBalance.textContent = formatUsd(paper.current_balance);
  els.metricCapital.textContent = `of ${formatUsd(paper.total_capital)} capital`;
  els.metricPnl.textContent = formatUsd(paper.total_pnl, { signed: true });
  els.metricPnl.className = `metric-value ${classForNumber(paper.total_pnl)}`.trim();
  els.metricWinrate.textContent = `Win rate ${formatPct(paper.win_rate)}`;
  els.metricOpen.textContent = `${paper.open_positions ?? 0}`;
  els.metricTrades.textContent = `${paper.total_trades ?? 0} trades tracked`;
  els.metricBudget.textContent = formatUsd(bot.order_size_usd, { decimals: 0 });
  els.metricState.textContent = bot.kill_switch ? "Kill switch active" : bot.is_paused ? "Paused" : bot.is_active ? "Scanning live" : "Stopped";
}

function renderBudgetChips() {
  const current = state.snapshot.bot?.order_size_usd ?? 0;
  renderChipSet(els.budgetRow, withCurrentPreset(budgetPresetsBase, current), current, async (value) => {
    await runAction("/api/actions/set-budget", { budget: value });
  });
}

function renderCapitalChips() {
  const current = state.snapshot.stats?.paper?.total_capital ?? 0;
  renderChipSet(els.capitalRow, withCurrentPreset(capitalPresetsBase, current), current, async (value) => {
    openConfirm(
      `Set capital to ${formatUsd(value, { decimals: 0 })}?`,
      "This resets the paper bankroll to a new starting point.",
      () => runAction("/api/actions/set-capital", { capital: value }),
    );
  });
}

function renderChipSet(container, values, currentValue, onSelect) {
  container.innerHTML = "";
  for (const value of values) {
    const button = document.createElement("button");
    button.className = `chip-btn ${Math.abs(value - currentValue) < 0.001 ? "active" : ""}`.trim();
    button.textContent = formatUsd(value, { decimals: 0 });
    button.disabled = state.pendingAction;
    button.addEventListener("click", async () => onSelect(value));
    container.appendChild(button);
  }
}

function renderResolutionRail() {
  const focus = getFocusCoin();
  const resolutions = focus?.recent_resolution_details || [];
  if (!resolutions.length) {
    els.resolutionRail.innerHTML = `
      <article class="resolution-empty">
        <p class="resolution-note">No resolved markets logged yet for ${focus?.coin?.toUpperCase() || "this coin"}.</p>
      </article>
    `;
    return;
  }

  els.resolutionRail.innerHTML = resolutions.map((resolution) => {
    const winner = String(resolution.winner || "").toUpperCase();
    const tone = winner === "UP" ? "up" : "down";
    const movePct = calculateMovePct(resolution.open_price, resolution.close_price);
    return `
      <article class="resolution-card ${tone}">
        <div class="resolution-card-head">
          <span class="resolution-badge">${winner || "?"}</span>
          <span class="resolution-time">${formatShortTimestamp(resolution.resolved_at)}</span>
        </div>
        <p class="resolution-move ${classForNumber(movePct)}">${formatSignedPercent(movePct)}</p>
        <p class="resolution-meta">${formatUsd(resolution.open_price)} -> ${formatUsd(resolution.close_price)}</p>
        <p class="resolution-slug">${shortMarketSlug(resolution.market_slug, focus.coin)}</p>
      </article>
    `;
  }).join("");
}

function renderMarketBoard() {
  const coins = state.snapshot.coins || [];
  els.marketBoard.innerHTML = "";
  for (const coin of coins) {
    const trend = calculateTrendPct(getRecentSpotSeries(coin.coin, 48));
    const card = document.createElement("article");
    card.className = `market-card ${state.focusCoin === coin.coin ? "is-focus" : ""}`.trim();
    card.innerHTML = `
      <div class="market-head">
        <div>
          <p class="metric-meta">${coin.market?.active ? "Live market" : "Waiting for market"}</p>
          <h3>${coin.coin.toUpperCase()}</h3>
        </div>
        <span class="signal-pill ${coin.signal?.kind || "none"}">${coin.signal?.label || "No signal"}</span>
      </div>
      <p class="board-value">${formatUsd(coin.price?.spot)}</p>
      <p class="price-note">${describeMarketNote(coin)}</p>
      ${renderSparklineCard(coin, trend)}
      <div class="track-group">
        <div class="track-label"><span>UP ask</span><span>${formatProb(coin.book?.up?.best_ask)}</span></div>
        <div class="track"><div class="track-fill up" style="width:${toTrackWidth(coin.book?.up?.best_ask)}%"></div></div>
        <div class="track-label"><span>DOWN ask</span><span>${formatProb(coin.book?.down?.best_ask)}</span></div>
        <div class="track"><div class="track-fill down" style="width:${toTrackWidth(coin.book?.down?.best_ask)}%"></div></div>
      </div>
      <div class="outcome-row">${renderOutcomes(coin.recent_outcomes)}</div>
    `;
    card.addEventListener("click", () => {
      state.focusCoin = coin.coin;
      render();
    });
    els.marketBoard.appendChild(card);
  }
}

function renderSparklineCard(coin, trend) {
  const points = getRecentSpotSeries(coin.coin, 48);
  return `
    <div class="market-sparkline">
      <div class="sparkline-meta">
        <span>${describeSparklineWindow(points.length)}</span>
        <strong class="${classForNumber(trend)}">${formatSignedPercent(trend)}</strong>
      </div>
      ${buildSparklineSvg(points, coin.market?.reference_price)}
    </div>
  `;
}

function buildSparklineSvg(points, referencePrice) {
  const width = 180;
  const height = 54;
  const inset = 4;
  const numeric = points.filter((point) => Number.isFinite(point.spot_price));
  if (!numeric.length) {
    return `
      <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-hidden="true">
        <text x="8" y="30" fill="rgba(142, 170, 177, 0.72)" font-size="10">No history yet</text>
      </svg>
    `;
  }

  const values = numeric.map((point) => point.spot_price);
  if (Number.isFinite(referencePrice)) {
    values.push(referencePrice);
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = Math.max(0.0001, max - min);
  const plotWidth = width - inset * 2;
  const plotHeight = height - inset * 2;
  const pathPoints = numeric.map((point, index) => {
    const x = inset + (numeric.length <= 1 ? plotWidth / 2 : (index / (numeric.length - 1)) * plotWidth);
    const y = inset + ((max - point.spot_price) / span) * plotHeight;
    return { x, y };
  });
  const path = pathPoints.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`).join(" ");
  const area = `${path} L ${pathPoints[pathPoints.length - 1].x.toFixed(2)} ${(height - inset).toFixed(2)} L ${pathPoints[0].x.toFixed(2)} ${(height - inset).toFixed(2)} Z`;
  const refY = Number.isFinite(referencePrice) ? inset + ((max - referencePrice) / span) * plotHeight : null;
  const end = pathPoints[pathPoints.length - 1];

  return `
    <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-hidden="true">
      ${Number.isFinite(refY) ? `<line x1="${inset}" y1="${refY.toFixed(2)}" x2="${width - inset}" y2="${refY.toFixed(2)}" stroke="rgba(245, 197, 96, 0.72)" stroke-dasharray="4 4" stroke-width="1"></line>` : ""}
      <path d="${area}" fill="rgba(122, 226, 255, 0.10)"></path>
      <path d="${path}" fill="none" stroke="rgba(122, 226, 255, 0.96)" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"></path>
      <circle cx="${end.x.toFixed(2)}" cy="${end.y.toFixed(2)}" r="2.8" fill="rgba(122, 226, 255, 1)"></circle>
    </svg>
  `;
}

function renderOutcomes(outcomes) {
  const list = Array.isArray(outcomes) ? outcomes : [];
  if (!list.length) {
    return `<span class="outcome-chip empty">No recent resolutions</span>`;
  }
  return list.map((outcome) => {
    const up = String(outcome).toUpperCase() === "UP";
    return `<span class="outcome-chip ${up ? "up" : "down"}">${up ? "UP" : "DOWN"}</span>`;
  }).join("");
}

function renderTrades() {
  const trades = state.snapshot.recent_trades || [];
  if (!trades.length) {
    els.recentTrades.innerHTML = `<article class="trade-row"><p class="trade-title">No trades yet</p><p class="trade-meta">Recent fills and dry-run outcomes will land here.</p></article>`;
    return;
  }
  els.recentTrades.innerHTML = trades.map((trade) => {
    const pnl = trade.actual_profit;
    const pnlClass = classForNumber(pnl);
    return `
      <article class="trade-row">
        <div class="trade-head">
          <div>
            <p class="trade-title">${(trade.coin || "").toUpperCase()} ${trade.strategy_type || ""}</p>
            <p class="trade-meta">${formatTimestamp(trade.timestamp)} - ${trade.status}</p>
          </div>
          <p class="trade-value ${pnlClass}">${pnl == null ? "pending" : formatUsd(pnl, { signed: true, decimals: 4 })}</p>
        </div>
      </article>
    `;
  }).join("");
}

function renderSystem() {
  const bot = state.snapshot.bot || {};
  const transport = state.snapshot.transport || {};
  const focus = getFocusCoin();
  const rows = [
    ["Run ID", bot.run_id || "--"],
    ["Config Hash", bot.config_hash || "--"],
    ["Update Cadence", `${transport.update_interval_ms || 0} ms`],
    ["Chart Buffer", `${transport.history_points || 0} points`],
    ["Chart Window", labelForWindow(state.chartWindow)],
    ["Focused Coin", focus?.coin?.toUpperCase() || "--"],
    ["Reference Source", focus?.market?.reference_source || "--"],
    ["Focused Signal", focus?.signal?.label || "--"],
    ["Last Generated", formatTimestamp(state.snapshot.generated_at)],
    ["Connection", state.connection],
  ];
  els.systemList.innerHTML = rows.map(([label, value]) => `<dt>${label}</dt><dd>${value}</dd>`).join("");
}

async function runAction(path, body = null) {
  if (state.pendingAction) return;
  state.pendingAction = true;
  render();
  try {
    const response = await fetch(apiUrl(path), {
      method: "POST",
      headers: body ? { "Content-Type": "application/json" } : {},
      body: body ? JSON.stringify(body) : null,
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(text || `Request failed (${response.status})`);
    }
    const payload = await response.json();
    if (payload.snapshot) {
      applySnapshot(payload.snapshot, { replaceSeries: false });
      render();
    }
    showToast(`${payload.action} applied`, "ok");
  } catch (error) {
    console.error(error);
    showToast(error.message || "Action failed", "error");
  } finally {
    state.pendingAction = false;
    render();
  }
}

function openConfirm(title, body, onApprove) {
  state.confirmAction = onApprove;
  els.confirmTitle.textContent = title;
  els.confirmBody.textContent = body;
  els.confirmModal.classList.remove("hidden");
}

function closeConfirm() {
  state.confirmAction = null;
  els.confirmModal.classList.add("hidden");
}

function showToast(message, variant = "ok") {
  const toast = document.createElement("div");
  toast.className = `toast ${variant}`;
  toast.textContent = message;
  els.toastStack.appendChild(toast);
  window.setTimeout(() => toast.remove(), 3400);
}

function drawChart() {
  const canvas = els.chart;
  const focus = getFocusCoin();
  if (!canvas || !focus) return;

  const rect = canvas.getBoundingClientRect();
  if (!rect.width || !rect.height) return;

  const dpr = window.devicePixelRatio || 1;
  canvas.width = Math.max(360, Math.floor(rect.width * dpr));
  canvas.height = Math.max(240, Math.floor(rect.height * dpr));

  const ctx = canvas.getContext("2d");
  ctx.setTransform(1, 0, 0, 1, 0, 0);
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, rect.width, rect.height);

  const points = getVisibleSeries(focus.coin);
  const panelValues = collectPanelValues(points);
  if (!panelValues.price.length && !panelValues.ask.length) {
    ctx.fillStyle = "rgba(142, 170, 177, 0.82)";
    ctx.font = '16px "Cascadia Code", monospace';
    ctx.fillText("Waiting for live series data", 24, 32);
    return;
  }

  const padding = { top: 32, right: 76, bottom: 42, left: 18 };
  const innerWidth = rect.width - padding.left - padding.right;
  const innerHeight = rect.height - padding.top - padding.bottom;
  const priceArea = {
    top: padding.top,
    bottom: padding.top + innerHeight * 0.7,
  };
  const askArea = {
    top: priceArea.bottom + 18,
    bottom: rect.height - padding.bottom,
  };
  const priceScale = buildScale(panelValues.price.length ? panelValues.price : [0, 1], priceArea.top, priceArea.bottom, 0.002);
  const askScale = buildScale(panelValues.ask.length ? panelValues.ask : [0, 1], askArea.top, askArea.bottom, 0.14);
  const projectX = (index) => padding.left + (points.length <= 1 ? innerWidth / 2 : (index / (points.length - 1)) * innerWidth);
  const projectPriceY = (value) => priceScale.project(value);
  const projectAskY = (value) => askScale.project(value);

  drawPanelGrid(ctx, rect, padding, priceArea, priceScale, "Price", formatUsd);
  drawPanelGrid(ctx, rect, padding, askArea, askScale, "Asks", formatProb);
  drawTimeAxis(ctx, points, rect, padding, projectX);
  drawMarketBoundaries(ctx, points, priceArea.top, askArea.bottom, projectX);

  const spotFill = ctx.createLinearGradient(0, padding.top, 0, rect.height - padding.bottom);
  spotFill.addColorStop(0, "rgba(122, 226, 255, 0.18)");
  spotFill.addColorStop(1, "rgba(122, 226, 255, 0.02)");

  drawSeriesLine(ctx, points, "spot_price", projectX, projectPriceY, {
    stroke: "rgba(122, 226, 255, 1)",
    width: 2.6,
    fill: spotFill,
    fillBottom: priceArea.bottom,
  });
  drawSeriesLine(ctx, points, "reference_price", projectX, projectPriceY, {
    stroke: "rgba(245, 197, 96, 0.95)",
    width: 1.4,
    dash: [8, 6],
  });
  drawSeriesLine(ctx, points, "up_ask", projectX, projectAskY, {
    stroke: "rgba(47, 226, 155, 0.92)",
    width: 1.5,
  });
  drawSeriesLine(ctx, points, "down_ask", projectX, projectAskY, {
    stroke: "rgba(255, 106, 111, 0.92)",
    width: 1.5,
  });

  drawCurrentMarker(ctx, lastNumericPoint(points, "spot_price"), projectX, projectPriceY, "rgba(122, 226, 255, 1)", 5.2);
  drawCurrentMarker(ctx, lastNumericPoint(points, "up_ask"), projectX, projectAskY, "rgba(47, 226, 155, 0.96)", 3.4);
  drawCurrentMarker(ctx, lastNumericPoint(points, "down_ask"), projectX, projectAskY, "rgba(255, 106, 111, 0.96)", 3.4);

  drawReferenceBadge(ctx, focus, points, projectPriceY, padding, rect.width);
  drawValueBadges(ctx, points, projectX, { spot_price: projectPriceY, up_ask: projectAskY, down_ask: projectAskY }, rect.width - padding.right + 8, padding, askArea.bottom);

  ctx.fillStyle = "rgba(235, 247, 245, 0.92)";
  ctx.font = '600 15px "Bahnschrift", sans-serif';
  ctx.fillText(`${focus.coin.toUpperCase()} live`, 20, 22);
  ctx.fillStyle = "rgba(142, 170, 177, 0.76)";
  ctx.font = '12px "Cascadia Code", monospace';
  ctx.fillText("Spot and strike", 20, priceArea.top + 14);
  ctx.fillText("UP and DOWN asks", 20, askArea.top + 14);
}

function buildScale(values, top, bottom, marginRatio) {
  const minRaw = Math.min(...values);
  const maxRaw = Math.max(...values);
  const margin = Math.max((maxRaw - minRaw) * marginRatio, Math.abs(maxRaw) * 0.002, 0.0001);
  const min = minRaw - margin;
  const max = maxRaw + margin;
  const span = Math.max(0.0001, max - min);
  return {
    min,
    max,
    span,
    top,
    bottom,
    project(value) {
      return top + ((max - value) / span) * (bottom - top);
    },
  };
}

function drawPanelGrid(ctx, rect, padding, area, scale, label, formatter) {
  const ticks = 5;
  ctx.save();
  ctx.strokeStyle = "rgba(255, 255, 255, 0.06)";
  ctx.fillStyle = "rgba(142, 170, 177, 0.78)";
  ctx.font = '12px "Cascadia Code", monospace';
  for (let index = 0; index < ticks; index += 1) {
    const ratio = index / (ticks - 1);
    const y = area.top + ratio * (area.bottom - area.top);
    const value = scale.max - ratio * (scale.max - scale.min);
    ctx.beginPath();
    ctx.moveTo(padding.left, y);
    ctx.lineTo(rect.width - padding.right, y);
    ctx.stroke();
    ctx.fillText(formatter(value), rect.width - padding.right + 8, y + 4);
  }

  const vTicks = 5;
  for (let index = 0; index < vTicks; index += 1) {
    const x = padding.left + (index / (vTicks - 1)) * (rect.width - padding.left - padding.right);
    ctx.beginPath();
    ctx.moveTo(x, area.top);
    ctx.lineTo(x, area.bottom);
    ctx.stroke();
  }
  ctx.fillText(label.toUpperCase(), padding.left + 4, area.top + 12);
  ctx.restore();
}

function drawTimeAxis(ctx, points, rect, padding, projectX) {
  ctx.save();
  ctx.fillStyle = "rgba(142, 170, 177, 0.74)";
  ctx.font = '12px "Cascadia Code", monospace';
  const tickCount = Math.min(4, points.length);
  for (let index = 0; index < tickCount; index += 1) {
    const pointIndex = tickCount === 1 ? points.length - 1 : Math.round((index / (tickCount - 1)) * (points.length - 1));
    const point = points[pointIndex];
    const label = formatChartTime(point?.ts);
    const x = projectX(pointIndex);
    ctx.fillText(label, x - 18, rect.height - 16);
  }
  ctx.restore();
}

function drawMarketBoundaries(ctx, points, top, bottom, projectX) {
  let previousSlug = points[0]?.market_slug || null;
  ctx.save();
  ctx.strokeStyle = "rgba(245, 197, 96, 0.18)";
  ctx.fillStyle = "rgba(245, 197, 96, 0.72)";
  ctx.setLineDash([4, 8]);
  ctx.font = '11px "Cascadia Code", monospace';
  for (let index = 1; index < points.length; index += 1) {
    const slug = points[index]?.market_slug || null;
    if (!slug || slug === previousSlug) continue;
    const x = projectX(index);
    ctx.beginPath();
    ctx.moveTo(x, top);
    ctx.lineTo(x, bottom);
    ctx.stroke();
    ctx.fillText("market", x + 4, top + 12);
    previousSlug = slug;
  }
  ctx.restore();
}

function drawSeriesLine(ctx, points, key, projectX, projectY, options) {
  const valid = points
    .map((point, index) => ({ index, value: point[key] }))
    .filter((point) => Number.isFinite(point.value));
  if (!valid.length) return;

  ctx.save();
  ctx.setLineDash(options.dash || []);
  ctx.strokeStyle = options.stroke;
  ctx.lineWidth = options.width;
  ctx.beginPath();
  for (let index = 0; index < valid.length; index += 1) {
    const point = valid[index];
    const x = projectX(point.index);
    const y = projectY(point.value);
    if (index === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  }

  if (options.fill) {
    const firstX = projectX(valid[0].index);
    const lastX = projectX(valid[valid.length - 1].index);
    ctx.lineTo(lastX, options.fillBottom);
    ctx.lineTo(firstX, options.fillBottom);
    ctx.closePath();
    ctx.fillStyle = options.fill;
    ctx.fill();

    ctx.beginPath();
    for (let index = 0; index < valid.length; index += 1) {
      const point = valid[index];
      const x = projectX(point.index);
      const y = projectY(point.value);
      if (index === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
  }

  ctx.stroke();
  ctx.restore();
}

function drawCurrentMarker(ctx, latest, projectX, projectY, fillStyle, radius) {
  if (!latest) return;
  const x = projectX(latest.index);
  const y = projectY(latest.value);
  ctx.save();
  ctx.shadowColor = fillStyle;
  ctx.shadowBlur = 16;
  ctx.fillStyle = fillStyle;
  ctx.beginPath();
  ctx.arc(x, y, radius, 0, Math.PI * 2);
  ctx.fill();
  ctx.restore();
}

function drawReferenceBadge(ctx, focus, points, projectY, padding, chartWidth) {
  const latestReference = lastNumericPoint(points, "reference_price");
  const value = latestReference?.value ?? focus.market?.reference_price;
  if (!Number.isFinite(value)) return;
  const y = projectY(value);
  const label = `Strike ${formatUsd(value)}`;
  const width = Math.min(148, Math.max(94, label.length * 7.4));
  const x = Math.max(padding.left + 4, chartWidth - padding.right - width - 10);

  ctx.save();
  ctx.fillStyle = "rgba(16, 24, 28, 0.88)";
  ctx.strokeStyle = "rgba(245, 197, 96, 0.5)";
  roundRect(ctx, x, y - 13, width, 24, 12);
  ctx.fill();
  ctx.stroke();
  ctx.fillStyle = "rgba(245, 197, 96, 0.95)";
  ctx.font = '12px "Cascadia Code", monospace';
  ctx.fillText(label, x + 10, y + 5);
  ctx.restore();
}

function drawValueBadges(ctx, points, projectX, projectYByKey, x, padding, canvasHeight) {
  const badges = [
    buildValueBadge("Spot", "spot_price", "rgba(122, 226, 255, 0.16)", "rgba(122, 226, 255, 0.82)", "rgba(235, 247, 245, 0.92)"),
    buildValueBadge("UP", "up_ask", "rgba(47, 226, 155, 0.14)", "rgba(47, 226, 155, 0.76)", "rgba(47, 226, 155, 0.95)"),
    buildValueBadge("DOWN", "down_ask", "rgba(255, 106, 111, 0.14)", "rgba(255, 106, 111, 0.76)", "rgba(255, 106, 111, 0.95)"),
  ]
    .map((badge) => {
      const latest = lastNumericPoint(points, badge.key);
      if (!latest) return null;
      return {
        ...badge,
        y: projectYByKey[badge.key](latest.value),
        value: latest.value,
      };
    })
    .filter(Boolean)
    .sort((a, b) => a.y - b.y);

  for (let index = 1; index < badges.length; index += 1) {
    if (badges[index].y - badges[index - 1].y < 24) {
      badges[index].y = badges[index - 1].y + 24;
    }
  }
  for (let index = badges.length - 2; index >= 0; index -= 1) {
    if (badges[index + 1].y > canvasHeight - padding.bottom - 6) {
      badges[index + 1].y = canvasHeight - padding.bottom - 6;
    }
    if (badges[index + 1].y - badges[index].y < 24) {
      badges[index].y = badges[index + 1].y - 24;
    }
  }

  badges.forEach((badge) => {
    drawBadge(ctx, x, Math.max(padding.top + 12, Math.min(canvasHeight - padding.bottom - 12, badge.y)), `${badge.label} ${formatBadgeValue(badge.key, badge.value)}`, badge.fill, badge.stroke, badge.text);
  });
}

function buildValueBadge(label, key, fill, stroke, text) {
  return { label, key, fill, stroke, text };
}

function drawBadge(ctx, x, y, label, fill, stroke, text) {
  const width = Math.max(58, label.length * 7.2 + 18);
  const height = 22;
  ctx.save();
  ctx.fillStyle = fill;
  ctx.strokeStyle = stroke;
  roundRect(ctx, x, y - height / 2, width, height, 11);
  ctx.fill();
  ctx.stroke();
  ctx.fillStyle = text;
  ctx.font = '11px "Cascadia Code", monospace';
  ctx.fillText(label, x + 9, y + 4);
  ctx.restore();
}

function roundRect(ctx, x, y, width, height, radius) {
  ctx.beginPath();
  ctx.moveTo(x + radius, y);
  ctx.arcTo(x + width, y, x + width, y + height, radius);
  ctx.arcTo(x + width, y + height, x, y + height, radius);
  ctx.arcTo(x, y + height, x, y, radius);
  ctx.arcTo(x, y, x + width, y, radius);
  ctx.closePath();
}

function renderChartStatus() {
  const focus = getFocusCoin();
  if (!focus || !state.snapshot) {
    els.chartStatus.textContent = "Waiting for live series";
    return;
  }
  const points = getVisibleSeries(focus.coin);
  const source = state.connection === "live" ? "ws live" : "fallback";
  const lag = state.snapshot.generated_at ? formatAgeFromIso(state.snapshot.generated_at) : "--";
  els.chartStatus.textContent = `${points.length} pts - ${labelForWindow(state.chartWindow)} - ${source} - ${lag}`;
}

function getFocusCoin() {
  return (state.snapshot?.coins || []).find((coin) => coin.coin === state.focusCoin) || null;
}

function getVisibleSeries(coin) {
  const series = state.series[coin] || [];
  if (!series.length) return [];
  const option = chartWindows.find((item) => item.id === state.chartWindow);
  if (!option || option.durationMs == null) {
    return series.slice(-600);
  }
  const lastTs = parseTimestampMs(series[series.length - 1]?.ts) || Date.now();
  let visible = series.filter((point) => {
    const ts = parseTimestampMs(point.ts);
    return Number.isFinite(ts) && (lastTs - ts) <= option.durationMs;
  });
  if (visible.length < 12) {
    const sampleMs = Math.max(250, state.snapshot?.transport?.history_sample_interval_ms || 1000);
    const fallbackCount = Math.max(12, Math.ceil(option.durationMs / sampleMs));
    visible = series.slice(-fallbackCount);
  }
  return visible;
}

function getRecentSpotSeries(coin, count) {
  return (state.series[coin] || []).slice(-count);
}

function collectPanelValues(points) {
  const values = { price: [], ask: [] };
  for (const point of points) {
    for (const key of ["spot_price", "reference_price"]) {
      if (Number.isFinite(point[key])) {
        values.price.push(point[key]);
      }
    }
    for (const key of ["up_ask", "down_ask"]) {
      if (Number.isFinite(point[key])) {
        values.ask.push(point[key]);
      }
    }
  }
  return values;
}

function lastNumericPoint(points, key) {
  for (let index = points.length - 1; index >= 0; index -= 1) {
    const value = points[index]?.[key];
    if (Number.isFinite(value)) {
      return { index, value };
    }
  }
  return null;
}

function apiUrl(path) {
  return `${basePath}${path}`;
}

function describeFocusSubtitle(focus) {
  const parts = [];
  if (focus.market?.question) {
    parts.push(focus.market.question);
  } else if (focus.market?.slug) {
    parts.push(shortMarketSlug(focus.market.slug, focus.coin));
  }
  if (Number.isFinite(focus.market?.reference_price)) {
    parts.push(`Strike ${formatUsd(focus.market.reference_price)}`);
  }
  if (focus.market?.reference_source) {
    parts.push(`ref ${focus.market.reference_source}`);
  }
  if (Number.isFinite(focus.market?.seconds_remaining)) {
    parts.push(`${formatDuration(focus.market.seconds_remaining)} remaining`);
  }
  return parts.filter(Boolean).join(" - ") || "Waiting for market context";
}

function describeMarketNote(coin) {
  const parts = [];
  if (Number.isFinite(coin.market?.reference_price)) {
    parts.push(`Ref ${formatUsd(coin.market.reference_price)}`);
  }
  if (Number.isFinite(coin.market?.seconds_remaining)) {
    parts.push(`${Math.round(coin.market.seconds_remaining)}s left`);
  }
  return parts.join(" - ") || "Awaiting live market";
}

function describeSparklineWindow(pointCount) {
  if (!pointCount) return "No history";
  const option = chartWindows.find((item) => item.id === state.chartWindow);
  return `${pointCount} pts - ${option?.label || "MAX"}`;
}

function describeBookDepth(depthUsd) {
  return Number.isFinite(depthUsd) ? `Depth ${formatUsd(depthUsd, { decimals: 0 })}` : "Depth --";
}

function calculateReferenceDrift(spot, reference) {
  if (!Number.isFinite(spot) || !Number.isFinite(reference) || reference === 0) return null;
  return {
    usd: spot - reference,
    pct: ((spot - reference) / reference) * 100,
  };
}

function formatReferenceDrift(drift) {
  if (!drift) return "--";
  return `${formatUsd(drift.usd, { signed: true })} / ${formatSignedPercent(drift.pct)}`;
}

function calculateMovePct(openPrice, closePrice) {
  if (!Number.isFinite(openPrice) || !Number.isFinite(closePrice) || openPrice === 0) return null;
  return ((closePrice - openPrice) / openPrice) * 100;
}

function calculateTrendPct(points) {
  const numeric = points.filter((point) => Number.isFinite(point.spot_price));
  if (numeric.length < 2) return null;
  const first = numeric[0].spot_price;
  const last = numeric[numeric.length - 1].spot_price;
  if (!Number.isFinite(first) || !Number.isFinite(last) || first === 0) return null;
  return ((last - first) / first) * 100;
}

function formatUsd(value, options = {}) {
  if (!Number.isFinite(value)) return "--";
  const signed = options.signed ? (value >= 0 ? "+" : "") : "";
  const decimals = Number.isFinite(options.decimals) ? options.decimals : Math.abs(value) >= 100 ? 0 : Math.abs(value) >= 1 ? 2 : 4;
  return `${signed}$${Number(value).toFixed(decimals)}`;
}

function formatProb(value) {
  if (!Number.isFinite(value)) return "--";
  return `${Math.round(value * 100)}c`;
}

function formatPct(value) {
  if (!Number.isFinite(value)) return "--";
  return `${Math.round(value)}%`;
}

function formatSignedPercent(value) {
  if (!Number.isFinite(value)) return "--";
  const sign = value >= 0 ? "+" : "";
  return `${sign}${Number(value).toFixed(2)}%`;
}

function formatBadgeValue(key, value) {
  return key === "spot_price" || key === "reference_price" ? formatUsd(value) : formatProb(value);
}

function toTrackWidth(value) {
  if (!Number.isFinite(value)) return 0;
  return Math.max(2, Math.min(100, value * 100));
}

function formatAgo(timestampMs) {
  const delta = Math.max(0, Date.now() - timestampMs);
  const seconds = Math.round(delta / 1000);
  return seconds <= 1 ? "just now" : `${seconds}s ago`;
}

function formatAgeFromIso(value) {
  const ts = parseTimestampMs(value);
  if (!Number.isFinite(ts)) return "--";
  return formatAgo(ts);
}

function formatTimestamp(value) {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function formatShortTimestamp(value) {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  return date.toLocaleString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
}

function formatChartTime(value) {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function formatDuration(seconds) {
  if (!Number.isFinite(seconds)) return "--";
  const whole = Math.max(0, Math.round(seconds));
  const minutes = Math.floor(whole / 60);
  const remainder = whole % 60;
  return minutes ? `${minutes}m ${String(remainder).padStart(2, "0")}s` : `${remainder}s`;
}

function parseTimestampMs(value) {
  if (!value) return null;
  const ts = new Date(value).getTime();
  return Number.isNaN(ts) ? null : ts;
}

function classForNumber(value) {
  if (!Number.isFinite(value)) return "";
  if (value > 0) return "positive";
  if (value < 0) return "negative";
  return "";
}

function labelForWindow(windowId) {
  return chartWindows.find((item) => item.id === windowId)?.label || "MAX";
}

function shortMarketSlug(slug, coin) {
  if (!slug) return "--";
  const prefix = `${coin}-updown-5m-`;
  return slug.startsWith(prefix) ? slug.slice(prefix.length) : slug;
}

function withCurrentPreset(base, current) {
  const values = new Set(base);
  if (Number.isFinite(current) && current > 0) values.add(Math.round(current));
  return Array.from(values).sort((a, b) => a - b);
}
