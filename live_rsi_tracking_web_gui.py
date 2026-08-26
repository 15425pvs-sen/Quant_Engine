#!/usr/bin/env python
from __future__ import annotations

import base64
import json
import os
import queue
import subprocess
import sys
import threading
import time
import webbrowser
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from flask import Flask, Response, jsonify, request, stream_with_context


BASE_DIR = Path(__file__).resolve().parent
TRACKER_SCRIPT = BASE_DIR / "live_rsi_tracking.py"
HOST = "127.0.0.1"
PORT = 8799


HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Live RSI Trading Console</title>
  <style>
    :root {
      --bg: #08111f;
      --bg-2: #101a2f;
      --panel: rgba(16, 25, 42, 0.82);
      --panel-strong: rgba(13, 21, 36, 0.94);
      --text: #edf3ff;
      --muted: #98a9ca;
      --muted-2: #6f7ea0;
      --accent: #6ea8ff;
      --accent-2: #8b7bff;
      --success: #2dd4bf;
      --danger: #fb7185;
      --warning: #fbbf24;
      --console: #07101c;
      --line: rgba(132, 152, 196, 0.18);
      --shadow: 0 24px 72px rgba(0, 0, 0, 0.35);
    }
    * { box-sizing: border-box; }
    *::selection { background: rgba(110, 168, 255, 0.28); }
    body {
      margin: 0;
      font-family: "Aptos", "Segoe UI Variable", "Segoe UI", system-ui, sans-serif;
      background: var(--bg);
      color: var(--text);
      min-height: 100vh;
      padding: 22px;
      position: relative;
      overflow-x: hidden;
    }
    body::before,
    body::after {
      content: "";
      position: fixed;
      inset: auto;
      pointer-events: none;
      filter: blur(60px);
      opacity: 0.55;
      z-index: 0;
      animation: floatGlow 16s ease-in-out infinite alternate;
    }
    body::before {
      width: 420px;
      height: 420px;
      top: -120px;
      right: -100px;
      background: radial-gradient(circle, rgba(110, 168, 255, 0.22) 0%, rgba(110, 168, 255, 0.02) 70%);
    }
    body::after {
      width: 320px;
      height: 320px;
      bottom: -100px;
      left: -60px;
      background: radial-gradient(circle, rgba(139, 123, 255, 0.18) 0%, rgba(139, 123, 255, 0.03) 70%);
    }
    .shell {
      position: relative;
      z-index: 1;
      max-width: 1440px;
      margin: 0 auto;
    }
    .hero {
      display: flex;
      justify-content: space-between;
      gap: 18px;
      align-items: flex-end;
      margin-bottom: 18px;
    }
    .hero h1 {
      margin: 0;
      font-size: clamp(34px, 4.5vw, 64px);
      line-height: 1.05;
      letter-spacing: -0.06em;
      font-weight: 900;
      position: relative;
      display: inline-flex;
      align-items: center;
      gap: 14px;
      background: linear-gradient(90deg, #ffffff 0%, #cfe2ff 22%, #6ea8ff 45%, #9a7bff 68%, #ffffff 100%);
      background-size: 220% 100%;
      -webkit-background-clip: text;
      background-clip: text;
      color: transparent;
      text-shadow: 0 0 28px rgba(110, 168, 255, 0.12);
      animation: wordShift 7s ease-in-out infinite alternate;
    }
    .hero h1::after {
      content: "";
      width: 14px;
      height: 14px;
      border-radius: 999px;
      background: linear-gradient(135deg, var(--success), var(--accent));
      box-shadow:
        0 0 0 6px rgba(45, 212, 191, 0.12),
        0 0 26px rgba(45, 212, 191, 0.38);
      animation: pulseDot 1.9s ease-in-out infinite;
    }
    .hero p {
      margin: 8px 0 0;
      color: var(--muted);
      max-width: 760px;
      line-height: 1.5;
    }
    .hero-meta {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .chip {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 9px 12px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(15, 24, 41, 0.7);
      color: var(--muted);
      font-size: 13px;
      box-shadow: 0 10px 20px rgba(0, 0, 0, 0.15);
      backdrop-filter: blur(10px);
    }
    .chip strong {
      color: var(--text);
      font-weight: 600;
    }
    .card {
      background: linear-gradient(180deg, rgba(22, 32, 52, 0.9), rgba(14, 22, 38, 0.88));
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 18px;
      margin-bottom: 14px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(12px);
    }
    .card-title {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 14px;
    }
    .card-title h2 {
      margin: 0;
      font-size: 15px;
      letter-spacing: 0.02em;
      text-transform: uppercase;
      color: var(--muted);
      font-weight: 700;
    }
    .card-title span {
      color: var(--muted-2);
      font-size: 12px;
    }
    .metrics {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 14px;
      margin-bottom: 14px;
    }
    .metric {
      position: relative;
      overflow: hidden;
      background: linear-gradient(180deg, rgba(22, 32, 52, 0.92), rgba(10, 16, 28, 0.92));
      border: 1px solid rgba(132, 152, 196, 0.18);
      border-radius: 18px;
      padding: 16px 16px 14px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(14px);
      min-height: 118px;
    }
    .metric::before {
      content: "";
      position: absolute;
      inset: 0;
      background: linear-gradient(135deg, rgba(110, 168, 255, 0.16), transparent 36%, rgba(139, 123, 255, 0.12));
      opacity: 0.65;
      pointer-events: none;
    }
    .metric::after {
      content: "";
      position: absolute;
      width: 120px;
      height: 120px;
      top: -50px;
      right: -50px;
      background: radial-gradient(circle, rgba(110, 168, 255, 0.22) 0%, rgba(110, 168, 255, 0) 70%);
      animation: drift 7s ease-in-out infinite alternate;
      pointer-events: none;
    }
    .metric .label {
      position: relative;
      z-index: 1;
      display: inline-flex;
      align-items: center;
      gap: 8px;
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    .metric .label i {
      width: 9px;
      height: 9px;
      border-radius: 999px;
      background: var(--accent);
      box-shadow: 0 0 0 5px rgba(110, 168, 255, 0.12);
    }
    .metric .value {
      position: relative;
      z-index: 1;
      margin-top: 12px;
      font-size: clamp(24px, 3vw, 34px);
      font-weight: 700;
      letter-spacing: -0.04em;
    }
    .metric .subtext {
      position: relative;
      z-index: 1;
      margin-top: 8px;
      color: var(--muted-2);
      font-size: 12px;
      line-height: 1.4;
    }
    .metric.green .label i { background: var(--success); box-shadow: 0 0 0 5px rgba(45, 212, 191, 0.12); }
    .metric.green .value { color: #d8fff6; }
    .metric.blue .label i { background: var(--accent); }
    .metric.blue .value { color: #dce8ff; }
    .metric.purple .label i { background: var(--accent-2); box-shadow: 0 0 0 5px rgba(139, 123, 255, 0.12); }
    .metric.purple .value { color: #ebe6ff; }
    .metric.gold .label i { background: var(--warning); box-shadow: 0 0 0 5px rgba(251, 191, 36, 0.12); }
    .metric.gold .value { color: #fff2bf; }
    @keyframes drift {
      from { transform: translate3d(0, 0, 0) scale(1); opacity: 0.45; }
      to { transform: translate3d(-14px, 12px, 0) scale(1.12); opacity: 0.8; }
    }
    @keyframes floatGlow {
      from { transform: translate3d(0, 0, 0) scale(1); }
      to { transform: translate3d(0, 18px, 0) scale(1.04); }
    }
    @keyframes wordShift {
      from { background-position: 0% 50%; filter: drop-shadow(0 0 12px rgba(110, 168, 255, 0.14)); }
      to { background-position: 100% 50%; filter: drop-shadow(0 0 18px rgba(139, 123, 255, 0.22)); }
    }
    @keyframes pulseDot {
      0%, 100% { transform: scale(0.92); opacity: 0.8; }
      50% { transform: scale(1.15); opacity: 1; }
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 14px 16px;
      align-items: center;
    }
    .field {
      display: grid;
      gap: 8px;
    }
    label {
      color: var(--muted);
      font-size: 12px;
      letter-spacing: 0.06em;
      text-transform: uppercase;
    }
    input[type="text"], input[type="number"] {
      width: 100%;
      padding: 11px 12px;
      border-radius: 12px;
      border: 1px solid rgba(132, 152, 196, 0.2);
      background: rgba(6, 12, 22, 0.72);
      color: var(--text);
      outline: none;
      transition: border-color 0.18s ease, transform 0.18s ease, box-shadow 0.18s ease;
    }
    input[type="text"]:focus, input[type="number"]:focus {
      border-color: rgba(110, 168, 255, 0.7);
      box-shadow: 0 0 0 4px rgba(110, 168, 255, 0.12);
      transform: translateY(-1px);
    }
    .checks {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 16px;
    }
    .checks label {
      color: var(--text);
      display: inline-flex;
      gap: 8px;
      align-items: center;
      padding: 9px 12px;
      border-radius: 999px;
      background: rgba(8, 14, 24, 0.72);
      border: 1px solid rgba(132, 152, 196, 0.16);
      text-transform: none;
      letter-spacing: 0;
      font-size: 13px;
    }
    .checks input {
      accent-color: var(--accent);
      transform: translateY(-0.5px);
    }
    .row {
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
    }
    button {
      border: none;
      border-radius: 12px;
      padding: 11px 14px;
      cursor: pointer;
      background: rgba(27, 39, 59, 0.92);
      color: var(--text);
      font-weight: 600;
      transition: transform 0.16s ease, box-shadow 0.16s ease, background 0.16s ease;
    }
    button:hover:not(:disabled) {
      transform: translateY(-1px);
      box-shadow: 0 14px 22px rgba(0, 0, 0, 0.18);
    }
    button.primary {
      background: linear-gradient(135deg, var(--accent), var(--accent-2));
      color: white;
    }
    button.ghost {
      background: rgba(9, 16, 28, 0.9);
      border: 1px solid rgba(132, 152, 196, 0.18);
    }
    button.danger {
      background: rgba(251, 113, 133, 0.13);
      border: 1px solid rgba(251, 113, 133, 0.25);
      color: #ffd8df;
    }
    button:disabled { opacity: 0.45; cursor: default; transform: none; box-shadow: none; }
    .status-wrap {
      margin-left: auto;
      display: flex;
      align-items: center;
      gap: 10px;
    }
    #status {
      color: var(--muted);
      display: inline-flex;
      align-items: center;
      gap: 10px;
      padding: 10px 14px;
      border: 1px solid rgba(132, 152, 196, 0.16);
      border-radius: 999px;
      background: rgba(8, 14, 24, 0.72);
    }
    #status::before {
      content: "";
      width: 10px;
      height: 10px;
      border-radius: 999px;
      background: var(--muted-2);
      box-shadow: 0 0 0 4px rgba(111, 126, 160, 0.12);
    }
    #status.running {
      color: #d8fff6;
      border-color: rgba(45, 212, 191, 0.28);
    }
    #status.running::before {
      background: var(--success);
      box-shadow: 0 0 0 4px rgba(45, 212, 191, 0.14);
    }
    #status.error {
      color: #ffe1e7;
      border-color: rgba(251, 113, 133, 0.24);
    }
    #status.error::before {
      background: var(--danger);
      box-shadow: 0 0 0 4px rgba(251, 113, 133, 0.14);
    }
    .console-shell {
      padding: 0;
      overflow: hidden;
    }
    .console-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 16px 18px;
      border-bottom: 1px solid rgba(132, 152, 196, 0.14);
      background: linear-gradient(180deg, rgba(8, 16, 28, 0.8), rgba(10, 16, 28, 0.55));
    }
    .console-head .title {
      display: flex;
      flex-direction: column;
      gap: 3px;
    }
    .console-head .title strong {
      font-size: 15px;
      letter-spacing: 0.02em;
    }
    .console-head .title span {
      color: var(--muted-2);
      font-size: 12px;
    }
    #console {
      height: min(64vh, 720px);
      overflow: auto;
      white-space: pre-wrap;
      font-family: "Cascadia Mono", "SFMono-Regular", Consolas, Menlo, monospace;
      font-size: 13px;
      line-height: 1.5;
      background: linear-gradient(180deg, rgba(4, 10, 18, 0.98), rgba(8, 14, 24, 0.98));
      padding: 16px 18px;
      color: #dce7ff;
      scrollbar-color: rgba(110, 168, 255, 0.4) rgba(255, 255, 255, 0.04);
    }
    #console::-webkit-scrollbar { width: 10px; }
    #console::-webkit-scrollbar-track { background: rgba(255, 255, 255, 0.04); }
    #console::-webkit-scrollbar-thumb {
      background: linear-gradient(180deg, rgba(110, 168, 255, 0.45), rgba(139, 123, 255, 0.45));
      border-radius: 999px;
    }
    .inputbar {
      margin-top: 0;
      display: flex;
      gap: 10px;
      padding: 16px 18px 18px;
      border-top: 1px solid rgba(132, 152, 196, 0.14);
      background: rgba(8, 13, 22, 0.72);
    }
    .inputbar input {
      flex: 1;
      background: rgba(6, 12, 22, 0.9);
    }
    @media (max-width: 1024px) {
      .grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
      .metrics {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
      .hero {
        flex-direction: column;
        align-items: flex-start;
      }
      .hero-meta {
        justify-content: flex-start;
      }
    }
    @media (max-width: 720px) {
      body { padding: 14px; }
      .grid { grid-template-columns: 1fr; }
      .metrics { grid-template-columns: 1fr; }
      .row { align-items: stretch; }
      .status-wrap { margin-left: 0; width: 100%; }
      #status { width: 100%; justify-content: center; }
      .inputbar { flex-direction: column; }
      .console-head { flex-direction: column; align-items: flex-start; }
      .card { padding: 14px; border-radius: 18px; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <div class="hero">
      <div>
        <h1>StockEdge</h1>
        <p>Local control center for <code>StockEdge</code> with live order control, console streaming, trading-session visibility, and realtime execution oversight.</p>
      </div>
      <div class="hero-meta">
        <div class="chip"><strong>Mode</strong> Live stream</div>
        <div class="chip"><strong>Host</strong> 127.0.0.1:8799</div>
        <div class="chip"><strong>Engine</strong> Upstox linked</div>
      </div>
    </div>

    <div class="card">
      <div class="card-title">
        <h2>Execution Settings</h2>
        <span>Signal generation and order execution parameters</span>
      </div>
      <div class="grid">
        <div class="field">
          <label>Interval (sec)</label>
          <input id="interval" type="number" value="30" min="1" />
        </div>
        <div class="field">
          <label>Symbols (space separated)</label>
          <input id="symbols" type="text" placeholder="RELIANCE TCS" />
        </div>
        <div class="field">
          <label>Buy RSI Protection</label>
          <input id="buyRsiProtection" type="text" value="1.0" />
        </div>
        <div class="field">
          <label>Min Profit %</label>
          <input id="minProfitPct" type="text" value="0.0" />
        </div>
      </div>

      <div class="checks">
        <label><input id="hybrid" type="checkbox" checked /> Hybrid</label>
        <label><input id="telegram" type="checkbox" checked /> Telegram</label>
        <label><input id="confirmOrder" type="checkbox" checked /> Confirm Order</label>
        <label><input id="dryRun" type="checkbox" /> Dry Run</label>
        <label><input id="upstoxLive" type="checkbox" checked /> Upstox Live</label>
        <label><input id="syncDailyData" type="checkbox" checked /> Auto Sync Daily Data</label>
        <label><input id="results" type="checkbox" /> Results Only</label>
      </div>
    </div>

    <div class="metrics">
      <div class="metric blue" id="metricRsi">
        <div class="label"><i></i> Live RSI</div>
        <div class="value" id="metricRsiValue">--</div>
        <div class="subtext" id="metricRsiNote">Waiting for a BUY/SELL line with RSI.</div>
      </div>
      <div class="metric green" id="metricLtp">
        <div class="label"><i></i> Live LTP</div>
        <div class="value" id="metricLtpValue">--</div>
        <div class="subtext" id="metricLtpNote">Last seen price from the stream.</div>
      </div>
      <div class="metric purple" id="metricPnl">
        <div class="label"><i></i> PnL</div>
        <div class="value" id="metricPnlValue">--</div>
        <div class="subtext" id="metricPnlNote">Closed SELLs will refresh this tile.</div>
      </div>
      <div class="metric gold" id="metricFunds">
        <div class="label"><i></i> Funds</div>
        <div class="value" id="metricFundsValue">--</div>
        <div class="subtext" id="metricFundsNote">Latest available Upstox funds.</div>
      </div>
    </div>

    <div class="card">
      <div class="card-title">
        <h2>Controls</h2>
        <span>Start, stop, and send operator responses</span>
      </div>
      <div class="row">
        <button id="startBtn" class="primary">Start</button>
        <button id="stopBtn" class="danger">Stop</button>
        <button id="clearBtn" class="ghost">Clear Log</button>
        <button id="sendYBtn" class="ghost">Send Y</button>
        <button id="sendNBtn" class="ghost">Send N</button>
        <div class="status-wrap">
          <span id="status">Status: Idle</span>
        </div>
      </div>
    </div>

    <div class="card console-shell">
      <div class="console-head">
        <div class="title">
          <strong>Live Output</strong>
          <span>Realtime engine logs, prompts, and order state</span>
        </div>
      </div>
      <div id="console"></div>
      <div class="inputbar">
        <input id="consoleInput" type="text" placeholder="Type Y/N or any console input, then Enter" />
        <button id="sendBtn" class="primary">Send</button>
      </div>
    </div>
  </div>

  <script src="/app.js?v=20260825_1"></script>
</body>
</html>
"""


APP_JS = r"""
    const consoleEl = document.getElementById('console');
    const statusEl = document.getElementById('status');
    const startBtn = document.getElementById('startBtn');
    const stopBtn = document.getElementById('stopBtn');
    const sendBtn = document.getElementById('sendBtn');
    const sendYBtn = document.getElementById('sendYBtn');
    const sendNBtn = document.getElementById('sendNBtn');
    const inputEl = document.getElementById('consoleInput');
    const metricRsiValue = document.getElementById('metricRsiValue');
    const metricRsiNote = document.getElementById('metricRsiNote');
    const metricLtpValue = document.getElementById('metricLtpValue');
    const metricLtpNote = document.getElementById('metricLtpNote');
    const metricPnlValue = document.getElementById('metricPnlValue');
    const metricPnlNote = document.getElementById('metricPnlNote');
    const metricFundsValue = document.getElementById('metricFundsValue');
    const metricFundsNote = document.getElementById('metricFundsNote');

    let eventSource = null;
    let audioCtx = null;
    let lastBeepAt = 0;
    let latestMetrics = {
      rsi: null,
      ltp: null,
      pnl: null,
      pnlPct: null,
      funds: null,
      context: '',
    };

    function payload() {
      return {
        interval: document.getElementById('interval').value,
        symbols: document.getElementById('symbols').value,
        buy_rsi_protection: document.getElementById('buyRsiProtection').value,
        min_profit_pct: document.getElementById('minProfitPct').value,
        hybrid: document.getElementById('hybrid').checked,
        telegram: document.getElementById('telegram').checked,
        confirmOrder: document.getElementById('confirmOrder').checked,
        dry_run: document.getElementById('dryRun').checked,
        upstox_live: document.getElementById('upstoxLive').checked,
        sync_daily_data: document.getElementById('syncDailyData').checked,
        results: document.getElementById('results').checked,
      };
    }

    function setRunningState(running) {
      startBtn.disabled = running;
      stopBtn.disabled = !running;
      sendBtn.disabled = !running;
      sendYBtn.disabled = !running;
      sendNBtn.disabled = !running;
      inputEl.disabled = !running;
    }

    function ensureAudio() {
      try {
        if (!audioCtx) {
          const Ctor = window.AudioContext || window.webkitAudioContext;
          if (!Ctor) return null;
          audioCtx = new Ctor();
        }
        if (audioCtx.state === 'suspended') {
          audioCtx.resume();
        }
        return audioCtx;
      } catch (err) {
        return null;
      }
    }

    function beep(kind) {
      const ctx = ensureAudio();
      if (!ctx) return;
      const now = Date.now();
      if (now - lastBeepAt < 450) return;
      lastBeepAt = now;

      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.type = kind === 'SELL' ? 'triangle' : 'sine';
      osc.frequency.value = kind === 'SELL' ? 740 : 880;
      gain.gain.setValueAtTime(0.0001, ctx.currentTime);
      gain.gain.exponentialRampToValueAtTime(0.07, ctx.currentTime + 0.015);
      gain.gain.exponentialRampToValueAtTime(0.0001, ctx.currentTime + 0.16);
      osc.connect(gain);
      gain.connect(ctx.destination);
      osc.start();
      osc.stop(ctx.currentTime + 0.18);
    }

    function formatMoney(value) {
      if (value === null || value === undefined || Number.isNaN(value)) return '--';
      return Number(value).toFixed(2);
    }

    function formatPrice(value) {
      if (value === null || value === undefined || Number.isNaN(value)) return '--';
      return Number(value).toFixed(2);
    }

    function formatRsi(value) {
      if (value === null || value === undefined || Number.isNaN(value)) return '--';
      return Number(value).toFixed(2);
    }

    function updateMetricTiles() {
      metricRsiValue.textContent = formatRsi(latestMetrics.rsi);
      metricLtpValue.textContent = formatPrice(latestMetrics.ltp);
      metricPnlValue.textContent = latestMetrics.pnl === null
        ? '--'
        : `${latestMetrics.pnl >= 0 ? '+' : ''}${formatMoney(latestMetrics.pnl)}${latestMetrics.pnlPct === null ? '' : ` (${latestMetrics.pnlPct >= 0 ? '+' : ''}${Number(latestMetrics.pnlPct).toFixed(2)}%)`}`;
      metricFundsValue.textContent = formatMoney(latestMetrics.funds);

      metricRsiNote.textContent = latestMetrics.context
        ? latestMetrics.context
        : 'Waiting for a BUY/SELL line with RSI.';
      metricLtpNote.textContent = latestMetrics.context
        ? `Captured from ${latestMetrics.context}`
        : 'Last seen price from the stream.';
      metricPnlNote.textContent = latestMetrics.pnl === null
        ? 'Closed SELLs will refresh this tile.'
        : 'Last closed trade PnL from the stream.';
      metricFundsNote.textContent = latestMetrics.funds === null
        ? 'Latest available Upstox funds.'
        : 'Updated from the latest funds check.';
    }

    function parseMetricsFromText(text) {
      if (!text) return;
      const lines = String(text).split(/\r?\n/);
      for (const rawLine of lines) {
        const line = rawLine.trim();
        if (!line) continue;

        const buySellMatch = line.match(/\b(BUY|SELL)\s+([A-Z0-9._-]+).*?\bRSI\s+(-?\d+(?:\.\d+)?).*?\bLTP\s+(-?\d+(?:\.\d+)?)/i);
        if (buySellMatch) {
          latestMetrics.context = `${buySellMatch[1].toUpperCase()} ${buySellMatch[2].toUpperCase()}`;
          latestMetrics.rsi = Number(buySellMatch[3]);
          latestMetrics.ltp = Number(buySellMatch[4]);
          beep(buySellMatch[1].toUpperCase());
        }

        const pnlMatch = line.match(/\bPnL\s+(-?\d+(?:\.\d+)?)\s+\((-?\d+(?:\.\d+)?)%\)/i);
        if (pnlMatch) {
          latestMetrics.pnl = Number(pnlMatch[1]);
          latestMetrics.pnlPct = Number(pnlMatch[2]);
        }

        const fundsMatch = line.match(/Available Upstox funds:\s*(-?\d+(?:\.\d+)?)/i);
        if (fundsMatch) {
          latestMetrics.funds = Number(fundsMatch[1]);
        }

        const confirmMatch = line.match(/\bConfirm\s+(BUY|SELL)\s+([A-Z0-9._-]+)/i);
        if (confirmMatch) {
          latestMetrics.context = `${confirmMatch[1].toUpperCase()} ${confirmMatch[2].toUpperCase()}`;
        }
      }
      updateMetricTiles();
    }

    function appendLog(text) {
      if (!text) return;
      parseMetricsFromText(text);
      consoleEl.textContent += text;
      consoleEl.scrollTop = consoleEl.scrollHeight;
    }

    function setStatus(text, state) {
      statusEl.textContent = text;
      statusEl.classList.remove('running', 'error');
      if (state === 'running') {
        statusEl.classList.add('running');
      } else if (state === 'error') {
        statusEl.classList.add('error');
      }
    }

    async function startProcess() {
      appendLog('\nSending START request...\n');
      setStatus('Status: Starting...', 'running');
      ensureAudio();
      try {
        const res = await fetch('/api/start', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify(payload())
        });
        const data = await res.json();
        if (!res.ok) {
          appendLog(`\nSTART failed: ${data.error || 'Unknown error'}\n`);
          setStatus('Status: Start failed', 'error');
          alert(`START failed: ${data.error || 'Unknown error'}`);
          return;
        }
        appendLog((data.message || 'Started') + "\n");
        setRunningState(true);
      } catch (err) {
        appendLog(`\nSTART request error: ${err}\n`);
        setStatus('Status: Start error', 'error');
        alert(`START request error: ${err}`);
      }
    }

    async function stopProcess() {
      try {
        const res = await fetch('/api/stop', {method: 'POST'});
        const data = await res.json();
        if (data.message) appendLog(data.message + "\n");
      } catch (err) {
        appendLog(`\nSTOP request error: ${err}\n`);
        setStatus('Status: Stop error', 'error');
        alert(`STOP request error: ${err}`);
      }
    }

    async function sendInput(value) {
      const text = ((value !== undefined && value !== null) ? value : inputEl.value).trim();
      if (!text) return;
      const res = await fetch('/api/send-input', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({text})
      });
      const data = await res.json();
      if (!res.ok) {
        appendLog(`\nINPUT failed: ${data.error || 'Failed to send input'}\n`);
        setStatus('Status: Input error', 'error');
        alert(`INPUT failed: ${data.error || 'Failed to send input'}`);
        return;
      }
      appendLog(`\n> ${text}\n`);
      inputEl.value = '';
    }

    function connectStream() {
      if (eventSource) {
        eventSource.close();
      }

      eventSource = new EventSource('/api/stream');
      eventSource.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          if (data.type === 'log' && data.text) {
            appendLog(data.text);
            return;
          }
          if (data.type === 'state') {
            setStatus(
              data.running
                ? `Status: Running (PID ${data.pid})`
                : 'Status: Idle',
              data.running ? 'running' : 'idle'
            );
            setRunningState(Boolean(data.running));
          }
        } catch (err) {
          appendLog(`\nSTREAM parse error: ${err}\n`);
        }
      };
      eventSource.onerror = () => {
        appendLog('\nSTREAM disconnected. Reconnecting...\n');
        setStatus('Status: Reconnecting...', 'error');
        setTimeout(connectStream, 1000);
      };
    }

    startBtn.addEventListener('click', startProcess);
    stopBtn.addEventListener('click', stopProcess);
    document.getElementById('clearBtn').addEventListener('click', () => {
      consoleEl.textContent = '';
    });
    sendBtn.addEventListener('click', () => sendInput());
    sendYBtn.addEventListener('click', () => sendInput('Y'));
    sendNBtn.addEventListener('click', () => sendInput('N'));
    inputEl.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        sendInput();
      }
    });

    setRunningState(false);
    appendLog('Web UI ready. Click START to launch live_rsi_tracking.py\n');
    setStatus('Status: Idle', 'idle');
    updateMetricTiles();
    connectStream();
"""


@dataclass
class ProcessState:
    process: subprocess.Popen[str] | None = None
    chunks: list[str] = None
    events: queue.Queue[dict[str, Any]] = None

    def __post_init__(self) -> None:
        if self.chunks is None:
            self.chunks = []
        if self.events is None:
            self.events = queue.Queue()


app = Flask(__name__)


@app.after_request
def disable_cache(response: Response) -> Response:
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response
state = ProcessState()
state_lock = threading.Lock()
logged_state_poll = False
logged_logs_poll = False


def _enqueue_event(event: dict[str, Any]) -> None:
    with state_lock:
        state.events.put(event)


def _append_log(text: str) -> None:
    if not text:
        return
    with state_lock:
        state.chunks.append(text)
        if len(state.chunks) > 50000:
            state.chunks = state.chunks[-30000:]
        state.events.put({"type": "log", "text": text})


def _publish_state() -> None:
    with state_lock:
        proc = state.process
        running = proc is not None and proc.poll() is None
        pid = proc.pid if running else None
        state.events.put({"type": "state", "running": running, "pid": pid})


def _is_running() -> bool:
    with state_lock:
        proc = state.process
    return proc is not None and proc.poll() is None


def _build_command(payload: dict[str, Any]) -> list[str]:
    if not TRACKER_SCRIPT.exists():
        raise FileNotFoundError(f"Script not found: {TRACKER_SCRIPT}")

    cmd = [sys.executable, "-u", str(TRACKER_SCRIPT)]

    interval_val = str(payload.get("interval", "")).strip()
    if interval_val:
        int(interval_val)
        cmd.extend(["--interval", interval_val])

    symbol_text = str(payload.get("symbols", "")).strip()
    if symbol_text:
        symbols = [part.strip().upper() for part in symbol_text.split() if part.strip()]
        if symbols:
            cmd.append("--symbols")
            cmd.extend(symbols)

    buy_protection = str(payload.get("buy_rsi_protection", "")).strip()
    if buy_protection:
        float(buy_protection)
        cmd.extend(["--buy-rsi-protection", buy_protection])

    min_profit = str(payload.get("min_profit_pct", "")).strip()
    if min_profit:
        float(min_profit)
        cmd.extend(["--min-profit-pct", min_profit])

    if payload.get("hybrid"):
        cmd.append("--hybrid")
    if payload.get("telegram"):
        cmd.append("--telegram")
    if payload.get("confirmOrder"):
        cmd.append("--confirmOrder")
    if payload.get("dry_run"):
        cmd.append("--dry-run")
    if payload.get("results"):
        cmd.append("--results")

    if payload.get("upstox_live", True):
        cmd.append("--upstox-live")
    else:
        cmd.append("--no-upstox-live")

    if not payload.get("sync_daily_data", True):
        cmd.append("--no-sync-daily-data")

    return cmd


def _stream_output_worker(proc: subprocess.Popen[str]) -> None:
    if proc.stdout is None:
        return

    while True:
        chunk = proc.stdout.read(1)
        if chunk == "":
            break
        _append_log(chunk)

    proc.wait()
    _append_log(f"\n\nProcess exited with code {proc.returncode}\n")

    with state_lock:
        if state.process is proc:
            state.process = None
    _publish_state()


@app.get("/")
def index() -> str:
    print("[web-gui] GET /", flush=True)
    return HTML


FAVICON_ICO = base64.b64decode(
    "AAABAAEAEBAAAAEAIABoBAAAFgAAACgAAAAQAAAAIAAAAAEABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"
    "AAAAAAAAAAD///8A////AP///wD///8A////AP///wD///8A////AP///wD///8A////AP///wD///8A\n"
    "////AP///wD///8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
)




@app.get("/app.js")
def app_js() -> Any:
    print("[web-gui] GET /app.js", flush=True)
    return Response(APP_JS, mimetype="application/javascript")

@app.get("/favicon.ico")
def favicon() -> Any:
    return Response(FAVICON_ICO, mimetype="image/x-icon")


@app.get("/api/state")
def api_state() -> Any:
    global logged_state_poll
    if not logged_state_poll:
        print("[web-gui] /api/state polling active", flush=True)
        logged_state_poll = True
    with state_lock:
        proc = state.process
        running = proc is not None and proc.poll() is None
        pid = proc.pid if running else None
    return jsonify({"running": running, "pid": pid})


@app.get("/api/logs")
def api_logs() -> Any:
    global logged_logs_poll
    if not logged_logs_poll:
        print("[web-gui] /api/logs polling active", flush=True)
        logged_logs_poll = True
    try:
        from_idx = int(request.args.get("from", "0"))
    except ValueError:
        from_idx = 0

    with state_lock:
        total = len(state.chunks)
        from_idx = max(0, min(from_idx, total))
        new_chunks = state.chunks[from_idx:]
        next_idx = total

    return jsonify({"chunks": new_chunks, "next": next_idx})


@app.get("/api/stream")
def api_stream() -> Any:
    print("[web-gui] GET /api/stream", flush=True)

    def generate():
        with state_lock:
            proc = state.process
            running = proc is not None and proc.poll() is None
            pid = proc.pid if running else None
            for chunk in state.chunks:
                yield f"data: {json.dumps({'type': 'log', 'text': chunk})}\n\n"
            yield f"data: {json.dumps({'type': 'state', 'running': running, 'pid': pid})}\n\n"

        while True:
            try:
                event = state.events.get(timeout=15)
                yield f"data: {json.dumps(event)}\n\n"
            except queue.Empty:
                yield ": keepalive\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/api/start")
def api_start() -> Any:
    print("[web-gui] /api/start called", flush=True)
    if _is_running():
        print("[web-gui] start ignored: process already running", flush=True)
        return jsonify({"error": "Process is already running."}), 409

    payload = request.get_json(silent=True) or {}

    try:
        cmd = _build_command(payload)
    except Exception as exc:
        print(f"[web-gui] invalid start payload: {exc}", flush=True)
        return jsonify({"error": f"Invalid input: {exc}"}), 400

    print(f"[web-gui] launching: {' '.join(cmd)}", flush=True)

    child_env = dict(os.environ)
    child_env["PYTHONUNBUFFERED"] = "1"

    creation_flags = 0
    if os.name == "nt":
        creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP

    proc = subprocess.Popen(
        cmd,
        cwd=str(BASE_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.PIPE,
        text=True,
        bufsize=0,
        env=child_env,
        creationflags=creation_flags,
    )

    with state_lock:
        state.process = proc
        state.chunks.append("\nStarting command:\n" + " ".join(cmd) + "\n\n")
    _publish_state()

    threading.Thread(target=_stream_output_worker, args=(proc,), daemon=True).start()
    print(f"[web-gui] child started pid={proc.pid}", flush=True)

    return jsonify({"message": f"Started (PID {proc.pid})"})


@app.post("/api/stop")
def api_stop() -> Any:
    with state_lock:
        proc = state.process

    if proc is None or proc.poll() is not None:
        return jsonify({"message": "No running process."})

    try:
        proc.terminate()
        return jsonify({"message": "Stopping process..."})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.post("/api/send-input")
def api_send_input() -> Any:
    payload = request.get_json(silent=True) or {}
    text = str(payload.get("text", "")).strip()
    if not text:
        return jsonify({"error": "Input text is required."}), 400

    with state_lock:
        proc = state.process

    if proc is None or proc.poll() is not None or proc.stdin is None:
        return jsonify({"error": "Process is not running."}), 409

    try:
        proc.stdin.write(text + "\n")
        proc.stdin.flush()
        return jsonify({"message": "Input sent."})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


def main() -> None:
    url = f"http://{HOST}:{PORT}"
    print(f"Open {url} in your browser")

    def _open_browser() -> None:
        try:
            webbrowser.open(url, new=2)
        except Exception:
            pass

    threading.Timer(0.8, _open_browser).start()
    app.run(host=HOST, port=PORT, debug=False, threaded=True)


if __name__ == "__main__":
    main()
