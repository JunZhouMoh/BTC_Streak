"""
Local web dashboard: FastAPI + single-page UI, JSON at /api/state.
Runs in a daemon thread; state is updated from the bot's main loop.

Market Resolution:
  Markets resolve UP if Chainlink BTC/USD TWAP (Time-Weighted Average Price)
  >= opening price for the time window, otherwise DOWN.
  
  Resolution source: https://data.chain.link/streams/btc-usd-twap-60s-streams
"""

from __future__ import annotations

import math
import logging
import socket
import threading
import time
from typing import Any, Callable, Dict, Optional

import uvicorn
from fastapi import Body, FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, Response

logger = logging.getLogger("web_dashboard")

_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>BTC Trading Dashboard</title>
  <style>
    :root {
      color-scheme: dark;
      --bg:#090e18; --surface:#111a2a; --surface-raised:#152136; --border:#233149;
      --text:#edf3ff; --muted:#8c9ab1; --green:#49d6a0; --red:#ff7185;
      --yellow:#f4c76b; --blue:#77aaff; --accent:#8b9cff;
    }
    * { box-sizing: border-box; }
    body { min-width:320px; margin:0; color:var(--text); background:radial-gradient(ellipse at 50% -25%,#1d2b49 0,transparent 58%),var(--bg); font:14px/1.5 ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif; }
    button,input,select { font:inherit; }
    button:focus-visible,input:focus-visible,select:focus-visible,a:focus-visible { outline:2px solid var(--blue); outline-offset:2px; }
    .app-shell { width:min(1440px,100%); margin:0 auto; padding:26px 30px 36px; }
    .topbar { display:flex; align-items:center; justify-content:space-between; gap:20px; padding:4px 0 20px; border-bottom:1px solid var(--border); }
    .brand { display:flex; align-items:center; gap:13px; min-width:0; }
    .brand-mark { display:grid; place-items:center; width:42px; height:42px; flex:none; border:1px solid #705722; border-radius:14px; color:#ffd27b; background:linear-gradient(145deg,#45351d,#211d19); font-size:23px; font-weight:750; }
    h1 { margin:0; font-size:19px; font-weight:680; letter-spacing:-.025em; }
    .meta { overflow:hidden; color:var(--muted); font-size:12px; text-overflow:ellipsis; white-space:nowrap; }
    .header-status { display:flex; flex:none; align-items:center; gap:8px; }
    .badge { display:inline-flex; align-items:center; gap:7px; padding:6px 10px; border:1px solid var(--border); border-radius:999px; color:var(--muted); background:#101827; font-size:11px; font-weight:650; letter-spacing:.04em; text-transform:uppercase; }
    .badge:before { width:7px; height:7px; border-radius:50%; background:currentColor; content:""; }
    .badge.connected,.badge.simulation { color:var(--green); border-color:#245442; background:#10251f; }
    .badge.disconnected,.badge.live { color:var(--yellow); border-color:#66532b; background:#292315; }
    .main-nav { display:flex; gap:6px; padding:16px 0 20px; }
    .main-nav a { padding:7px 11px; border:1px solid transparent; border-radius:8px; color:var(--muted); font-size:12px; font-weight:600; text-decoration:none; }
    .main-nav a:hover { color:var(--text); border-color:var(--border); background:var(--surface); }
    .section-heading { display:flex; align-items:end; justify-content:space-between; gap:16px; margin:4px 0 13px; }
    .section-heading h2 { margin:0; font-size:15px; font-weight:650; letter-spacing:-.01em; }
    .section-heading p { margin:3px 0 0; color:var(--muted); font-size:12px; }
    .grid { display:grid; grid-template-columns:repeat(12,minmax(0,1fr)); gap:14px; }
    .card { min-width:0; padding:18px; border:1px solid var(--border); border-radius:14px; background:linear-gradient(150deg,rgba(21,33,54,.96),rgba(15,23,37,.96)); box-shadow:0 12px 30px rgba(0,0,0,.12); }
    .card h2 { display:flex; align-items:center; gap:8px; margin:0 0 14px; color:#bdc9dc; font-size:11px; font-weight:700; letter-spacing:.09em; text-transform:uppercase; }
    .card h2:before { width:7px; height:7px; border-radius:2px; background:var(--accent); content:""; }
    .session-card { grid-column:span 7; }
    .strategy-card { grid-column:span 5; }
    .market-card { grid-column:span 6; }
    .btc-card,.trading-card { grid-column:span 6; }
    .manual-card { grid-column:1/-1; }
    .controls-card { grid-column:span 6; }
    .wide-card { grid-column:1/-1; }
    .mono { font-family:ui-monospace,SFMono-Regular,Consolas,monospace; font-size:12px; }
    .session-metrics { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:12px; }
    .metric { min-width:0; padding:11px 12px; border:1px solid rgba(140,154,177,.13); border-radius:10px; background:rgba(7,13,23,.4); }
    .metric span { display:block; margin-bottom:4px; color:var(--muted); font-size:10px; font-weight:650; letter-spacing:.07em; text-transform:uppercase; }
    .metric strong { display:block; overflow-wrap:anywhere; color:var(--text); font-size:13px; font-weight:620; }
    .sig { margin:-2px 0 12px; font-size:18px; font-weight:700; letter-spacing:-.02em; }
    .sig.wait { color:var(--yellow); }
    .sig.buy { color:var(--green); }
    .sig.block { color:var(--red); }
    .strategy-details { color:#b0bdd1; line-height:1.9; }
    .market-card.up-card { border-top:2px solid var(--green); }
    .market-card.down-card { border-top:2px solid var(--red); }
    .market-card.up-card h2:before { background:var(--green); }
    .market-card.down-card h2:before { background:var(--red); }
    .book-details { color:#b8c5d8; line-height:1.9; }
    .btc-card { border-top:2px solid var(--yellow); }
    .trading-card { border-top:2px solid var(--blue); }
    .btc-details,.trading-details { color:#bdc9dc; line-height:1.9; }
    .trade-warning { margin:-2px 0 15px; color:var(--muted); font-size:12px; }
    .trade-warning strong { color:var(--yellow); }
    .trade-entry { display:flex; flex-wrap:wrap; align-items:end; gap:12px; padding-bottom:16px; border-bottom:1px solid var(--border); }
    .trade-entry label { display:block; margin-bottom:5px; color:var(--muted); font-size:11px; font-weight:650; }
    .amount-control { display:flex; align-items:center; gap:7px; width:150px; padding:0 10px; border:1px solid var(--border); border-radius:8px; background:#0b1220; }
    .amount-control span { color:var(--muted); }
    .amount-control input { width:100%; min-width:0; padding:9px 0; border:0; outline:0; color:var(--text); background:transparent; }
    .amount-control:focus-within { border-color:var(--blue); }
    .preset-buttons,.trade-buttons { display:flex; flex-wrap:wrap; gap:7px; }
    .trade-groups { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:18px; padding-top:16px; }
    .trade-group-title { margin:0 0 9px; color:var(--muted); font-size:10px; font-weight:700; letter-spacing:.07em; text-transform:uppercase; }
    .btn { min-height:34px; padding:7px 11px; border:1px solid #5277d2; border-radius:8px; color:#fff; background:#365bb2; cursor:pointer; font-size:12px; font-weight:650; transition:filter .15s,transform .15s; }
    .btn:hover { filter:brightness(1.16); }
    .btn:active { transform:translateY(1px); }
    .btn.secondary { border-color:var(--border); color:#d5deed; background:#1b273a; }
    .btn.up-action { border-color:#286b55; color:#c5f9e3; background:#173d32; }
    .btn.down-action { border-color:#783c4c; color:#ffd6dc; background:#482633; }
    .btn.sell-action { border-color:#75494a; color:#ffced0; background:#382327; }
    .status { min-height:18px; color:var(--muted); font-size:12px; }
    .status:empty { display:none; }
    .section-block { margin-top:27px; scroll-margin-top:16px; }
    .section-block .section-heading { margin-bottom:13px; }
    .row { display:flex; justify-content:space-between; gap:8px; }
    .controls .row { align-items:center; margin-bottom:7px; }
    .controls label { min-width:105px; color:var(--muted); font-size:12px; }
    .controls input[type="number"],.controls input[type="text"],.controls input[type="date"],.controls input[type="time"],.controls select { max-width:100%; border:1px solid var(--border); border-radius:7px; color:var(--text); background:#0b1220; }
    .controls input[type="number"],.controls input[type="text"],.controls input[type="date"],.controls input[type="time"] { min-height:32px; padding:5px 8px; }
    .controls input[type="checkbox"],.controls input[type="radio"] { accent-color:var(--accent); }
    .mode-grid { display:grid; gap:9px; }
    .mode-box { padding:10px; border:1px solid var(--border); border-radius:9px; background:#0d1523; }
    .mode-title { margin-bottom:7px; color:var(--blue); font-size:12px; font-weight:650; }
    .controls .btn { margin:3px 0; }
    footer { display:flex; justify-content:space-between; gap:14px; margin-top:24px; padding-top:14px; border-top:1px solid var(--border); color:var(--muted); font-size:11px; }
    #err { color:var(--red); }
    @media (max-width:760px) {
      .app-shell { padding:18px 16px 28px; }
      .session-card,.strategy-card,.market-card,.btc-card,.trading-card,.controls-card { grid-column:1/-1; }
      .trade-groups { grid-template-columns:1fr; gap:14px; }
      .topbar { align-items:flex-start; }
      .header-status { flex-direction:column; align-items:flex-end; }
    }
    @media (max-width:480px) {
      .app-shell { padding:14px 12px 24px; }
      .topbar { gap:10px; }
      .brand { gap:9px; }
      .brand-mark { width:36px; height:36px; border-radius:11px; }
      h1 { font-size:16px; }
      .badge { padding:5px 7px; font-size:9px; }
      .main-nav { overflow:auto; }
      .main-nav a { flex:none; }
      .session-metrics { grid-template-columns:1fr; }
      .card { padding:15px; }
      footer { flex-direction:column; }
    }
  </style>
</head>
<body>
  <div class="app-shell">
    <header class="topbar">
      <div class="brand">
        <div class="brand-mark" aria-hidden="true">₿</div>
        <div><h1>BTC trading dashboard</h1><div class="meta" id="meta">Connecting to live market…</div></div>
      </div>
      <div class="header-status" aria-label="Bot status">
        <span class="badge disconnected" id="connectionBadge">Connecting</span>
        <span class="badge simulation" id="modeBadge">Simulation</span>
      </div>
    </header>
    <nav class="main-nav" aria-label="Dashboard sections">
      <a href="#overview">Overview</a><a href="#automation">Automation</a><a href="#analytics">Analytics</a>
    </nav>
    <main>
      <section id="overview" class="section-block">
        <div class="section-heading"><div><h2>Live overview</h2><p>Market conditions, strategy signal, and open exposure.</p></div></div>
        <div class="grid">
          <div class="card session-card"><h2>Market session</h2><div id="session" class="session-metrics"></div></div>
          <div class="card strategy-card"><h2>Strategy signal</h2><div id="strategy"></div></div>
          <div class="card market-card up-card"><h2>Up token</h2><div id="up" class="book-details mono">Waiting for market data…</div></div>
          <div class="card market-card down-card"><h2>Down token</h2><div id="down" class="book-details mono">Waiting for market data…</div></div>
          <div class="card btc-card"><h2>BTC / USD · Chainlink</h2><div id="btc" class="btc-details mono">Waiting for Chainlink…</div></div>
          <div class="card trading-card"><h2>Position &amp; activity</h2><div id="trading" class="trading-details mono"></div></div>
          <div class="card manual-card">
            <h2>Manual trading</h2>
            <p class="trade-warning"><strong>Order controls:</strong> live mode can place real orders. Check the mode badge and amount before submitting.</p>
            <div id="manualTrade"></div>
          </div>
        </div>
      </section>
      <section id="automation" class="section-block">
        <div class="section-heading"><div><h2>Automation</h2><p>Configure streak alerts and automated entry behavior.</p></div></div>
        <div class="grid">
          <div class="card controls controls-card"><h2>Telegram streak alerts</h2><div id="streakAlertPanel" class="mono">Loading…</div></div>
          <div class="card controls wide-card"><h2>Streak-based buy bot</h2><div id="streakReversalBotPanel" class="mono">Loading…</div></div>
          <div class="card controls wide-card"><h2>Dynamic streak selection</h2><div id="hourlyDynamicStreaks" class="mono">Loading…</div></div>
        </div>
      </section>
      <section id="analytics" class="section-block">
        <div class="section-heading"><div><h2>Streak analytics</h2><p>Historical streak endings and observed patterns.</p></div></div>
        <div class="grid"><div class="card wide-card"><h2>Streak end counts</h2><div id="streakEnds" class="mono">Loading…</div></div></div>
      </section>
    </main>
    <footer><span>Live data refreshes every second while this page is active.</span><span id="err" role="status" aria-live="polite"></span></footer>
  </div>
  <script>
    function esc(s){ if(s===null||s===undefined) return ''; var el=document.createElement('div'); el.textContent=String(s); return el.innerHTML; }
    function sigClass(t){ if(!t) return 'wait'; if(t.indexOf('BUY')>=0) return 'buy'; if(t.indexOf('NO ENTRY')>=0) return 'block'; return 'wait'; }
    function numFmt(n,d){ if(n===null||n===undefined||typeof n!=='number'||isNaN(n)) return '\u2014'; return n.toFixed(d); }
    function boolHtml(v){ if(v===true) return '\u2713'; if(v===false) return '\u2717'; return '\u2014'; }
    function readNum(id,f){ var el=document.getElementById(id); if(!el) return f; var v=parseFloat(el.value); return isNaN(v)?f:v; }
    function readInt(id,f){ var el=document.getElementById(id); if(!el) return f; var v=parseInt(el.value,10); return isNaN(v)?f:v; }
    function requestJson(method,url,payload,onDone){ var r=new XMLHttpRequest(); r.open(method,url,true); if(payload!==null&&payload!==undefined){ r.setRequestHeader('Content-Type','application/json'); } r.onreadystatechange=function(){ if(r.readyState!==4) return; if(r.status<200||r.status>=300) return; try{ var d=JSON.parse(r.responseText); if(onDone) onDone(d);}catch(e){} }; r.send(payload!==null&&payload!==undefined?JSON.stringify(payload):null); }

  var streakAlertCfg=null;
  var streakReversalBotCfg=null;
  var hourlyLookbackDraft=null;
    window.latestModePerfData={};
    var pollTimer=null;
    var pollInFlight=false;
    var pollSeq=0;
    var isMobile=/Android|iPhone|iPad|iPod|Mobile/i.test(navigator.userAgent||'');
    var POLL_MS_ACTIVE=1000;
    var POLL_MS_HIDDEN=5000;
    var lastHtml={meta:'',session:'',strategy:'',btc:'',trading:'',modePerf:'',streakEnds:''};

    function setHtmlIfChanged(id,htmlKey,html){
      var el=document.getElementById(id);
      if(!el) return;
      if(lastHtml[htmlKey]===html) return;
      lastHtml[htmlKey]=html;
      el.innerHTML=html;
    }

    function scheduleTick(ms){
      if(pollTimer){ clearTimeout(pollTimer); pollTimer=null; }
      pollTimer=setTimeout(tick, Math.max(250, ms||POLL_MS_ACTIVE));
    }

    function numFmtSigned(n,d){ if(n===null||n===undefined||typeof n!=='number'||isNaN(n)) return '\u2014'; var fixed=n.toFixed(d); if(n>0) return '+'+fixed; return fixed; }

    function renderStreakAlertPanel(){
      var box=document.getElementById('streakAlertPanel'); if(!box) return;
      var s=streakAlertCfg||{};
      var h=[];
      var runTxt='none';
      var cs=parseInt(s.current_streak)||0;
      var cd=String(s.current_direction||'').trim();
      if(cs>0&&cd){ runTxt=cs+'x '+cd; }
      h.push('<div class="row"><label>Enabled</label><input type="checkbox" id="sa_enabled" '+(s.enabled?'checked':'')+'/></div>');
      h.push('<div class="row"><label>Current Run</label><span id="sa_current_run">'+esc(runTxt)+'</span></div>');
      h.push('<div class="row"><label>Min Streak</label><input type="number" id="sa_min_streak" step="1" min="2" value="'+esc(s.min_streak!=null?s.min_streak:3)+'"/></div>');
      h.push('<div class="row"><label>Alert Each Extension</label><input type="checkbox" id="sa_notify_every_extension" '+(s.notify_every_extension?'checked':'')+'/></div>');
      h.push('<div class="row"><label>Alert On Startup</label><input type="checkbox" id="sa_notify_on_startup" '+(s.notify_on_startup?'checked':'')+'/></div>');
      h.push('<div class="row"><label>Use Timer Bot</label><input type="checkbox" id="sa_use_timer_bot" '+(s.use_timer_bot?'checked':'')+'/></div>');
      if(s.use_timer_bot&&!s.timer_bot_ready){ h.push('<div class="row"><label>Timer Bot</label><span>missing token/chat</span></div>'); }
      h.push('<div style="margin-top:0.5rem" class="row"><button class="btn" onclick="saveStreakAlertConfig()">Apply</button><button class="btn secondary" onclick="loadStreakAlertConfig()">Reload</button></div>');
      h.push('<div id="streakAlertStatus" class="status"></div>');
      box.innerHTML=h.join('<br/>');
    }

    function loadStreakAlertConfig(onDone){
      requestJson('GET','/api/streak-alert',null,function(cfg){
        streakAlertCfg=cfg||{};
        renderStreakAlertPanel();
        if(onDone) onDone(streakAlertCfg);
      });
    }

    function saveStreakAlertConfig(){
      var status=document.getElementById('streakAlertStatus'); if(status) status.textContent='Applying...';
      var s=streakAlertCfg||{};
      var payload={
        enabled:!!(document.getElementById('sa_enabled')&&document.getElementById('sa_enabled').checked),
        min_streak:readInt('sa_min_streak',s.min_streak||3),
        notify_every_extension:!!(document.getElementById('sa_notify_every_extension')&&document.getElementById('sa_notify_every_extension').checked),
        notify_on_startup:!!(document.getElementById('sa_notify_on_startup')&&document.getElementById('sa_notify_on_startup').checked),
        use_timer_bot:!!(document.getElementById('sa_use_timer_bot')&&document.getElementById('sa_use_timer_bot').checked)
      };
      requestJson('POST','/api/streak-alert',payload,function(resp){
        streakAlertCfg=resp||payload;
        if(status) status.textContent='Applied';
        renderStreakAlertPanel();
      });
    }

    function renderStreakReversalBotPanel(){
      var box=document.getElementById('streakReversalBotPanel'); if(!box) return;
      var sb=streakReversalBotCfg||{};
      var h=[];
      h.push('<div class="row"><label>Global Reverse Enabled</label><input type="checkbox" id="srb_enabled" '+(sb.enabled?'checked':'')+'/></div>');
      h.push('<div class="row"><label>Global Continue Enabled</label><input type="checkbox" id="srb_same_direction_enabled" '+(sb.same_direction_enabled?'checked':'')+'/></div>');
      var prebuy=sb.prebuy_next_session||{};
      h.push('<div style="border:1px solid #30363d;border-radius:6px;padding:0.5rem;margin:0.45rem 0;background:#0d1117"><div class="mode-title">Pre-buy Next Session</div>');
      h.push('<div class="row"><label>Enabled</label><input type="checkbox" id="srb_prebuy_enabled" '+(prebuy.enabled?'checked':'')+'/></div>');
      h.push('<div class="row"><label>Time Left (sec)</label><input type="number" id="srb_prebuy_time_left" min="1" max="300" step="1" value="'+esc(prebuy.time_left_sec!=null?prebuy.time_left_sec:20)+'"/></div>');
      h.push('<div class="row"><label>Confirm Price</label><input type="number" id="srb_prebuy_confirmation_price" min="0.5" max="1" step="0.001" value="'+esc(prebuy.confirmation_price!=null?prebuy.confirmation_price:0.99)+'"/></div>');
      h.push('<div style="font-size:0.72rem;color:#8b949e">If the current market direction reaches this price before the time limit, the bot projects the ending streak (+1 when direction continues) and sends an immediate next-window buy using that streak mode. Choose one direction (Reverse or Same) for the projected streak.</div></div>');
      h.push('<div style="color:#8b949e;font-size:0.75rem;margin:0.35rem 0">Set each streak length from 1–10. Per-mode direction selects which enabled global strategy can trade.</div>');
      h.push('<div class="row"><label>Current Streak</label><span id="srb_current_streak">—</span></div>');
      h.push('<div style="margin-top:0.5rem"><strong style="color:#58a6ff;font-size:0.85rem">Modes (Streak Length → Buy Amount → Max Triggers):</strong></div>');
      
      var modes=sb.modes||[];
      for(var mi=0;mi<10;mi++){
        var sl=mi+1;
        var mode=null;
        for(var msi=0;msi<modes.length;msi++){
          if(parseInt((modes[msi]||{}).streak_length,10)===sl){ mode=modes[msi]; break; }
        }
        mode=mode||{};
        var ba=mode.buy_amount_usd!=null?mode.buy_amount_usd:(5+mi*5);
        var mt=mode.max_triggers!=null?mode.max_triggers:1;
        var direction=mode.direction||'global';
        var pairs=mode.time_left_price_pairs||[];
        
        h.push('<div style="border:1px solid #30363d;border-radius:6px;padding:0.5rem;margin-top:0.35rem;background:#0d1117">');
        h.push('<div class="mode-title">Streak '+sl+'</div>');
        h.push('<div class="row"><label style="min-width:120px">Direction</label><select id="srb_mode_'+mi+'_direction" style="background:#161b22;border:1px solid #30363d;color:#e6edf3;border-radius:6px;padding:0.25rem 0.35rem"><option value="global"'+(direction==='global'?' selected':'')+'>Global</option><option value="reverse"'+(direction==='reverse'?' selected':'')+'>Reverse</option><option value="same"'+(direction==='same'?' selected':'')+'>Same direction</option><option value="both"'+(direction==='both'?' selected':'')+'>Both</option><option value="off"'+(direction==='off'?' selected':'')+'>Off</option></select></div>');
        h.push('<div class="row"><label style="min-width:120px">Buy Amount ($)</label><input type="number" id="srb_mode_'+mi+'_buy_amount_usd" step="0.1" min="0.1" value="'+esc(ba)+'" style="width:60px;background:#161b22;border:1px solid #30363d;color:#e6edf3;border-radius:6px;padding:0.25rem 0.35rem;"/></div>');
        h.push('<div class="row"><label style="min-width:120px">Max Triggers</label><input type="number" id="srb_mode_'+mi+'_max_triggers" step="1" min="1" value="'+esc(mt)+'" style="width:60px;background:#161b22;border:1px solid #30363d;color:#e6edf3;border-radius:6px;padding:0.25rem 0.35rem;" title="Max times this mode can trigger per market window"/></div>');
        h.push('<div style="font-size:0.75rem;color:#8b949e;margin-top:0.35rem">Time Left Thresholds (sec):</div>');
        
        for(var pi=0;pi<Math.max(pairs.length,2);pi++){
          var pair=pairs[pi]||{};
          var t=pair.time_left_sec!=null?pair.time_left_sec:(300-pi*150);
          var p=pair.buy_price!=null?pair.buy_price:0.4;
          var en=pair.enabled!==false;
          h.push('<div style="margin-top:0.25rem;display:flex;gap:0.35rem;align-items:center"><input type="checkbox" id="srb_mode_'+mi+'_pair_'+pi+'_enabled" '+(en?'checked':'')+' style="transform:scale(0.9)"/> <input type="number" id="srb_mode_'+mi+'_pair_'+pi+'_time_left" step="1" min="0" value="'+esc(t)+'" placeholder="time" style="width:70px;background:#161b22;border:1px solid #30363d;color:#e6edf3;border-radius:4px;padding:0.2rem;font-size:0.75rem"/> <span style="color:#8b949e">→</span> <input type="number" id="srb_mode_'+mi+'_pair_'+pi+'_buy_price" step="0.01" min="0" max="1" value="'+esc(p)+'" placeholder="price" style="width:60px;background:#161b22;border:1px solid #30363d;color:#e6edf3;border-radius:4px;padding:0.2rem;font-size:0.75rem"/></div>');
        }
        h.push('</div>');
      }

      h.push('<div style="margin-top:0.75rem" class="row"><button class="btn" onclick="saveStreakReversalBotConfig()">Apply</button><button class="btn secondary" onclick="loadStreakReversalBotConfig()">Reload</button></div>');
      h.push('<div id="streakReversalBotStatus" class="status"></div>');
      box.innerHTML=h.join('');
    }

    function loadStreakReversalBotConfig(onDone){
      requestJson('GET','/api/streak-reversal-bot',null,function(cfg){
        streakReversalBotCfg=cfg||{};
        renderStreakReversalBotPanel();
        renderHourlyDynamicStreaks(window.latestHourlyDynamicStats||[]);
        if(onDone) onDone(streakReversalBotCfg);
      });
    }

    function saveStreakReversalBotConfig(){
      var status=document.getElementById('streakReversalBotStatus'); if(status) status.textContent='Applying...';
      var sb=streakReversalBotCfg||{};
      var modes=[];
      for(var mi=0;mi<10;mi++){
        var dirEl=document.getElementById('srb_mode_'+mi+'_direction');
        var baEl=document.getElementById('srb_mode_'+mi+'_buy_amount_usd');
        var mtEl=document.getElementById('srb_mode_'+mi+'_max_triggers');
        if(dirEl&&baEl&&mtEl){
          var sl=mi+1;
          var ba=parseFloat(baEl.value);
          var mt=parseInt(mtEl.value,10);
          if(!isNaN(sl)&&!isNaN(ba)&&!isNaN(mt)&&sl>0&&ba>0&&mt>0){
            var pairs=[];
            for(var pi=0;pi<2;pi++){
              var enEl=document.getElementById('srb_mode_'+mi+'_pair_'+pi+'_enabled');
              var tEl=document.getElementById('srb_mode_'+mi+'_pair_'+pi+'_time_left');
              var pEl=document.getElementById('srb_mode_'+mi+'_pair_'+pi+'_buy_price');
              if(tEl&&pEl){
                var t=parseInt(tEl.value,10);
                var p=parseFloat(pEl.value);
                var en=enEl?!!enEl.checked:true;
                if(!isNaN(t)&&!isNaN(p)&&t>=0&&p>=0&&p<=1){
                  pairs.push({time_left_sec:t,buy_price:p,enabled:en});
                }
              }
            }
            modes.push({streak_length:sl,buy_amount_usd:ba,max_triggers:mt,direction:dirEl?dirEl.value:'global',time_left_price_pairs:pairs});
          }
        }
      }
      var payload={
        enabled:!!(document.getElementById('srb_enabled')&&document.getElementById('srb_enabled').checked),
        same_direction_enabled:!!(document.getElementById('srb_same_direction_enabled')&&document.getElementById('srb_same_direction_enabled').checked),
        prebuy_next_session:{
          enabled:!!(document.getElementById('srb_prebuy_enabled')&&document.getElementById('srb_prebuy_enabled').checked),
          time_left_sec:readInt('srb_prebuy_time_left',20),
          confirmation_price:readNum('srb_prebuy_confirmation_price',0.99)
        },
        modes:modes
      };
      requestJson('POST','/api/streak-reversal-bot',payload,function(resp){
        streakReversalBotCfg=resp||payload;
        if(status) status.textContent='Applied';
        renderStreakReversalBotPanel();
      });
    }

    function renderHourlyDynamicStreaks(hourlyStats){
      var box=document.getElementById('hourlyDynamicStreaks'); if(!box) return;
      var dm=(streakReversalBotCfg&&streakReversalBotCfg.dynamic_mode)||{};
      var selectedHours=hourlyLookbackDraft!=null?hourlyLookbackDraft:(parseInt(dm.lookback_hours,10)||6);
      var dynamicAmounts=Array.isArray(dm.buy_amounts_usd)?dm.buy_amounts_usd:[30,50,70];
      var dynamicPairs=Array.isArray(dm.time_left_price_pairs)?dm.time_left_price_pairs:[];
      var lines=['<div style="color:#8b949e;margin-bottom:0.45rem">Choose the trailing data window used to rank streak lengths. This selection is used by dynamic trading.</div>'];
      lines.push('<div class="row"><label style="min-width:120px">Enable Dynamic</label><input type="checkbox" id="srb_dynamic_enabled" '+(dm.enabled?'checked':'')+'/></div>');
      lines.push('<div class="row"><label style="min-width:120px">Choose Top X</label><input type="number" id="srb_dynamic_top_x" step="1" min="1" value="'+esc(dm.top_x!=null?dm.top_x:3)+'" oninput="updateHourlyDynamicStats(window.latestHourlyDynamicStats||[])" style="width:70px;background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:6px;padding:0.25rem 0.35rem"/></div>');
      lines.push('<div class="row"><label style="min-width:120px">Min End Chance (%)</label><input type="number" id="srb_dynamic_min_probability_pct" step="0.1" min="0" max="100" value="'+esc(dm.min_probability_pct!=null?dm.min_probability_pct:0)+'" oninput="updateHourlyDynamicStats(window.latestHourlyDynamicStats||[])" style="width:70px;background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:6px;padding:0.25rem 0.35rem" title="A selected streak length qualifies only when its conditional end probability is strictly greater than this percentage"/></div>');
      lines.push('<div class="row"><label style="min-width:120px">Buy Amounts ($)</label><input type="text" id="srb_dynamic_amounts" value="'+esc(dynamicAmounts.join(', '))+'" placeholder="30, 50, 70" style="width:150px;background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:6px;padding:0.25rem 0.35rem"/></div>');
      lines.push('<div style="font-size:0.72rem;color:#8b949e;margin:0.25rem 0 0.35rem">Enter at least Top X amounts. Amounts map to selected streak lengths in ascending order.</div>');
      lines.push('<div style="font-size:0.75rem;color:#8b949e;margin-top:0.4rem">Dynamic time-left thresholds (seconds → max buy price):</div>');
      for(var dpi=0;dpi<2;dpi++){
        var dp=dynamicPairs[dpi]||{};
        var dt=dp.time_left_sec!=null?dp.time_left_sec:(300-dpi*150);
        var dprice=dp.buy_price!=null?dp.buy_price:(dpi===0?0.45:0.50);
        lines.push('<div style="margin-top:0.25rem;display:flex;gap:0.35rem;align-items:center"><input type="checkbox" id="srb_dynamic_pair_'+dpi+'_enabled" '+(dp.enabled!==false?'checked':'')+' style="transform:scale(0.9)"/> <input type="number" id="srb_dynamic_pair_'+dpi+'_time_left" step="1" min="0" value="'+esc(dt)+'" placeholder="time" style="width:70px;background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:4px;padding:0.2rem;font-size:0.75rem"/> <span style="color:#8b949e">→</span> <input type="number" id="srb_dynamic_pair_'+dpi+'_buy_price" step="0.01" min="0" max="1" value="'+esc(dprice)+'" placeholder="price" style="width:60px;background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:4px;padding:0.2rem;font-size:0.75rem"/></div>');
      }
      lines.push('<div id="hourlyDynamicRows" style="margin-top:0.5rem"></div>');
      lines.push('<div style="margin-top:0.55rem"><button class="btn" onclick="saveHourlyDynamicConfig()">Apply Dynamic Selection</button> <span id="hourlyDynamicStatus" class="status"></span></div>');
      box.innerHTML=lines.join('<br/>');
      updateHourlyDynamicStats(hourlyStats);
    }

    function updateHourlyDynamicStats(hourlyStats){
      window.latestHourlyDynamicStats=hourlyStats||[];
      var rowsEl=document.getElementById('hourlyDynamicRows'); if(!rowsEl) return;
      var dm=(streakReversalBotCfg&&streakReversalBotCfg.dynamic_mode)||{};
      var selectedHours=hourlyLookbackDraft!=null?hourlyLookbackDraft:(parseInt(dm.lookback_hours,10)||6);
      var topX=readInt('srb_dynamic_top_x',parseInt(dm.top_x,10)||3);
      var minChance=readNum('srb_dynamic_min_probability_pct',Number(dm.min_probability_pct)||0);
      var statsByHour={};
      (hourlyStats||[]).forEach(function(row){statsByHour[Number(row.hours)]=row;});
      var lines=[];
      for(var hours=1;hours<=6;hours++){
        var row=statsByHour[hours]||{};
        var probabilities=Array.isArray(row.probabilities)?row.probabilities:[];
        var selected=probabilities.slice().sort(function(a,b){
          return (Number(b.pct_end_here)||0)-(Number(a.pct_end_here)||0)||((Number(a.length)||0)-(Number(b.length)||0));
        }).slice(0,Math.max(1,topX)).filter(function(item){return Number(item.pct_end_here)>minChance;}).sort(function(a,b){return Number(a.length)-Number(b.length);});
        var summary=selected.length?selected.map(function(item){return esc(item.length)+'x ('+numFmt(Number(item.pct_end_here),1)+'%)';}).join(', '):'No qualifying samples';
        lines.push('<label style="display:flex;justify-content:space-between;gap:0.5rem;align-items:flex-start;margin:0.3rem 0;cursor:pointer"><span><input type="radio" name="hourly_dynamic_hours" value="'+hours+'" '+(selectedHours===hours?'checked':'')+' onchange="hourlyLookbackDraft='+hours+';updateHourlyDynamicStats(window.latestHourlyDynamicStats||[])"/> '+hours+' hour'+(hours===1?'':'s')+' <span style="color:#8b949e">('+esc(row.sample_count||0)+' ends)</span></span><span style="text-align:right">'+summary+'</span></label>');
      }
      lines.push('<div style="font-size:0.75rem;color:#8b949e;margin-top:0.35rem">Selected duration: '+selectedHours+' hour'+(selectedHours===1?'':'s')+'. Apply to make this the active dynamic-trading window.</div>');
      rowsEl.innerHTML=lines.join('');
    }

    function saveHourlyDynamicConfig(){
      var status=document.getElementById('hourlyDynamicStatus');
      var dm=(streakReversalBotCfg&&streakReversalBotCfg.dynamic_mode)||{};
      var selected=document.querySelector('input[name="hourly_dynamic_hours"]:checked');
      var hours=selected?parseInt(selected.value,10):(hourlyLookbackDraft||parseInt(dm.lookback_hours,10)||6);
      var topX=readInt('srb_dynamic_top_x',parseInt(dm.top_x,10)||3);
      var minProbability=readNum('srb_dynamic_min_probability_pct',Number(dm.min_probability_pct)||0);
      var amountsRaw=(document.getElementById('srb_dynamic_amounts')||{}).value||'';
      var amounts=amountsRaw.split(/[\\s,]+/).map(function(value){return parseFloat(value.trim());}).filter(function(value){return !isNaN(value)&&value>0;});
      if(minProbability<0||minProbability>100){ if(status) status.textContent='Minimum end chance must be between 0 and 100.'; return; }
      if(topX<1||amounts.length<topX){ if(status) status.textContent='Enter at least one positive buy amount for each Top X selection.'; return; }
      var pairs=[];
      for(var i=0;i<2;i++){
        var timeEl=document.getElementById('srb_dynamic_pair_'+i+'_time_left');
        var priceEl=document.getElementById('srb_dynamic_pair_'+i+'_buy_price');
        if(timeEl&&priceEl){
          var threshold=parseInt(timeEl.value,10), price=parseFloat(priceEl.value);
          var enabledEl=document.getElementById('srb_dynamic_pair_'+i+'_enabled');
          if(!isNaN(threshold)&&!isNaN(price)&&threshold>=0&&price>=0&&price<=1){
            pairs.push({time_left_sec:threshold,buy_price:price,enabled:enabledEl?!!enabledEl.checked:true});
          }
        }
      }
      if(status) status.textContent='Applying...';
      var payload={
        dynamic_mode:{
          enabled:!!(document.getElementById('srb_dynamic_enabled')&&document.getElementById('srb_dynamic_enabled').checked),
          top_x:topX,
          min_probability_pct:minProbability,
          lookback_hours:Math.max(1,Math.min(6,hours)),
          buy_amounts_usd:amounts,
          time_left_price_pairs:pairs
        }
      };
      requestJson('POST','/api/streak-reversal-bot',payload,function(resp){
        streakReversalBotCfg=resp||streakReversalBotCfg;
        hourlyLookbackDraft=null;
        if(status) status.textContent='Applied: trailing '+hours+' hour'+(hours===1?'':'s');
        updateHourlyDynamicStats(window.latestHourlyDynamicStats||[]);
      });
    }

    function manualBuyWithDirection(direction){
      var status=document.getElementById('buyStatus');
      var amount=readNum('buyAmount',0);
      if(!(amount>0)){
        if(status) status.textContent='Amount must be > 0';
        return;
      }
      if(status) status.textContent='Submitting...';
      requestJson('POST','/api/manual-buy',{amount_usd:amount,direction:direction},function(resp){
        if(!status) return;
        if(resp&&resp.ok===false){
          status.textContent=String(resp.error||'Request failed');
          return;
        }
        status.textContent=String((resp&&resp.message)||('Queued '+direction));
      });
    }

    function manualBuyUp(){ manualBuyWithDirection('UP'); }
    function manualBuyDown(){ manualBuyWithDirection('DOWN'); }

    function manualBuyNextWithDirection(direction){
      var status=document.getElementById('buyStatus');
      var amount=readNum('buyAmount',0);
      if(!(amount>0)){
        if(status) status.textContent='Amount must be > 0';
        return;
      }
      if(status) status.textContent='Queueing next window...';
      requestJson('POST','/api/manual-buy-next',{amount_usd:amount,direction:direction},function(resp){
        if(!status) return;
        if(resp&&resp.ok===false){
          status.textContent=String(resp.error||'Request failed');
          return;
        }
        status.textContent=String((resp&&resp.message)||('Queued next '+direction));
      });
    }

    function manualBuyNextUp(){ manualBuyNextWithDirection('UP'); }
    function manualBuyNextDown(){ manualBuyNextWithDirection('DOWN'); }

    function manualBuyNextNowWithDirection(direction){
      var status=document.getElementById('buyStatus');
      var amount=readNum('buyAmount',0);
      if(!(amount>0)){
        if(status) status.textContent='Amount must be > 0';
        return;
      }
      if(status) status.textContent='Buying next window NOW...';
      requestJson('POST','/api/manual-buy-next-now',{amount_usd:amount,direction:direction},function(resp){
        if(!status) return;
        if(resp&&resp.ok===false){
          status.textContent=String(resp.error||'Request failed');
          return;
        }
        status.textContent=String((resp&&resp.message)||('Bought next '+direction+' NOW'));
      });
    }

    function manualBuyNextNowUp(){ manualBuyNextNowWithDirection('UP'); }
    function manualBuyNextNowDown(){ manualBuyNextNowWithDirection('DOWN'); }

    function manualSell(){
      var status=document.getElementById('buyStatus');
      if(status) status.textContent='Submitting sell...';
      requestJson('POST','/api/manual-sell',{},function(resp){
        if(!status) return;
        if(resp&&resp.ok===false){
          status.textContent=String(resp.error||'Sell request failed');
          scheduleTick(50);
          return;
        }
        status.textContent=String((resp&&resp.message)||'Queued manual sell');
        scheduleTick(50);
      });
    }

    function setBuyAmount(v){
      var el=document.getElementById('buyAmount');
      if(!el) return;
      el.value=String(v);
    }

    function applyControls(){
      var payload={ momentum:!!document.getElementById('ctl-momentum').checked, vwap_deviation:!!document.getElementById('ctl-vwap').checked, zscore:!!document.getElementById('ctl-zscore').checked };
      requestJson('POST','/api/indicator-controls',payload,function(){});
    }

    function tick(){
      if(pollInFlight) return;
      pollInFlight=true;
      var seq=++pollSeq;
      var startedAt=Date.now();
      var errEl=document.getElementById('err');
      var r=new XMLHttpRequest();
      r.open('GET','/api/state',true);
      r.onreadystatechange=function(){
        if(r.readyState!==4) return;
        if(seq!==pollSeq) return;
        pollInFlight=false;
        try{
          if(r.status!==200) throw new Error('HTTP '+r.status);
          var d=JSON.parse(r.responseText);
          errEl.textContent='';
          var hdr=d.header||{};
          var slug=hdr.slug!=null?String(hdr.slug):'\u2014';
          var ts='';
          if(d.ts) ts=new Date(d.ts*1000).toISOString();
          var metaHtml=esc(slug)+' \u00b7 Updated '+esc(ts);
          setHtmlIfChanged('meta','meta',metaHtml);
          var connectionBadge=document.getElementById('connectionBadge');
          if(connectionBadge){
            connectionBadge.textContent=hdr.ws_connected?'Market feed live':'Market feed offline';
            connectionBadge.className='badge '+(hdr.ws_connected?'connected':'disconnected');
          }
          var modeBadge=document.getElementById('modeBadge');
          if(modeBadge){
            modeBadge.textContent=hdr.simulation?'Simulation':'Live trading';
            modeBadge.className='badge '+(hdr.simulation?'simulation':'live');
          }
          var existingAmountEl=document.getElementById('buyAmount');
          var buyAmountVal=existingAmountEl?existingAmountEl.value:'';
          var existingBuyStatusEl=document.getElementById('buyStatus');
          var buyStatusVal=existingBuyStatusEl?existingBuyStatusEl.textContent:'';
          var liveStatusVal=d.manual_buy_live_status?String(d.manual_buy_live_status):'idle';
          var nextBuy=d.manual_buy_next||{};
          var nextPendingText='none';
          if(nextBuy&&nextBuy.pending&&nextBuy.signal){
            var nbAmt=(nextBuy.amount_usd!=null&&typeof nextBuy.amount_usd==='number'&&!isNaN(nextBuy.amount_usd))?numFmt(nextBuy.amount_usd,2):'\u2014';
            nextPendingText=String(nextBuy.signal)+' $'+String(nbAmt);
          }
          var defaultBuyAmount=10;
          if(!buyAmountVal) buyAmountVal=String(defaultBuyAmount);
          var upPrice=((d.up&&d.up.book&&typeof d.up.book.last_price==='number')?numFmt(d.up.book.last_price,3):'\u2014');
          var downPrice=((d.down&&d.down.book&&typeof d.down.book.last_price==='number')?numFmt(d.down.book.last_price,3):'\u2014');
          var nextUpPrice=((d.next_up&&d.next_up.book&&typeof d.next_up.book.last_price==='number')?numFmt(d.next_up.book.last_price,3):'\u2014');
          var nextDownPrice=((d.next_down&&d.next_down.book&&typeof d.next_down.book.last_price==='number')?numFmt(d.next_down.book.last_price,3):'\u2014');
          var sessionHtml=[
            '<div class="metric"><span>Time remaining</span><strong>'+(hdr.time_left_sec!=null?esc(Math.floor(hdr.time_left_sec)+' seconds'):'\u2014')+'</strong></div>',
            '<div class="metric"><span>Market</span><strong>'+esc(slug)+'</strong></div>',
            '<div class="metric"><span>Current odds · UP / DOWN</span><strong>$'+esc(upPrice)+' / $'+esc(downPrice)+'</strong></div>',
            '<div class="metric"><span>Next window · UP / DOWN</span><strong>$'+esc(nextUpPrice)+' / $'+esc(nextDownPrice)+'</strong></div>',
            '<div class="metric"><span>Next queued order</span><strong>'+esc(nextPendingText)+'</strong></div>',
            '<div class="metric"><span>Latest order status</span><strong>'+esc(liveStatusVal)+'</strong></div>'
          ].join('');
          setHtmlIfChanged('session','session',sessionHtml);
          var manualHtml=[
            '<div class="trade-entry"><div><label for="buyAmount">Order amount</label><div class="amount-control"><span>$</span><input type="number" id="buyAmount" min="0.1" step="0.1" value="'+esc(buyAmountVal)+'"/></div></div>',
            '<div><label>Quick amounts</label><div class="preset-buttons"><button class="btn secondary" onclick="setBuyAmount(39)">$39</button><button class="btn secondary" onclick="setBuyAmount(49)">$49</button><button class="btn secondary" onclick="setBuyAmount(99)">$99</button></div></div></div>',
            '<div class="trade-groups"><div><div class="trade-group-title">Current window</div><div class="trade-buttons"><button class="btn up-action" onclick="manualBuyUp()">Buy UP</button><button class="btn down-action" onclick="manualBuyDown()">Buy DOWN</button></div></div>',
            '<div><div class="trade-group-title">Queue for next window</div><div class="trade-buttons"><button class="btn secondary" onclick="manualBuyNextUp()">Queue UP</button><button class="btn secondary" onclick="manualBuyNextDown()">Queue DOWN</button></div></div>',
            '<div><div class="trade-group-title">Other actions</div><div class="trade-buttons"><button class="btn" onclick="manualBuyNextNowUp()">Buy next UP now</button><button class="btn" onclick="manualBuyNextNowDown()">Buy next DOWN now</button><button class="btn sell-action" onclick="manualSell()">Sell position</button></div></div></div>',
            '<div id="buyStatus" class="status" role="status" aria-live="polite">'+esc(buyStatusVal)+'</div>'
          ].join('');
          setHtmlIfChanged('manualTrade','manualTrade',manualHtml);

          var st=d.strategy||{}, sig=st.signal_text||'\u2014';
          function chk(x){ return x===true?'\u2713':x===false?'\u2717':'\u2014'; }
          var favKey='';
          if(st.favorite){
            var favText=String(st.favorite).toUpperCase();
            if(favText.indexOf('UP')===0) favKey='up';
            else if(favText.indexOf('DOWN')===0) favKey='down';
          }
          var favBook=(favKey&&(d[favKey]&&d[favKey].book))?d[favKey].book:null;
          var buyVol=(favBook&&typeof favBook.volume_buy==='number'&&!isNaN(favBook.volume_buy))?Math.round(favBook.volume_buy):null;
          var totalVol=(favBook&&typeof favBook.volume_total==='number'&&!isNaN(favBook.volume_total))?Math.round(favBook.volume_total):null;
          var strategyBits=['Fav: '+esc(st.favorite)+' \u00b7 WR: '+esc(st.win_rate_str),'Volume: Buy '+esc(buyVol!=null?buyVol:'\u2014')+' | Total '+esc(totalVol!=null?totalVol:'\u2014')];
          var btcNow=d.btc||{};
          var currMin=(typeof btcNow.current_window_min_usd==='number'&&!isNaN(btcNow.current_window_min_usd))?numFmtSigned(btcNow.current_window_min_usd,2):'\u2014';
          var currMax=(typeof btcNow.current_window_max_usd==='number'&&!isNaN(btcNow.current_window_max_usd))?numFmtSigned(btcNow.current_window_max_usd,2):'\u2014';
          strategyBits.push('Current BTC move min/max: $'+esc(currMin)+' / $'+esc(currMax));
          var currentStreakText='none';
          var ds=btcNow.direction_streak||{};
          if(ds.length>0&&ds.direction){ currentStreakText=ds.length+'x '+ds.direction; }
          strategyBits.push('Current Streak: '+esc(currentStreakText));
          var vs=st.volume_speed||null;
          if(vs){
            var buyDiff=(vs.curr_diff_buy!=null&&typeof vs.curr_diff_buy==='number'&&!isNaN(vs.curr_diff_buy))?numFmtSigned(vs.curr_diff_buy,0):'\u2014';
            var totalDiff=(vs.curr_diff_total!=null&&typeof vs.curr_diff_total==='number'&&!isNaN(vs.curr_diff_total))?numFmtSigned(vs.curr_diff_total,0):'\u2014';
            var minDiff=(vs.min_current_volume_diff!=null&&typeof vs.min_current_volume_diff==='number'&&!isNaN(vs.min_current_volume_diff))?numFmt(vs.min_current_volume_diff,0):'\u2014';
            strategyBits.push('Vol cmp: buy '+buyDiff+' | total '+totalDiff+' | min '+minDiff+' | ok '+chk(vs.ok));
          }
          if(st.btc_buffer_line) strategyBits.push('BTC Buffer: '+esc(st.btc_buffer_line));
          var modeBtcLines=st.btc_buffer_mode_lines||[];
          if(modeBtcLines&&modeBtcLines.length){
            for(var bi=0;bi<modeBtcLines.length;bi++){
              strategyBits.push('BTC Buffer '+esc(String(modeBtcLines[bi]||'')));
            }
          }
          var strategyHtml='<div class="sig '+sigClass(sig)+'">'+esc(sig)+'</div><div class="strategy-details mono">'+strategyBits.join('<br/>')+'</div>';
          setHtmlIfChanged('strategy','strategy',strategyHtml);

          function book(x,id){ var el=document.getElementById(id); if(!el) return; if(!x){ el.textContent='No data'; return; } var bk=x.book||{}, ind=x.indicators||{}; el.innerHTML=['Last '+esc(bk.last_price),'Bid '+esc(bk.best_bid)+' / Ask '+esc(bk.best_ask),'PM VWAP '+numFmt(ind.pm_vwap,4)+' \u00b7 BTC VWAP '+(ind.btc_vwap_weighted!=null?numFmt(ind.btc_vwap_weighted,4):'\u2014'),'Dev '+(ind.deviation_pct!=null?numFmt(ind.deviation_pct,2)+'%':'\u2014')+' \u00b7 BTC Vol Bias '+(ind.btc_vol_ratio!=null?numFmt(ind.btc_vol_ratio,1)+'%':'\u2014'),'Z '+numFmt(ind.zscore,2)+' \u00b7 Mom '+(ind.momentum_pct!=null?numFmt(ind.momentum_pct,2)+'%':'\u2014'),'Vol '+(bk.volume_total!=null?esc(Math.round(bk.volume_total)):'\u2014')].join('<br/>'); }
          book(d.up,'up'); book(d.down,'down');

          var b=d.btc||{}, btcEl=document.getElementById('btc');
          if(b.btc_current_price>0){
            var bits=['$'+esc(numFmt(b.btc_current_price,2)),'Anchor $'+(b.btc_anchor_price>0?esc(numFmt(b.btc_anchor_price,2)):'\u2014'),esc(b.deviation_line||'')];
            if(b.buffer_avg_abs_usd!=null||b.buffer_avg_abs_pct!=null){ var usd=b.buffer_avg_abs_usd!=null?'$'+esc(numFmt(b.buffer_avg_abs_usd,2)):'\u2014'; var pct=b.buffer_avg_abs_pct!=null?esc(numFmt(b.buffer_avg_abs_pct,3))+'%':'\u2014'; bits.push('Buffer avg(5): +/-'+usd+' (+/-'+pct+')'); }
            if(b.buffer_windows&&b.buffer_windows.length){ bits.push('\u2014 last 5 windows \u2014'); for(var wi=0;wi<b.buffer_windows.length;wi++){ var ww=b.buffer_windows[wi]; var wt=ww.window_ts?new Date(ww.window_ts*1000).toISOString().substr(11,8):'?'; var sUsd=(ww.signed_usd!=null&&typeof ww.signed_usd==='number'&&!isNaN(ww.signed_usd))?numFmtSigned(ww.signed_usd,2):numFmtSigned((ww.abs_usd!=null?ww.abs_usd:0),2); var minUsd=(ww.min_usd!=null&&typeof ww.min_usd==='number'&&!isNaN(ww.min_usd))?numFmtSigned(ww.min_usd,2):'\u2014'; var maxUsd=(ww.max_usd!=null&&typeof ww.max_usd==='number'&&!isNaN(ww.max_usd))?numFmtSigned(ww.max_usd,2):'\u2014'; bits.push(esc(wt)+' $'+esc(sUsd)+' | min $'+esc(minUsd)+' | max $'+esc(maxUsd)); } }
            bits.push('Feed: '+(b.btc_connected?'ok':'off')+(b.fresh_sec!=null?' \u00b7 '+Math.floor(b.fresh_sec)+'s':''));
            var btcHtml=bits.join('<br/>');
            if(lastHtml.btc!==btcHtml){
              lastHtml.btc=btcHtml;
              btcEl.innerHTML=btcHtml;
            }
          } else { btcEl.textContent='Waiting for Chainlink...'; }

          var saRun=document.getElementById('sa_current_run');
          if(saRun){ var ds=b.direction_streak||{}; saRun.textContent=(ds.length>0&&ds.direction)?(ds.length+'x '+ds.direction):'none'; }

          var srbRun=document.getElementById('srb_current_streak');
          if(srbRun){ var ds=b.direction_streak||{}; srbRun.textContent=(ds.length>0&&ds.direction)?(ds.length+'x '+ds.direction):'none'; }

          var streakEnds=(d.streak_end_counts&&d.streak_end_counts.length)?d.streak_end_counts:[];
          var streakEndLines=[];
          var summaryData=null;
          var hourlyDynamicStats=[];
          var sequenceData='';
          for(var sei=0;sei<streakEnds.length;sei++){
            var row=streakEnds[sei]||{};
            if(row._summary){
              summaryData=row.by_length||[];
              hourlyDynamicStats=row.hourly_dynamic_stats||[];
              window.latestHourlyDynamicStats=hourlyDynamicStats;
              sequenceData=row.sequence||'';
              continue;
            }
            var lenVal=(row.length!=null)?String(row.length):'\u2014';
            var dirVal=row.direction?String(row.direction):'?';
            var cntVal=(row.ended_count!=null)?String(row.ended_count):'0';
            var dirColor=dirVal.toLowerCase()==='up'?'#3fb950':dirVal.toLowerCase()==='down'?'#f85149':'#e6edf3';
            var dirHtml='<span style="color:'+dirColor+'">'+esc(dirVal)+'</span>';
            streakEndLines.push(esc(lenVal+'x ')+dirHtml+esc(' ended: '+cntVal));
          }
          updateHourlyDynamicStats(hourlyDynamicStats);
          if(sequenceData){
            var coloredSeq='';
            var i=0;
            while(i<sequenceData.length){
              var numStr='';
              while(i<sequenceData.length && sequenceData[i]>='0' && sequenceData[i]<='9'){
                numStr+=sequenceData[i];
                i++;
              }
              var dirChar=i<sequenceData.length?sequenceData[i]:'';
              var itemColor='#e6edf3';
              if(dirChar==='U'){
                itemColor='#3fb950';
              } else if(dirChar==='D'){
                itemColor='#f85149';
              }
              var item=esc(numStr);
              coloredSeq+='<span style="color:'+itemColor+'">'+item+'</span> ';
              i++;
            }
            streakEndLines.push('Sequence: '+coloredSeq);
          }
          if(summaryData&&summaryData.length){
            streakEndLines.push('---');
            for(var ssi=0;ssi<summaryData.length;ssi++){
              var srow=summaryData[ssi]||{};
              var slLen=(srow.length!=null)?String(srow.length):'\u2014';
              var slTotal=(srow.total_ends!=null)?String(srow.total_ends):'0';
              var slPct=(srow.pct_end_here!=null)?String(srow.pct_end_here):'0';
              streakEndLines.push(esc(slLen+'x: '+slTotal+' ('+slPct+'%)'));
            }
          }
          var streakEndsHtml=streakEndLines.length?streakEndLines.join('<br/>'):'No streak endings yet';
          setHtmlIfChanged('streakEnds','streakEnds',streakEndsHtml);

          var tr=d.trading||{};
          var tHtml='Markets '+esc(tr.markets_seen)+' \u00b7 Trades '+esc(tr.trade_count)+' \u00b7 PnL $'+(tr.total_pnl!=null?numFmt(tr.total_pnl,2):'\u2014')+'<br/>';
          var liveLower=String(liveStatusVal||'').toLowerCase();
          var sellPhase=(liveLower.indexOf('sell')>=0);
          if(sellPhase){
            tHtml+='Sell status: '+esc(liveStatusVal)+'<br/>';
          }
          if(tr.position){ var p=tr.position; var posPrefix=sellPhase?'SELLING LONG ':'LONG '; tHtml+=posPrefix+esc(p.token_name)+' @ '+esc(p.entry_price)+' \u00d7'+esc(p.contracts)+(p.hedged?' hedged':'')+'<br/>'; tHtml+='Unreal $'+(p.unrealized_pnl!=null?numFmt(p.unrealized_pnl,2):'\u2014')+'<br/>'; } else { tHtml+='No open position<br/>'; }
          var nextBuyInfo=d.manual_buy_next||{}; if(nextBuyInfo.pending){ var nextDir=String(nextBuyInfo.direction||'?'); var nextAmt=(nextBuyInfo.amount_usd!=null)?numFmt(nextBuyInfo.amount_usd,2):'\u2014'; tHtml+='<br/>Next window order: BUY '+esc(nextDir)+' $'+esc(nextAmt)+'<br/>'; }
          var nextWinResult=d.next_window_order_result||null; if(nextWinResult){ var nwrStatus=String(nextWinResult.status||'?'); var nwrDir=String(nextWinResult.direction||'?'); var nwrAmt=(nextWinResult.amount_usd!=null)?numFmt(nextWinResult.amount_usd,2):'\u2014'; var nwrLine=''; if(nwrStatus==='success'){ var nwrContracts=(nextWinResult.contracts!=null)?numFmt(nextWinResult.contracts,2):'\u2014'; var nwrPrice=(nextWinResult.price!=null)?numFmt(nextWinResult.price,4):'\u2014'; nwrLine='\u2705 Sent: BUY '+esc(nwrDir)+' $'+esc(nwrAmt)+' | '+esc(nwrContracts)+' contracts @ '+esc(nwrPrice); }else{ var nwrErr=String(nextWinResult.error||'Unknown'); nwrLine='\u274c Failed: BUY '+esc(nwrDir)+' $'+esc(nwrAmt)+' - '+esc(nwrErr); } tHtml+='<br/>Next Window Order Result: '+nwrLine+'<br/>'; }
          if(tr.recent_trades&&tr.recent_trades.length){ var lines=[]; for(var ri=0;ri<tr.recent_trades.length;ri++){ lines.push(esc(tr.recent_trades[ri].line)); } tHtml+='<br/>Recent:<br/>'+lines.join('<br/>'); }
          /* TEMPORARILY DISABLED: Mode Performance rendering
          var tr=d.trading||{}, modePerfEl=document.getElementById('modePerf');
          if(modePerfEl){ var modePerfLines=[], modePerfData=tr.win_rate_by_mode||{}; window.latestModePerfData=modePerfData; var panelHandledModes={}; var panelModeOrder=['normal','manual','mode_60s','mode_40s','mode_30s','mode_20s','unknown']; function pushPanelModeLine(modeKey){ if(!Object.prototype.hasOwnProperty.call(modePerfData,modeKey)) return; panelHandledModes[modeKey]=true; var ms=modePerfData[modeKey]||{}; var wrVal=(ms.win_rate_pct!=null&&typeof ms.win_rate_pct==='number'&&!isNaN(ms.win_rate_pct))?(numFmt(ms.win_rate_pct,1)+'%'):'\u2014'; var pnlVal=(ms.total_pnl_usd!=null&&typeof ms.total_pnl_usd==='number'&&!isNaN(ms.total_pnl_usd))?('$'+numFmtSigned(ms.total_pnl_usd,2)):'$\u2014'; var countVal=(ms.trade_count!=null)?String(ms.trade_count):'0'; modePerfLines.push(esc(modeKey)+' | PnL '+esc(pnlVal)+' | Triggers '+esc(countVal)+' | WR '+esc(wrVal)); } for(var pmo=0;pmo<panelModeOrder.length;pmo++){ pushPanelModeLine(panelModeOrder[pmo]); } for(var pmk in modePerfData){ if(!Object.prototype.hasOwnProperty.call(modePerfData,pmk)) continue; if(panelHandledModes[pmk]) continue; pushPanelModeLine(pmk); } var modePerfHtml=modePerfLines.length?modePerfLines.join('<br/>'):'No mode stats yet'; setHtmlIfChanged('modePerf','modePerf',modePerfHtml); }
          */
          window.latestModePerfData={};
          setHtmlIfChanged('trading','trading',tHtml);
          var targetMs=document.hidden?POLL_MS_HIDDEN:POLL_MS_ACTIVE;
          var elapsedMs=Math.max(0, Date.now()-startedAt);
          scheduleTick(Math.max(250, targetMs-elapsedMs));
        } catch(e){ errEl.textContent='Poll error: '+((e&&e.message)?e.message:e); scheduleTick(2000); }
      };
      r.onerror=function(){ pollInFlight=false; errEl.textContent='Network error (is the bot running?)'; scheduleTick(2000); };
      r.send();
    }

    document.addEventListener('visibilitychange', function(){
      scheduleTick(document.hidden?POLL_MS_HIDDEN:250);
    });

    loadStreakAlertConfig();
    loadStreakReversalBotConfig();
    tick();
  </script>
</body>
</html>
"""


def _sanitize_for_json(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, int) and not isinstance(obj, bool):
        return obj
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, str):
        return obj
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_json(v) for v in obj]
    return obj


class WebSnapshotHolder:
    """Thread-safe snapshot for /api/state."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: Dict[str, Any] = {"status": "starting"}

    def set(self, data: Dict[str, Any]) -> None:
        with self._lock:
            self._data = dict(data)

    def get(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._data)


def build_app(
  holder: WebSnapshotHolder,
  *,
  get_indicator_controls: Optional[Callable[[], Dict[str, Any]]] = None,
  update_indicator_controls: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  get_streak_alert: Optional[Callable[[], Dict[str, Any]]] = None,
  update_streak_alert: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  get_streak_reversal_bot: Optional[Callable[[], Dict[str, Any]]] = None,
  update_streak_reversal_bot: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  trigger_manual_buy: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  trigger_manual_buy_next: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  trigger_manual_sell: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
) -> FastAPI:
    app = FastAPI(title="BTC Live Bot", docs_url=None, redoc_url=None)

    @app.get("/", response_class=HTMLResponse)
    async def index() -> str:
        return _HTML

    @app.get("/favicon.ico", include_in_schema=False)
    async def favicon() -> Response:
        return Response(status_code=204)

    @app.get("/api/state")
    async def api_state():
        return JSONResponse(_sanitize_for_json(holder.get()))

    @app.get("/api/indicator-controls")
    async def api_get_indicator_controls():
      if not get_indicator_controls:
        return JSONResponse({"momentum": True, "vwap_deviation": True, "zscore": True})
      return JSONResponse(_sanitize_for_json(get_indicator_controls()))

    @app.post("/api/indicator-controls")
    async def api_update_indicator_controls(payload: Dict[str, Any] = Body(default={})):
      if not update_indicator_controls:
        return JSONResponse(_sanitize_for_json(payload or {}))
      return JSONResponse(_sanitize_for_json(update_indicator_controls(payload or {})))

    @app.get("/api/streak-alert")
    async def api_get_streak_alert():
      if not get_streak_alert:
        return JSONResponse({"enabled": False})
      return JSONResponse(_sanitize_for_json(get_streak_alert()))

    @app.post("/api/streak-alert")
    async def api_update_streak_alert(payload: Dict[str, Any] = Body(default={})):
      if not update_streak_alert:
        return JSONResponse(_sanitize_for_json(payload or {}))
      return JSONResponse(_sanitize_for_json(update_streak_alert(payload or {})))

    @app.get("/api/streak-reversal-bot")
    async def api_get_streak_reversal_bot():
      if not get_streak_reversal_bot:
        return JSONResponse({"enabled": False, "min_streak_length": 3, "time_left_price_pairs": []})
      return JSONResponse(_sanitize_for_json(get_streak_reversal_bot()))

    @app.post("/api/streak-reversal-bot")
    async def api_update_streak_reversal_bot(payload: Dict[str, Any] = Body(default={})):
      if not update_streak_reversal_bot:
        return JSONResponse(_sanitize_for_json(payload or {}))
      return JSONResponse(_sanitize_for_json(update_streak_reversal_bot(payload or {})))

    @app.post("/api/manual-buy")
    async def api_manual_buy(payload: Dict[str, Any] = Body(default={})):
      if not trigger_manual_buy:
        return JSONResponse({"ok": False, "error": "Manual buy is not enabled"})
      return JSONResponse(_sanitize_for_json(trigger_manual_buy(payload or {})))

    @app.post("/api/manual-buy-next")
    async def api_manual_buy_next(payload: Dict[str, Any] = Body(default={})):
      if not trigger_manual_buy_next:
        return JSONResponse({"ok": False, "error": "Next-window manual buy is not enabled"})
      return JSONResponse(_sanitize_for_json(trigger_manual_buy_next(payload or {})))

    @app.post("/api/manual-buy-next-now")
    async def api_manual_buy_next_now(payload: Dict[str, Any] = Body(default={})):
      if not trigger_manual_buy_next:
        return JSONResponse({"ok": False, "error": "Next-window manual buy is not enabled"})
      payload_copy = dict(payload or {})
      payload_copy["_buy_next_now"] = True
      return JSONResponse(_sanitize_for_json(trigger_manual_buy_next(payload_copy)))

    @app.post("/api/manual-sell")
    async def api_manual_sell(payload: Dict[str, Any] = Body(default={})):
      if not trigger_manual_sell:
        return JSONResponse({"ok": False, "error": "Manual sell is not enabled"})
      return JSONResponse(_sanitize_for_json(trigger_manual_sell(payload or {})))

    return app


def _client_probe_address(bind_host: str) -> str:
    """Address to test with socket.connect(); 0.0.0.0 / :: are not valid client targets."""
    if bind_host in ("0.0.0.0", ""):
        return "127.0.0.1"
    if bind_host in ("::", "[::]"):
        return "::1"
    return bind_host


def start_web_dashboard(
  host: str,
  port: int,
  holder: WebSnapshotHolder,
  *,
  get_indicator_controls: Optional[Callable[[], Dict[str, Any]]] = None,
  update_indicator_controls: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  get_streak_alert: Optional[Callable[[], Dict[str, Any]]] = None,
  update_streak_alert: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  get_streak_reversal_bot: Optional[Callable[[], Dict[str, Any]]] = None,
  update_streak_reversal_bot: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  trigger_manual_buy: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  trigger_manual_buy_next: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  trigger_manual_sell: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
) -> bool:
    """
    Start uvicorn in a daemon thread. Returns True if the port accepts connections
    shortly after start (False if bind failed or port is in use).
    """
    app = build_app(
      holder,
      get_indicator_controls=get_indicator_controls,
      update_indicator_controls=update_indicator_controls,
      get_streak_alert=get_streak_alert,
      update_streak_alert=update_streak_alert,
      get_streak_reversal_bot=get_streak_reversal_bot,
      update_streak_reversal_bot=update_streak_reversal_bot,
      trigger_manual_buy=trigger_manual_buy,
      trigger_manual_buy_next=trigger_manual_buy_next,
      trigger_manual_sell=trigger_manual_sell,
    )

    def run() -> None:
        try:
            uvicorn.run(
                app,
                host=host,
                port=port,
                log_level="warning",
                access_log=False,
            )
        except Exception:
            logger.exception("Web dashboard: uvicorn exited with an error")

    t = threading.Thread(target=run, name="web-dashboard", daemon=True)
    t.start()

    probe = _client_probe_address(host)
    for _ in range(60):
        time.sleep(0.1)
        try:
            with socket.create_connection((probe, port), timeout=0.4):
                return True
        except OSError:
            continue
    return False
