#!/usr/bin/env python3
"""
roon_web_display.py — Web-based Roon Display Server
Serves a browser display page with album art, clock, and track info.
Updates via WebSocket. Receives Shazam results from HyperPixel Pi.

Runs on the bridge Pi (192.168.8.118) alongside the Roon REST bridge.
Port: 8888
"""

import asyncio
import json
import os
import time
import threading
import base64
import requests
import math
from datetime import datetime
from io import BytesIO
from flask import Flask, render_template_string, jsonify, request, Response
from flask_sock import Sock

app = Flask(__name__)
sock = Sock(app)

CONFIG_FILE = os.path.expanduser("~/roon_web_config.json")

DEFAULT_CONFIG = {
    "bridge": "http://127.0.0.1:3001",
    "target_zone": "Lounge",
    "poll_interval": 3,
    "text_hold": 20,
    "touch_left_zone": 0.33,
    "touch_right_zone": 0.67,
    "touch_action_left": "volume_down",
    "touch_action_right": "volume_up",
    "touch_action_centre": "toggle_text",
    "touch_action_double": "play_pause",
    "touch_action_swipe_left": "next",
    "touch_action_swipe_right": "previous",
    "touch_vol_step": 5,
    "heat_pump_url": "http://192.168.8.118:5000/",
    # Display
    "bg_blur": 40,
    "bg_brightness": 0.5,
    "art_border_radius": 0,
    # Text
    "size_artist": 32,
    "size_album": 26,
    "size_track": 26,
    "line_spacing": 4,
    "artist_bold": True,
    "text_bg_opacity": 0.65,
    "text_bg_blur": 12,
    # Clock
    "clock_size": 300,
    "clock_day_size": 64,
    "clock_date_size": 48,
}

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE) as f:
            cfg = json.load(f)
        for k, v in DEFAULT_CONFIG.items():
            cfg.setdefault(k, v)
        return cfg
    return DEFAULT_CONFIG.copy()

cfg = load_config()

# --- Shared state ---
_state = {
    "mode": "clock",          # clock | roon | shazam
    "artist": "",
    "album": "",
    "track": "",
    "art_b64": "",
    "art_url": "",
    "seek": 0,
    "length": 0,
    "volume": 50,
    "zone_id": None,
    "output_id": None,
    "source": "roon",         # roon | shazam
}
_state_lock = threading.Lock()
_clients = set()
_clients_lock = threading.Lock()

# --- Roon API ---

def roon_get(endpoint, params=None):
    try:
        r = requests.get(f"{cfg['bridge']}/roonAPI/{endpoint}", params=params, timeout=5)
        return r.json()
    except:
        return None

def get_zone():
    data = roon_get("listZones")
    if not data:
        return None
    return next((z for z in data.get("zones", []) if z["display_name"] == cfg["target_zone"]), None)

def fetch_art_b64(image_key):
    try:
        r = requests.get(f"{cfg['bridge']}/roonAPI/getOriginalImage?image_key={image_key}", timeout=10)
        return base64.b64encode(r.content).decode()
    except:
        return ""

def fetch_url_b64(url):
    try:
        r = requests.get(url, timeout=10)
        return base64.b64encode(r.content).decode()
    except:
        return ""

def get_itunes_art_b64(artist, track):
    try:
        for sep in ['|', '(', '[']:
            track = track.split(sep)[0]
        track = track.strip()
        query = requests.utils.quote(f"{artist} {track}")
        r = requests.get(
            f"https://itunes.apple.com/search?term={query}&entity=song&limit=1",
            timeout=10
        )
        data = r.json()
        if data['resultCount'] > 0:
            url = data['results'][0].get('artworkUrl100', '').replace('100x100', '600x600')
            if url:
                return fetch_url_b64(url)
    except:
        pass
    return ""

def get_volume_pct(zone):
    try:
        vol = zone["outputs"][0]["volume"]
        v, mn, mx = vol["value"], vol["min"], vol["max"]
        return max(0, min(100, (v - mn) / (mx - mn) * 100))
    except:
        return 50

def push_state():
    with _state_lock:
        payload = json.dumps(_state)
    dead = set()
    with _clients_lock:
        clients = set(_clients)
    for ws in clients:
        try:
            ws.send(payload)
        except:
            dead.add(ws)
    if dead:
        with _clients_lock:
            _clients.difference_update(dead)

# --- Roon polling loop ---

_last_image_key = None
_last_track_id  = None

def roon_poll_loop():
    global _last_image_key, _last_track_id
    while True:
        try:
            zone = get_zone()
            if zone and zone["state"] == "playing" and "now_playing" in zone:
                np      = zone["now_playing"]
                image_key = np.get("image_key", "")
                line1   = np["two_line"]["line1"]
                line2   = np["two_line"]["line2"]
                album   = np.get("three_line", {}).get("line3", "")
                seek    = np.get("seek_position") or 0
                length  = np.get("length") or 0
                vol_pct = get_volume_pct(zone)
                zone_id   = zone.get("zone_id")
                output_id = zone["outputs"][0].get("output_id") if zone.get("outputs") else None
                radio   = length is None or length == 0

                if radio and ' - ' in line2:
                    parts = line2.split(' - ', 1)
                    artist, track = parts[0].strip(), parts[1].strip()
                    disp_album = line1
                    track_id   = line2
                    use_itunes = True
                else:
                    artist   = line2
                    track    = line1
                    disp_album = album
                    track_id = line1
                    use_itunes = False

                changed = (image_key != _last_image_key or track_id != _last_track_id)

                if changed:
                    print(f"[Roon] {artist} / {disp_album} / {track}")
                    art_b64 = ""
                    if use_itunes:
                        art_b64 = get_itunes_art_b64(artist, track)
                    if not art_b64 and image_key:
                        art_b64 = fetch_art_b64(image_key)
                    _last_image_key = image_key
                    _last_track_id  = track_id

                    with _state_lock:
                        _state.update({
                            "mode": "roon",
                            "artist": artist,
                            "album": disp_album,
                            "track": track,
                            "art_b64": art_b64,
                            "seek": seek,
                            "length": length,
                            "volume": vol_pct,
                            "zone_id": zone_id,
                            "output_id": output_id,
                            "source": "roon",
                        })
                    push_state()
                else:
                    with _state_lock:
                        _state.update({
                            "seek": seek,
                            "length": length,
                            "volume": vol_pct,
                            "zone_id": zone_id,
                            "output_id": output_id,
                        })
                    push_state()
            else:
                _last_image_key = None
                _last_track_id  = None
                with _state_lock:
                    if _state["mode"] == "roon":
                        _state["mode"] = "clock"
                        # Keep art_b64 for blurred clock background
                push_state()

        except Exception as e:
            print(f"[Poll] Error: {e}")

        time.sleep(cfg["poll_interval"])

# --- Routes ---

@app.route('/')
def index():
    return render_template_string(HTML, config=cfg)

@app.route('/shazam', methods=['POST'])
def shazam_result():
    data = request.get_json()
    if not data:
        return jsonify({"ok": False}), 400
    art_b64 = fetch_url_b64(data.get("art_url", ""))
    with _state_lock:
        _state.update({
            "mode": "shazam",
            "artist": data.get("artist", ""),
            "album": data.get("album", ""),
            "track": data.get("title", ""),
            "art_b64": art_b64,
            "source": "shazam",
        })
    push_state()
    print(f"[Shazam] {data.get('artist')} - {data.get('title')}")
    return jsonify({"ok": True})

@app.route('/action', methods=['POST'])
def action():
    data = request.get_json()
    act  = data.get("action")
    with _state_lock:
        zone_id   = _state.get("zone_id")
        output_id = _state.get("output_id")
        vol       = _state.get("volume", 50)
    step = cfg.get("touch_vol_step", 5)
    if act == "volume_up" and output_id:
        roon_get("change_volume_relative", {"volume": step, "outputId": output_id})
    elif act == "volume_down" and output_id:
        roon_get("change_volume_relative", {"volume": -step, "outputId": output_id})
    elif act == "next" and zone_id:
        roon_get("next", {"zoneId": zone_id})
    elif act == "previous" and zone_id:
        roon_get("previous", {"zoneId": zone_id})
    elif act == "play_pause" and zone_id:
        zone = get_zone()
        if zone:
            if zone["state"] == "playing":
                roon_get("pause", {"zoneId": zone_id})
            else:
                roon_get("play", {"zoneId": zone_id})
    return jsonify({"ok": True})

@app.route('/state')
def state():
    with _state_lock:
        return jsonify(_state)

@app.route('/config')
def config_page():
    return render_template_string(CONFIG_HTML, config=cfg, actions=TOUCH_ACTIONS)

@app.route('/config/save', methods=['POST'])
def config_save():
    global cfg
    try:
        new_cfg = request.get_json()
        cfg.update(new_cfg)
        with open(CONFIG_FILE, 'w') as f:
            json.dump(cfg, f, indent=2)
        # Push updated config to all clients
        push_state()
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@sock.route('/ws')
def ws(ws):
    with _clients_lock:
        _clients.add(ws)
    try:
        with _state_lock:
            payload = json.dumps(_state)
        ws.send(payload)
        while True:
            ws.receive(timeout=30)
    except:
        pass
    finally:
        with _clients_lock:
            _clients.discard(ws)

TOUCH_ACTIONS = ["volume_up","volume_down","next","previous","play_pause","toggle_text","heat_pump","none"]

CONFIG_HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Roon Web Display — Config</title>
<style>
:root {
  --bg:#0a0a0a; --surface:#111; --border:#222; --accent:#e8e8e8;
  --accent2:#888; --danger:#cc3333; --text:#e0e0e0; --text-dim:#666; --radius:4px;
}
* { box-sizing:border-box; margin:0; padding:0; }
body { background:var(--bg); color:var(--text); font-family:-apple-system,sans-serif;
  font-weight:300; min-height:100vh; padding:40px 20px 80px; }
.container { max-width:680px; margin:0 auto; }
header { margin-bottom:48px; border-bottom:1px solid var(--border); padding-bottom:24px; }
header h1 { font-size:11px; letter-spacing:.25em; text-transform:uppercase; color:var(--text-dim); margin-bottom:8px; }
header p { font-size:28px; font-weight:300; color:var(--accent); }
.badge { display:inline-block; font-size:10px; letter-spacing:.15em; text-transform:uppercase;
  color:#4488cc; border:1px solid #4488cc; padding:2px 8px; border-radius:2px; margin-top:8px; }
.section { margin-bottom:40px; overflow:visible; }
.section-title { font-size:10px; letter-spacing:.25em; text-transform:uppercase;
  color:var(--text-dim); margin-bottom:16px; padding-bottom:8px; border-bottom:1px solid var(--border); }
.field { display:flex; align-items:center; justify-content:space-between;
  padding:13px 0; border-bottom:1px solid #161616; gap:16px; overflow:visible; }
.field:last-child { border-bottom:none; }
.field-label { font-size:14px; font-weight:400; }
.field-hint { font-size:11px; color:var(--text-dim); margin-top:2px; font-family:monospace; }
.field-control { display:flex; align-items:center; gap:10px; flex-shrink:0; }
.field-value { font-family:monospace; font-size:12px; color:var(--accent2); min-width:44px; text-align:right; }
input[type=range] { -webkit-appearance:none; width:140px; height:2px; background:var(--border); outline:none; border-radius:1px; }
input[type=range]::-webkit-slider-thumb { -webkit-appearance:none; width:14px; height:14px; border-radius:50%; background:var(--accent); cursor:pointer; }
input[type=number], input[type=text], select {
  background:var(--surface); border:1px solid var(--border); color:var(--text);
  padding:6px 10px; font-family:monospace; font-size:12px; border-radius:var(--radius); text-align:center; }
select { width:160px; text-align:left; cursor:pointer; }
input[type=text] { width:200px; }
input[type=number] { width:90px; }
.toggle { position:relative; width:40px; height:22px; flex-shrink:0; }
.toggle input { opacity:0; width:0; height:0; }
.toggle-slider { position:absolute; inset:0; background:var(--border); border-radius:11px; cursor:pointer; transition:background .2s; }
.toggle-slider:before { content:""; position:absolute; width:16px; height:16px; left:3px; top:3px;
  background:var(--accent2); border-radius:50%; transition:transform .2s, background .2s; }
.toggle input:checked + .toggle-slider { background:#2a5a2a; }
.toggle input:checked + .toggle-slider:before { transform:translateX(18px); background:#66cc66; }
.actions { display:flex; gap:12px; margin-top:48px; flex-wrap:wrap; }
button { font-family:monospace; font-size:11px; letter-spacing:.15em; text-transform:uppercase;
  padding:11px 22px; border:1px solid var(--border); border-radius:var(--radius);
  cursor:pointer; transition:all .15s; background:transparent; color:var(--text); }
.btn-save { border-color:var(--accent2); color:var(--accent); }
.btn-save:hover { background:var(--accent); color:var(--bg); }
.btn-display { border-color:#224422; color:#88cc88; }
.btn-display:hover { background:#224422; }
.toast { position:fixed; bottom:24px; right:24px; background:var(--surface);
  border:1px solid var(--border); color:var(--text); padding:12px 20px;
  font-family:monospace; font-size:11px; border-radius:var(--radius);
  opacity:0; transform:translateY(8px); transition:all .2s; pointer-events:none; z-index:200; }
.toast.show { opacity:1; transform:translateY(0); }
.toast.error { border-color:var(--danger); color:#cc6666; }
</style>
</head>
<body>
<div class="container">
  <header>
    <h1>Configuration</h1>
    <p>Roon Web Display</p>
    <div class="badge">Web · Port 8888</div>
  </header>

  <div class="section">
    <div class="section-title">Roon Connection</div>
    <div class="field">
      <div><div class="field-label">Bridge Address</div><div class="field-hint">Roon REST bridge IP and port</div></div>
      <input type="text" id="bridge" value="{{ config.bridge }}">
    </div>
    <div class="field">
      <div><div class="field-label">Target Zone</div></div>
      <input type="text" id="target_zone" value="{{ config.target_zone }}" style="width:140px">
    </div>
    <div class="field">
      <div><div class="field-label">Poll Interval</div><div class="field-hint">Seconds between Roon checks</div></div>
      <div class="field-control">
        <input type="range" min="1" max="15" value="{{ config.poll_interval }}" oninput="upd('poll_interval',this.value,'s')">
        <span class="field-value" id="poll_interval_v">{{ config.poll_interval }}s</span>
      </div>
    </div>
  </div>

  <div class="section">
    <div class="section-title">Track Text</div>
    <div class="field">
      <div><div class="field-label">Artist Size</div></div>
      <div class="field-control">
        <input type="range" min="14" max="64" value="{{ config.size_artist }}" oninput="upd('size_artist',this.value,'px')">
        <span class="field-value" id="size_artist_v">{{ config.size_artist }}px</span>
      </div>
    </div>
    <div class="field">
      <div><div class="field-label">Album Size</div></div>
      <div class="field-control">
        <input type="range" min="12" max="52" value="{{ config.size_album }}" oninput="upd('size_album',this.value,'px')">
        <span class="field-value" id="size_album_v">{{ config.size_album }}px</span>
      </div>
    </div>
    <div class="field">
      <div><div class="field-label">Track Size</div></div>
      <div class="field-control">
        <input type="range" min="12" max="52" value="{{ config.size_track }}" oninput="upd('size_track',this.value,'px')">
        <span class="field-value" id="size_track_v">{{ config.size_track }}px</span>
      </div>
    </div>
    <div class="field">
      <div><div class="field-label">Line Spacing</div></div>
      <div class="field-control">
        <input type="range" min="0" max="20" value="{{ config.line_spacing }}" oninput="upd('line_spacing',this.value,'px')">
        <span class="field-value" id="line_spacing_v">{{ config.line_spacing }}px</span>
      </div>
    </div>
    <div class="field">
      <div><div class="field-label">Artist Bold</div></div>
      <label class="toggle">
        <input type="checkbox" id="artist_bold" {% if config.artist_bold %}checked{% endif %}>
        <span class="toggle-slider"></span>
      </label>
    </div>
    <div class="field">
      <div><div class="field-label">Text Hold</div><div class="field-hint">Seconds before text hides</div></div>
      <div class="field-control">
        <input type="range" min="5" max="60" value="{{ config.text_hold }}" oninput="upd('text_hold',this.value,'s')">
        <span class="field-value" id="text_hold_v">{{ config.text_hold }}s</span>
      </div>
    </div>
    <div class="field">
      <div><div class="field-label">Text BG Opacity</div><div class="field-hint">0=transparent 1=solid</div></div>
      <div class="field-control">
        <input type="range" min="0" max="1" step="0.05" value="{{ config.text_bg_opacity }}" oninput="upd('text_bg_opacity',this.value,'',2)">
        <span class="field-value" id="text_bg_opacity_v">{{ "%.2f"|format(config.text_bg_opacity) }}</span>
      </div>
    </div>
    <div class="field">
      <div><div class="field-label">Text BG Blur</div><div class="field-hint">Glassmorphism blur amount</div></div>
      <div class="field-control">
        <input type="range" min="0" max="40" value="{{ config.text_bg_blur }}" oninput="upd('text_bg_blur',this.value,'px')">
        <span class="field-value" id="text_bg_blur_v">{{ config.text_bg_blur }}px</span>
      </div>
    </div>
  </div>

  <div class="section">
    <div class="section-title">Background</div>
    <div class="field">
      <div><div class="field-label">Background Blur</div></div>
      <div class="field-control">
        <input type="range" min="0" max="80" value="{{ config.bg_blur }}" oninput="upd('bg_blur',this.value,'px')">
        <span class="field-value" id="bg_blur_v">{{ config.bg_blur }}px</span>
      </div>
    </div>
    <div class="field">
      <div><div class="field-label">Background Brightness</div><div class="field-hint">0=black 1=full</div></div>
      <div class="field-control">
        <input type="range" min="0.1" max="1" step="0.05" value="{{ config.bg_brightness }}" oninput="upd('bg_brightness',this.value,'',2)">
        <span class="field-value" id="bg_brightness_v">{{ "%.2f"|format(config.bg_brightness) }}</span>
      </div>
    </div>
    <div class="field">
      <div><div class="field-label">Art Border Radius</div><div class="field-hint">Rounded corners on art</div></div>
      <div class="field-control">
        <input type="range" min="0" max="48" value="{{ config.art_border_radius }}" oninput="upd('art_border_radius',this.value,'px')">
        <span class="field-value" id="art_border_radius_v">{{ config.art_border_radius }}px</span>
      </div>
    </div>
  </div>

  <div class="section">
    <div class="section-title">Clock</div>
    <div class="field">
      <div><div class="field-label">Clock Size</div><div class="field-hint">SVG width/height in px</div></div>
      <div class="field-control">
        <input type="range" min="150" max="500" value="{{ config.clock_size }}" oninput="upd('clock_size',this.value,'px')">
        <span class="field-value" id="clock_size_v">{{ config.clock_size }}px</span>
      </div>
    </div>
    <div class="field">
      <div><div class="field-label">Day Name Size</div></div>
      <div class="field-control">
        <input type="range" min="20" max="100" value="{{ config.clock_day_size }}" oninput="upd('clock_day_size',this.value,'px')">
        <span class="field-value" id="clock_day_size_v">{{ config.clock_day_size }}px</span>
      </div>
    </div>
    <div class="field">
      <div><div class="field-label">Date Size</div></div>
      <div class="field-control">
        <input type="range" min="16" max="80" value="{{ config.clock_date_size }}" oninput="upd('clock_date_size',this.value,'px')">
        <span class="field-value" id="clock_date_size_v">{{ config.clock_date_size }}px</span>
      </div>
    </div>
  </div>

  <div class="section">
    <div class="section-title">Touch Controls</div>
    <div class="field">
      <div><div class="field-label">Left Zone Width</div><div class="field-hint">Fraction of screen</div></div>
      <div class="field-control">
        <input type="range" min="0.1" max="0.45" step="0.01" value="{{ config.touch_left_zone }}" oninput="upd('touch_left_zone',this.value,'',2)">
        <span class="field-value" id="touch_left_zone_v">{{ "%.2f"|format(config.touch_left_zone) }}</span>
      </div>
    </div>
    <div class="field">
      <div><div class="field-label">Right Zone Start</div></div>
      <div class="field-control">
        <input type="range" min="0.55" max="0.9" step="0.01" value="{{ config.touch_right_zone }}" oninput="upd('touch_right_zone',this.value,'',2)">
        <span class="field-value" id="touch_right_zone_v">{{ "%.2f"|format(config.touch_right_zone) }}</span>
      </div>
    </div>
    <div class="field">
      <div><div class="field-label">Left Tap</div></div>
      <select id="touch_action_left">
        {% for a in actions %}<option value="{{ a }}" {% if config.touch_action_left==a %}selected{% endif %}>{{ a }}</option>{% endfor %}
      </select>
    </div>
    <div class="field">
      <div><div class="field-label">Right Tap</div></div>
      <select id="touch_action_right">
        {% for a in actions %}<option value="{{ a }}" {% if config.touch_action_right==a %}selected{% endif %}>{{ a }}</option>{% endfor %}
      </select>
    </div>
    <div class="field">
      <div><div class="field-label">Centre Tap</div></div>
      <select id="touch_action_centre">
        {% for a in actions %}<option value="{{ a }}" {% if config.touch_action_centre==a %}selected{% endif %}>{{ a }}</option>{% endfor %}
      </select>
    </div>
    <div class="field">
      <div><div class="field-label">Double Tap</div></div>
      <select id="touch_action_double">
        {% for a in actions %}<option value="{{ a }}" {% if config.touch_action_double==a %}selected{% endif %}>{{ a }}</option>{% endfor %}
      </select>
    </div>
    <div class="field">
      <div><div class="field-label">Swipe Left</div></div>
      <select id="touch_action_swipe_left">
        {% for a in actions %}<option value="{{ a }}" {% if config.touch_action_swipe_left==a %}selected{% endif %}>{{ a }}</option>{% endfor %}
      </select>
    </div>
    <div class="field">
      <div><div class="field-label">Swipe Right</div></div>
      <select id="touch_action_swipe_right">
        {% for a in actions %}<option value="{{ a }}" {% if config.touch_action_swipe_right==a %}selected{% endif %}>{{ a }}</option>{% endfor %}
      </select>
    </div>
    <div class="field">
      <div><div class="field-label">Volume Step</div></div>
      <div class="field-control">
        <input type="range" min="1" max="20" value="{{ config.touch_vol_step }}" oninput="upd('touch_vol_step',this.value,'')">
        <span class="field-value" id="touch_vol_step_v">{{ config.touch_vol_step }}</span>
      </div>
    </div>
    <div class="field">
      <div><div class="field-label">Heat Pump URL</div></div>
      <input type="text" id="heat_pump_url" value="{{ config.heat_pump_url }}">
    </div>
  </div>

  <div class="actions">
    <button class="btn-save" onclick="save()">Save Changes</button>
    <button class="btn-display" onclick="window.location.href='/'">Back to Display</button>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
function upd(key, value, suffix, decimals) {
  const el = document.getElementById(key + "_v");
  if (el) {
    const v = decimals !== undefined ? parseFloat(value).toFixed(decimals) : parseInt(value);
    el.textContent = v + (suffix || "");
  }
}

function getConfig() {
  return {
    bridge:              document.getElementById("bridge").value.trim(),
    target_zone:         document.getElementById("target_zone").value.trim(),
    poll_interval:       parseInt(document.querySelector("[oninput*=poll_interval]").value),
    size_artist:         parseInt(document.querySelector("[oninput*=size_artist]").value),
    size_album:          parseInt(document.querySelector("[oninput*=size_album]").value),
    size_track:          parseInt(document.querySelector("[oninput*=size_track]").value),
    line_spacing:        parseInt(document.querySelector("[oninput*=line_spacing]").value),
    artist_bold:         document.getElementById("artist_bold").checked,
    text_hold:           parseInt(document.querySelector("[oninput*=text_hold]").value),
    text_bg_opacity:     parseFloat(document.querySelector("[oninput*=text_bg_opacity]").value),
    text_bg_blur:        parseInt(document.querySelector("[oninput*=text_bg_blur]").value),
    bg_blur:             parseInt(document.querySelector("[oninput*=bg_blur]").value),
    bg_brightness:       parseFloat(document.querySelector("[oninput*=bg_brightness]").value),
    art_border_radius:   parseInt(document.querySelector("[oninput*=art_border_radius]").value),
    clock_size:          parseInt(document.querySelector("[oninput*=clock_size]").value),
    clock_day_size:      parseInt(document.querySelector("[oninput*=clock_day_size]").value),
    clock_date_size:     parseInt(document.querySelector("[oninput*=clock_date_size]").value),
    touch_left_zone:     parseFloat(document.querySelector("[oninput*=touch_left_zone]").value),
    touch_right_zone:    parseFloat(document.querySelector("[oninput*=touch_right_zone]").value),
    touch_action_left:   document.getElementById("touch_action_left").value,
    touch_action_right:  document.getElementById("touch_action_right").value,
    touch_action_centre: document.getElementById("touch_action_centre").value,
    touch_action_double: document.getElementById("touch_action_double").value,
    touch_action_swipe_left:  document.getElementById("touch_action_swipe_left").value,
    touch_action_swipe_right: document.getElementById("touch_action_swipe_right").value,
    touch_vol_step:      parseInt(document.querySelector("[oninput*=touch_vol_step]").value),
    heat_pump_url:       document.getElementById("heat_pump_url").value.trim(),
  };
}

function showToast(msg, error=false) {
  const t = document.getElementById("toast");
  t.textContent = msg;
  t.className = "toast show" + (error ? " error" : "");
  setTimeout(() => t.className = "toast", 2500);
}

async function save() {
  const r = await fetch("/config/save", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify(getConfig())
  });
  if (r.ok) showToast("Saved — reload display to apply");
  else showToast("Error saving", true);
}
</script>
</body>
</html>'''

# --- HTML ---

HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=no">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-fullscreen">
<title>Roon Display</title>
<style>
* { margin:0; padding:0; box-sizing:border-box; -webkit-tap-highlight-color:transparent; }

html, body {
  width:100%; height:100%;
  background:#000;
  overflow:hidden;
  font-family: -apple-system, 'Helvetica Neue', sans-serif;
}

#display {
  position:fixed; inset:0;
  display:flex; align-items:center; justify-content:center;
  background:#000;
}

/* Art background - blurred full screen */
#art-bg {
  position:absolute; inset:0;
  background-size:cover; background-position:center;
  filter:blur({{ config.bg_blur }}px) brightness({{ config.bg_brightness }});
  transform:scale(1.1);
  transition:background-image 0.8s ease;
}

/* Art square centred */
#art-img {
  position:relative; z-index:2;
  height:100vh; width:100vh;
  max-width:100vw;
  background-size:cover; background-position:center;
  flex-shrink:0;
  border-radius:{{ config.art_border_radius }}px;
  transition:background-image 0.8s ease;
}

/* Text overlay */
#text-overlay {
  position:absolute;
  bottom:32px; left:24px;
  z-index:10;
  max-width:65vw;
  background:rgba(0,0,0,{{ config.text_bg_opacity }});
  backdrop-filter:blur({{ config.text_bg_blur }}px);
  -webkit-backdrop-filter:blur({{ config.text_bg_blur }}px);
  border-radius:12px;
  padding:16px 20px;
  transition:opacity 0.8s ease;
}

#text-overlay.hidden { opacity:0; pointer-events:none; }

#text-artist {
  font-size:{{ config.size_artist }}px;
  font-weight:{{ "700" if config.artist_bold else "300" }};
  color:#fff;
  white-space:nowrap;
  overflow:hidden;
  text-overflow:ellipsis;
}

#text-album {
  font-size:{{ config.size_album }}px;
  font-weight:300;
  color:rgba(255,255,255,0.85);
  white-space:nowrap;
  overflow:hidden;
  text-overflow:ellipsis;
  margin-top:{{ config.line_spacing }}px;
}

#text-track {
  font-size:{{ config.size_track }}px;
  font-weight:300;
  color:rgba(255,255,255,0.85);
  white-space:nowrap;
  overflow:hidden;
  text-overflow:ellipsis;
  margin-top:{{ config.line_spacing }}px;
}

/* Progress bar */
#progress {
  position:absolute; bottom:0; left:0; right:0; height:4px;
  background:rgba(255,255,255,0.15); z-index:10;
}
#progress-fill {
  height:100%; background:#fff;
  transition:width 1s linear;
}

/* 4:3 layout (iPad landscape) */
@media (max-aspect-ratio: 5/4) {
  #art-img {
    height:100vw; width:100vw;
    max-height:100vh;
  }
  #text-overlay {
    left:50%; transform:translateX(-50%);
    max-width:90vw;
    bottom:48px;
    text-align:center;
  }
  #clock-container {
    flex-direction:column;
    align-items:center;
    gap:32px;
  }
  #clock-text { align-items:center; text-align:center; }
}

/* Clock screen */
#clock-screen {
  position:absolute; inset:0; z-index:5;
  display:flex; align-items:center; justify-content:center;
  background:#000;
  opacity:0; pointer-events:none;
  transition:opacity 0.5s;
}
#clock-screen.show { opacity:1; pointer-events:all; }

#clock-container {
  display:flex;
  align-items:center;
  gap:60px;
}

#clock-svg { flex-shrink:0; }

#clock-text {
  display:flex; flex-direction:column; gap:16px;
}

#clock-day {
  font-size:{{ config.clock_day_size }}px;
  font-weight:700; color:#fff;
  letter-spacing:-0.02em;
}

#clock-date {
  font-size:{{ config.clock_date_size }}px;
  font-weight:300; color:rgba(255,255,255,0.7);
}

/* Clock art bg */
#clock-art-bg {
  position:absolute; inset:0;
  background-size:cover; background-position:center;
  filter:blur(60px) brightness(0.3);
  transform:scale(1.1);
  opacity:0; transition:opacity 1s;
}
#clock-art-bg.show { opacity:1; }
</style>
</head>
<body>

<div id="display">
  <div id="art-bg"></div>
  <div id="art-img"></div>
  <div id="text-overlay" class="hidden">
    <div id="text-artist"></div>
    <div id="text-album"></div>
    <div id="text-track"></div>
  </div>
  <div id="progress"><div id="progress-fill" style="width:0%"></div></div>

</div>

<div id="clock-screen">
  <div id="clock-art-bg"></div>
  <div id="clock-container">
    <svg id="clock-svg" width="{{ config.clock_size }}" height="{{ config.clock_size }}" viewBox="0 0 300 300"></svg>
    <div id="clock-text">
      <div id="clock-day"></div>
      <div id="clock-date"></div>
    </div>
  </div>
</div>

<script>
const CFG = {
  leftZone:   {{ config.touch_left_zone }},
  rightZone:  {{ config.touch_right_zone }},
  actionLeft: "{{ config.touch_action_left }}",
  actionRight:"{{ config.touch_action_right }}",
  actionCentre:"{{ config.touch_action_centre }}",
  actionDouble:"{{ config.touch_action_double }}",
  actionSwipeLeft: "{{ config.touch_action_swipe_left }}",
  actionSwipeRight:"{{ config.touch_action_swipe_right }}",
  doubleTapMs: 400,
  swipeMinDx: 50,
  swipeMaxDy: 80,
  swipeMaxMs: 600,
  heatPumpUrl: "{{ config.heat_pump_url }}",
  textHold: {{ config.text_hold }} * 1000,
};

let state = null;
let showText = false;
let textTimer = null;
let lastArtKey = '';
let clockTimer = null;
let lastTapTime = 0;

// --- WebSocket ---
function connect() {
  const ws = new WebSocket(`ws://${location.host}/ws`);
  ws.onmessage = e => {
    const s = JSON.parse(e.data);
    handleState(s);
  };
  ws.onclose = () => setTimeout(connect, 2000);
}

function handleState(s) {
  state = s;
  if (s.mode === 'clock') {
    showClock();
  } else {
    hideClock();
    updateArt(s.art_b64);
    if (s.source === 'shazam') {
      } else {
      }
    // Show text on new track
    const artKey = s.artist + s.track;
    if (artKey !== lastArtKey) {
      lastArtKey = artKey;
      showTextOverlay();
    }
    updateText(s);
    updateProgress(s.seek, s.length);
  }
}

function updateArt(b64) {
  if (!b64) return;
  const url = `data:image/jpeg;base64,${b64}`;
  document.getElementById('art-bg').style.backgroundImage = `url(${url})`;
  document.getElementById('art-img').style.backgroundImage = `url(${url})`;
  document.getElementById('clock-art-bg').style.backgroundImage = `url(${url})`;
}

function updateText(s) {
  document.getElementById('text-artist').textContent = s.artist;
  document.getElementById('text-album').textContent  = s.album;
  document.getElementById('text-track').textContent  = s.track;
}

function showTextOverlay() {
  showText = true;
  document.getElementById('text-overlay').classList.remove('hidden');
  clearTimeout(textTimer);
  textTimer = setTimeout(() => {
    document.getElementById('text-overlay').classList.add('hidden');
    showText = false;
  }, CFG.textHold);
}

function hideTextOverlay() {
  clearTimeout(textTimer);
  document.getElementById('text-overlay').classList.add('hidden');
  showText = false;
}

function toggleText() {
  if (showText) hideTextOverlay();
  else showTextOverlay();
}

function updateProgress(seek, length) {
  if (!length) return;
  const pct = Math.min(100, (seek / length) * 100);
  document.getElementById('progress-fill').style.width = pct + '%';
}



// --- Clock ---
function showClock() {
  document.getElementById('clock-screen').classList.add('show');
  if (state && state.art_b64) {
    document.getElementById('clock-art-bg').style.backgroundImage = `url(data:image/jpeg;base64,${state.art_b64})`;
    document.getElementById('clock-art-bg').classList.add('show');
  }
  drawClock();
  if (!clockTimer) clockTimer = setInterval(drawClock, 1000);
}

function hideClock() {
  document.getElementById('clock-screen').classList.remove('show');
  clearInterval(clockTimer);
  clockTimer = null;
}

function drawClock() {
  const now  = new Date();
  const svg  = document.getElementById('clock-svg');
  const cx = 150, cy = 150, r = 130;
  let html = `<circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="rgba(255,255,255,0.9)" stroke-width="3"/>`;

  // Tick marks
  for (let i = 0; i < 12; i++) {
    const a = (i * 30 - 90) * Math.PI / 180;
    const inner = i % 3 === 0 ? r - 22 : r - 14;
    const w = i % 3 === 0 ? 3 : 2;
    html += `<line x1="${cx + inner*Math.cos(a)}" y1="${cy + inner*Math.sin(a)}"
      x2="${cx + (r-4)*Math.cos(a)}" y2="${cy + (r-4)*Math.sin(a)}"
      stroke="white" stroke-width="${w}"/>`;
  }

  // Hour hand
  const ha = ((now.getHours()%12)*30 + now.getMinutes()*0.5 - 90) * Math.PI / 180;
  html += `<line x1="${cx}" y1="${cy}"
    x2="${cx + r*0.55*Math.cos(ha)}" y2="${cy + r*0.55*Math.sin(ha)}"
    stroke="white" stroke-width="8" stroke-linecap="round"/>`;

  // Minute hand
  const ma = (now.getMinutes()*6 - 90) * Math.PI / 180;
  html += `<line x1="${cx}" y1="${cy}"
    x2="${cx + r*0.8*Math.cos(ma)}" y2="${cy + r*0.8*Math.sin(ma)}"
    stroke="white" stroke-width="5" stroke-linecap="round"/>`;

  // Centre dot
  html += `<circle cx="${cx}" cy="${cy}" r="8" fill="white"/>`;

  svg.innerHTML = html;

  // Day / date
  const days  = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
  const months = ['January','February','March','April','May','June',
                  'July','August','September','October','November','December'];
  document.getElementById('clock-day').textContent  = days[now.getDay()];
  document.getElementById('clock-date').textContent =
    `${now.getDate()} ${months[now.getMonth()]} ${String(now.getFullYear()).slice(2)}`;
}

// --- Touch ---
let touchStart = null;

document.addEventListener('touchstart', e => {
  const t = e.touches[0];
  touchStart = { x: t.clientX, y: t.clientY, time: Date.now() };
}, { passive: true });

document.addEventListener('touchend', e => {
  if (!touchStart) return;
  const t = e.changedTouches[0];
  const dx = t.clientX - touchStart.x;
  const dy = t.clientY - touchStart.y;
  const dt = Date.now() - touchStart.time;
  const W  = window.innerWidth;
  const mx = (t.clientX + touchStart.x) / 2;

  // Swipe
  if (Math.abs(dx) >= CFG.swipeMinDx && Math.abs(dy) <= CFG.swipeMaxDy && dt <= CFG.swipeMaxMs) {
    sendAction(dx < 0 ? CFG.actionSwipeLeft : CFG.actionSwipeRight);
    touchStart = null;
    return;
  }

  // Tap
  if (Math.abs(dx) <= 20 && Math.abs(dy) <= 20) {
    if (mx < W * CFG.leftZone) {
      sendAction(CFG.actionLeft);
    } else if (mx > W * CFG.rightZone) {
      sendAction(CFG.actionRight);
    } else {
      const now = Date.now();
      if (now - lastTapTime < CFG.doubleTapMs) {
        sendAction(CFG.actionDouble);
        lastTapTime = 0;
      } else {
        lastTapTime = now;
        toggleText();
        sendAction(CFG.actionCentre);
      }
    }
  }
  touchStart = null;
}, { passive: true });

function sendAction(action) {
  if (!action || action === 'none') return;
  if (action === 'toggle_text') { toggleText(); return; }
  if (action === 'heat_pump') { window.location.href = CFG.heatPumpUrl; return; }
  fetch('/action', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action })
  });
}

// Prevent context menu / default touch
document.addEventListener('contextmenu', e => e.preventDefault());

// Init
connect();
drawClock();
clockTimer = setInterval(drawClock, 1000);
</script>
</body>
</html>'''

if __name__ == '__main__':
    if not os.path.exists(CONFIG_FILE):
        import json as _j
        with open(CONFIG_FILE, 'w') as f:
            _j.dump(DEFAULT_CONFIG, f, indent=2)
    t = threading.Thread(target=roon_poll_loop, daemon=True)
    t.start()
    print("Roon Web Display running on http://0.0.0.0:8888")
    app.run(host='0.0.0.0', port=8888, debug=False)
