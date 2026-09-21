# DESIGN — Live Network Traffic Analyser (LNTA)

> Scope: the Streamlit dashboard (the only user-facing surface) + CLI/log conventions.
> Implementation notes assume **Streamlit + Plotly + custom CSS**. Owner: Priyan S.

---

## Design Philosophy

### Visual Style — "Network Operations Console"
The UI should feel like a professional **NOC / observability console** (think packet-analyser meets Grafana), because the project's core idea is *watching a live stream of packets*:
- **Dark, low-glare canvas** with luminous data colours — readable in a dim demo room and on a projector.
- **Data-dense but ordered**: a persistent KPI strip, then panels on a strict grid.
- **Monospace for machine data** (IPs, ports, counts); humanist sans for labels.
- **Protocol colour-coding** echoing Wireshark's colouring-rule idea, used consistently in every chart.
- A subtle **"live pulse"** indicator communicates that data is streaming.

### Branding Direction
- Wordmark: **LNTA** + subtitle "Live Network Traffic Analyser". Small glyph: three concentric arcs (Wi-Fi) ending in a pulse line. (SVG inline; no external image.)
- Tone: precise, technical, calm. No hype copy, no "ATTACK DETECTED" language — use "Traffic spike", "Unusual port activity".

### UX Goals
1. **Answer "what is happening right now?" in 3 seconds** (KPI strip + live chart).
2. **Show the algorithm, not just the number**: Concept panels show *exact vs estimated* side by side so evaluators can verify.
3. **Every alert explains itself** (value, baseline, change, threshold, reason).
4. **Never a blank or broken screen**: every panel has loading / empty / error / stale states.
5. **Demo-safe**: a visible source badge (`LIVE` / `REPLAY` / `DEMO DATA`) so the audience always knows what they are seeing.

---

## Information Architecture

### Navigation Structure
Single-page app; **persistent header + KPI strip**, then **tabs**:

```text
┌ Header: LNTA logo · source badge (LIVE/REPLAY) · pulse · last-update · theme note ┐
├ KPI strip: Packets/s · Bytes/s · Unique IPs · Active Ports · Decay Score · Alerts ┤
├ Sidebar (collapsible): Window length · Refresh · Protocol filter · Top-N · Source ┤
└ Tabs:  1 Live Overview │ 2 Stream Analytics │ 3 Link Analysis │ 4 Alerts │ 5 History │ 6 Pipeline ┘
```

### Screen Hierarchy
```text
Dashboard
├─ Live Overview        (traffic over time, protocol mix, top ports, recent behaviour)
├─ Stream Analytics    (filtering · sampling · distinct · counting ones · moments · decay · itemsets)
├─ Link Analysis        (IP graph · PageRank · Markov · centrality)
├─ Alerts               (active + recent alerts, explanations)
├─ History              (Hive / Spark SQL results)
└─ Pipeline             (health of each stage)
```

### User Flow (matches the 16-step demo)
```text
Start capture ─▶ Overview shows LIVE pulse & rising packets/s
   ─▶ Pipeline tab: Flume/HDFS/Spark healthy ─▶ Stream Analytics tab: filter → window → distinct → moments → decay → itemsets
   ─▶ Link Analysis: graph → PageRank/Markov ─▶ Alerts: trigger a spike, read explanation ─▶ History tab: Hive results
```

---

## Screen Specifications

### Global Header + KPI Strip
- **Purpose:** Persistent situational awareness.
- **Components:** Logo, source badge, live pulse dot, "updated 2 s ago", 6 KPI cards.
- **KPI cards:** value (mono, 28 px), label (12 px caps), delta vs previous window (▲/▼ + %), sparkline (last 60 s).
- **Loading:** skeleton shimmer cards. **Empty:** "—" with caption "Waiting for stream…". **Error:** card border red, tooltip with error.
- **Stale rule:** if last data > 15 s old → pulse turns amber, badge "STALE".
- **Responsive:** 6 → 3×2 (tablet) → 2×3 (mobile).

### 1 · Live Overview
- **Purpose:** Current traffic level and mix.
- **Components:** (a) *Traffic over time* dual-axis line/area (packets/s left, bytes/s right) with window-length toggle (10 s / 30 s / 60 s); (b) *Protocol distribution* donut (TCP/UDP/ICMP/Other, protocol colours); (c) *Top destination ports* horizontal bar with service names (443 HTTPS, 53 DNS, 80 HTTP…); (d) *Recent behaviour* strip: decay-score line + trend arrow.
- **Layout:** Row 1: (a) full width, height 320 px. Row 2: (b) 1/3 + (c) 2/3. Row 3: (d) full width, 140 px.
- **Interactions:** Hover tooltips; legend click toggles series; window toggle re-queries.
- **Empty:** ghost chart + "No traffic yet — start capture or replay". **Error:** inline banner with retry. **Loading:** skeleton.
- **Responsive:** stack vertically; charts min-height 240 px.

### 2 · Stream Analytics
- **Purpose:** Prove each core concept with visible evidence.
- **Layout:** 2-column grid of **concept cards**; each card has a title chip (e.g. `CONCEPT · COUNT DISTINCT`), a one-line plain-English explanation, the visual, and an "exact vs estimate" footer.
- **Cards:**
  1. **Stream Filtering** — chips for named filters (TCP-only, dport 443, UDP, size > N); counts per filter; bar of filtered share of total.
  2. **Sampling** — sample size, sample-mean vs full-mean packet size, error %.
  3. **Count Distinct** — unique src IPs / dst IPs / ports: Exact · HLL · Flajolet–Martin columns with % error.
  4. **Counting Ones** — predicate selector; exact ones vs DGIM estimate over last N packets; mini bit-stream strip.
  5. **Estimating Moments** — mean, variance, std of packet size; inter-arrival mean/std; AMS F2 vs exact F2.
  6. **Decaying Window** — live score + half-life selector + top decayed keys (IP/port).
  7. **Market Basket · Frequent Itemsets** — table `{TCP:443, UDP:53}  support 41%` with support bars; algorithm toggle (A-Priori / PCY) showing passes made; min-support slider.
- **Interactions:** Info popover per card (definition + formula); window-length control affects all cards.
- **States:** per-card skeleton / "not enough data (n < 2)" for variance / error badge.
- **Responsive:** 2 columns → 1 column below 900 px.

### 3 · Link Analysis
- **Purpose:** Show IP communication structure.
- **Components:** (a) *Interactive graph* (Plotly): node size = PageRank, node colour = private (cyan) vs public (violet), edge width = bytes, top-N pruning slider; (b) *PageRank table* (rank, IP, score, in/out degree); (c) *Markov panel*: transition heatmap for top-K nodes + "next-hop probabilities" for a selected IP; (d) *Centrality* bar (degree centrality).
- **Copy rule:** caption under PageRank: "Structural importance in the observed graph — not a measure of risk."
- **Empty:** "Need at least 2 IPs and 1 edge". **Loading:** graph skeleton. **Error:** banner.
- **Responsive:** graph full width, tables below.

### 4 · Alerts
- **Purpose:** Explainable behavioural alerts.
- **Components:** Active alert cards; recent-alert timeline; filter by type/severity.
- **Alert card:** severity icon + label (INFO/WARN/CRITICAL), type (TRAFFIC SPIKE / UNUSUAL PORT ACTIVITY / HIGH FAN-OUT), source IP (mono), *Current / Baseline / Change / Threshold*, reason sentence, timestamp, "View in graph" link.
- **Empty:** green "No alerts in the last N minutes". **Error:** banner.

### 5 · History
- **Purpose:** Historical (HDFS/Hive) results beside live data.
- **Components:** Query selector (protocol totals, top src, top dst, traffic by hour, avg packet size, trend); result table + chart; "last run" timestamp; note about data source ("Hive / Spark SQL over HDFS").
- **Empty:** "No historical results yet — run `run_historical.py`." **Error:** banner.

### 6 · Pipeline
- **Purpose:** Prove the Big Data stack is alive.
- **Components:** Stage tiles Capture → Flume → HDFS → Spark → Serving, each with status dot (OK/Lagging/Down), last-event time; charts: batch duration vs trigger interval, input rows/batch, end-to-end lag; counters: bad records, late records.
- **States:** Down = red tile + last-seen time.

---

## Design System

### Colors

| Token | Hex | Use |
|---|---|---|
| `--bg-base` | `#0A0F1C` | App background |
| `--bg-panel` | `#101828` | Cards/panels |
| `--bg-elevated` | `#162033` | Hover, popovers |
| `--border` | `#1F2A44` | Panel borders |
| `--text-primary` | `#E6EDF7` | Main text |
| `--text-secondary` | `#A9B7CC` | Labels |
| `--text-muted` | `#8496B0` | Captions (still ≥ 4.5:1 on panel — verify) |
| `--accent` | `#22D3EE` | Primary accent, links, focus ring |
| `--accent-2` | `#6366F1` | Secondary accent, selected tab |
| `--proto-tcp` | `#38BDF8` | TCP |
| `--proto-udp` | `#A78BFA` | UDP |
| `--proto-icmp` | `#FBBF24` | ICMP |
| `--proto-other` | `#94A3B8` | Other |
| `--ok` | `#22C55E` | Healthy / no alerts |
| `--warn` | `#F59E0B` | Warning / stale |
| `--critical` | `#EF4444` | Critical / down |
| `--info` | `#38BDF8` | Info |

Chart categorical order: `#22D3EE, #A78BFA, #FBBF24, #34D399, #F472B6, #94A3B8`. Provide a light theme only if time permits (Nice-to-have).

### Typography
- **UI:** Inter → fallback `Segoe UI, system-ui, sans-serif`.
- **Data:** JetBrains Mono → fallback `Consolas, "Courier New", monospace`. (Google Fonts need internet; fallbacks must look acceptable offline.)
- **Scale:** Display 28/32 · H1 22/28 · H2 18/24 · H3 15/20 · Body 14/20 · Caption 12/16 · KPI number 28 mono. Weights: 400 / 500 / 600.

### Spacing
Base unit **4 px**; scale 4, 8, 12, 16, 24, 32, 48. Panel padding 16; grid gap 16; section gap 24.

### Grid System
12-column, 16 px gutters; content max-width 1440 px. Breakpoints: **≥1200** desktop · **900–1199** small desktop · **600–899** tablet · **<600** mobile.

### Component Library
- **Buttons:** Primary (accent fill, dark text) · Secondary (outline) · Ghost (text). States: default, hover (+8% lighten), focus (2 px accent ring), active, disabled (40% opacity), loading (spinner).
- **Inputs (select, slider, toggle, text):** panel background, 1 px border; focus ring accent; error = critical border + helper text; disabled 40%.
- **Cards:** `--bg-panel`, 1 px `--border`, radius 12, optional 2 px top accent line coloured by concept/severity; hover raises to `--bg-elevated`.
- **Tables:** dense (row 32 px), sticky header, zebra off, mono numerics right-aligned, IPs left-aligned mono, row hover highlight; sortable headers.
- **Modals/Popovers:** used only for concept explanations (`?` icon); max-width 480; Esc closes; focus trapped.
- **Notifications/Toasts:** top-right; success/warn/critical/info with icon + text; auto-dismiss 5 s (critical persists until dismissed).
- **Badges/Chips:** source badge, protocol chips (protocol colour dot + label), concept chips.

---

## Accessibility Requirements
- **WCAG 2.1 AA:** body text ≥ 4.5:1, large text/UI ≥ 3:1 (verify every token pair with a contrast checker).
- **Keyboard:** all controls reachable by Tab; visible 2 px focus ring; Esc closes popovers.
- **Screen reader:** charts have a text summary (`aria-label`/caption) and a "View as table" toggle; alert cards use `role="status"` (critical: `role="alert"`).
- **Colour independence:** severity uses icon + text; protocols use label + colour; graph nodes differ by shape or label, not only colour.
- **Motion:** respect `prefers-reduced-motion` (disable pulse/animations).

## Micro-interactions
- **Hover:** cards lift to `--bg-elevated`; chart tooltips with crosshair; table row highlight.
- **Transitions:** tab change fade 150 ms; number change count-up ≤ 300 ms (disabled under reduced motion).
- **Loading:** skeleton shimmer 1.2 s loop; never spinners on whole page.
- **Live pulse:** 8 px dot, 2 s ease-in-out opacity pulse; green = fresh, amber = stale, red = down.
- **Success:** brief accent flash on updated KPI value.
- **Error:** shake-free — red border + inline message; retry button.

## Mobile Responsiveness Strategy
- **Breakpoints:** see Grid System. Streamlit columns collapse to single column < 900 px via CSS.
- **Layout shifts:** KPI strip wraps to 2 columns; sidebar becomes a collapsible drawer; graph height reduced to 360 px.
- **Touch targets:** ≥ 44 × 44 px; sliders have larger thumbs; tooltips also open on tap.
- **Priority on small screens:** KPI strip → traffic chart → alerts; heavy panels (graph, heatmap) collapsed by default.

---

## Streamlit Implementation Notes (for Priyan S / AI agent)
- Theme via `.streamlit/config.toml` (`base="dark"`, colours above) + one injected `dashboard/assets/theme.css`.
- Auto-refresh with a fragment/timer rather than full-page reruns (⚠️ `st.fragment(run_every=…)` requires a recent Streamlit release — verify installed version; fallback: `streamlit-autorefresh`).
- Plotly template `lnta_dark` defined once in `dashboard/theme.py`; **no per-chart colour hard-coding**.
- Read serving DB **read-only**, cached with short TTL (2 s); never write from the UI thread.
- Provide `--mock` mode (synthetic data) so design work never blocks on the pipeline.
