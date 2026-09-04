#!/usr/bin/env python3
"""One-time generator: assembles site/index.html for the public macro dashboard
from the private vol-dashboard-ibkr frontend.

Extracts only the Macro + Liquidity sections (HTML + their self-contained JS
block) so no volatility/trade-journal code ships in the public page, prepends
a fetch shim that serves /api/* from static JSON under site/data/, and rewrites
absolute /static/ paths to relative ones (github.io project pages are served
from a subpath).

Blocks are located by unique markers, not line numbers, so edits to the private
frontend don't break extraction. Re-run after improving the macro/liquidity
sections:
    python3 gen_frontend.py [/path/to/vol-dashboard-ibkr/static/index.html]
"""
import sys
from pathlib import Path

SRC = Path(sys.argv[1] if len(sys.argv) > 1 else
           Path.home() / "projects/vol-dashboard-ibkr/static/index.html")
OUT = Path(__file__).parent / "site" / "index.html"

lines = SRC.read_text().splitlines(keepends=True)


def _find(marker: str, start: int = 0) -> int:
    """0-based index of the first line at/after `start` containing marker."""
    for i in range(start, len(lines)):
        if marker in lines[i]:
            return i
    raise SystemExit(f"marker not found: {marker!r}")


def block(start_marker: str, end_marker: str, from_line1: bool = False,
          end_exclusive: bool = False) -> str:
    a = 0 if from_line1 else _find(start_marker)
    b = _find(end_marker, a + 1)
    return "".join(lines[a:b] if end_exclusive else lines[a:b + 1])


head = block("<!DOCTYPE", "</style>", from_line1=True)
header = block("</head>", "</header>")
macro_section = block('id="section-macro"', "<!-- /section-macro -->")
liq_section = block('id="section-liquidity"', "<!-- /section-liquidity -->")
macro_modal = block("<!-- Macro chart expand modal -->", 'id="bb-modal"',
                    end_exclusive=True)
main_js = block("// ── Meta nav", "Liquidity sentinel handling")

# ── Patches ───────────────────────────────────────────────────────────────────
# Public header: "Brave Hunter" without the "Trading" suffix (logo unchanged)
header = header.replace("Brave Hunter <span>Trading</span>", "Brave Hunter")
head = head.replace("<title>Brave Hunter Trading</title>", "<title>Brave Hunter</title>")
macro_section = macro_section.replace('id="section-macro" class="section-panel"',
                                      'id="section-macro" class="section-panel active"')
main_js = main_js.replace("let _activeSection = 'volatility'", "let _activeSection = 'macro'")
main_js = main_js.replace(
    "    if (newSection === 'journal' && !_journalLoaded) loadJournalSection()\n", "")

nav = """
<nav class="meta-nav">
  <button class="active" data-section="macro">Macro</button>
  <button data-section="liquidity">Liquidity</button>
</nav>
"""

shim = """
<script>
// ── Static-data shim ─────────────────────────────────────────────────────────
// This public build has no backend: /api/* calls are served from JSON files
// under ./data/, regenerated daily by GitHub Actions. `days` filtering happens
// client-side for simple series; liquidity payloads are pre-built per range.
(function () {
  const LIQ_BUCKETS = new Set([365, 730, 1825, 3650])
  const orig = window.fetch.bind(window)
  const fake = obj => Promise.resolve({ ok: true, status: 200, json: async () => obj })
  const fail = () => Promise.resolve({ ok: false, status: 404, json: async () => ({}) })
  const load = p => orig('data/' + p).then(r => { if (!r.ok) throw new Error(p); return r.json() })
  const clip = (obs, days) => {
    if (!days) return obs
    const c = new Date(); c.setDate(c.getDate() - days)
    const cs = c.toISOString().slice(0, 10)
    return obs.filter(o => o.ts >= cs)
  }
  window.fetch = function (url, opts) {
    if (typeof url !== 'string' || !url.startsWith('/api/')) return orig(url, opts)
    const [path, qs] = url.split('?')
    const days = parseInt(new URLSearchParams(qs || '').get('days') || '0', 10) || 0
    if (path === '/api/macro/overview') return load('macro_overview.json').then(fake)
    if (path === '/api/macro/velocity') return load('macro_velocity.json').then(fake)
    if (path.startsWith('/api/macro/series/')) {
      const sid = decodeURIComponent(path.split('/').pop())
      return load('series/' + sid + '.json')
        .then(d => { const obs = clip(d.observations || [], days)
                     return fake({ ...d, observations: obs, count: obs.length }) })
        .catch(fail)
    }
    if (path === '/api/forward-curves/treasury') return load('fwd_treasury.json').then(fake).catch(fail)
    if (path === '/api/forward-curves/fed-funds') return load('fwd_fedfunds.json').then(fake).catch(fail)
    if (path === '/api/liquidity/quality-overlays') return load('liq_quality.json').then(fake).catch(fail)
    if (path === '/api/liquidity/headline') {
      const b = LIQ_BUCKETS.has(days) ? days : 'all'
      return load('liq_headline_' + b + '.json').then(fake).catch(fail)
    }
    if (path.startsWith('/api/liquidity/sub-index/')) {
      const name = path.split('/').pop()
      const b = LIQ_BUCKETS.has(days) ? days : 'all'
      return load('liq_sub_' + name + '_' + b + '.json').then(fake).catch(fail)
    }
    return fail()
  }
})()
</script>
"""

glue = """
// ── Public-build init ────────────────────────────────────────────────────────
loadMacroSection()
fetch('/api/macro/overview').then(r => r.json()).then(d => {
  const latest = (d.indicators || []).reduce((m, x) => (!m || x.latest_ts > m) ? x.latest_ts : m, null)
  const st = document.getElementById('status-text')
  if (st) st.textContent = latest ? `Data as of ${latest}` : 'Static build'
  const dot = document.getElementById('dot')
  if (dot) dot.style.background = '#2fd18c'
}).catch(() => {})
"""

page = (head + header + nav
        + macro_section + "\n" + liq_section + "\n" + macro_modal
        + shim
        + "<script>\n" + main_js + glue + "\n</script>\n</body>\n</html>\n")

# Relative paths for github.io project-page subpath hosting
page = page.replace('"/static/', '"static/').replace("'/static/", "'static/")

OUT.write_text(page)
print(f"wrote {OUT} ({len(page):,} bytes)")
for token in ("scanner", "journal", "thetadata", "ibkr", "/api/trades", "/api/strategy"):
    hits = page.lower().count(token)
    print(f"  leak check {token!r}: {hits}")
