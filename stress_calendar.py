"""Stress calendar -> site/data/stress/calendar.json

Events from 14 days ago to ~90 days ahead:
  - quarter/month/year-ends, Japan fiscal year-end       (generated)
  - US tax dates (Apr 15, Jun 15, Sep 15, Dec 15, Jan 15) (generated, business-day adjusted)
  - US mid-month coupon settlement                        (generated)
  - Treasury quarterly refunding announcements            (manual table)
  - US debt-ceiling X-date                                (manual; only if relevant)
  - heavy auction weeks                                   (live, from auctions.json if present)
  - central bank decisions: Fed, ECB, BoC, BoJ, BoE       (manual, published schedules)
  - sovereign rating reviews (EU/UK CRA calendars)        (manual)
  - elections in the 7 countries                          (manual)

Each event: date, type, country, label, severity (low/med/high), source.
"""
import json
import logging
import statistics
from collections import defaultdict
from datetime import date, timedelta

from stress_common import DATA_DIR, dump, now_iso, source

logger = logging.getLogger("stress.calendar")

PAST_DAYS, AHEAD_DAYS = 14, 90

# --------------------------------------------------------------------------
# Manual tables. tentative=True where the date is not officially confirmed.
# --------------------------------------------------------------------------
# Central bank decision dates (announcement day; Fed/BoJ list the 2nd day).
CB_MEETINGS = {
    ("US", "Fed", "FOMC decision", "high", "federalreserve.gov FOMC calendar"): [
        "2026-01-28", "2026-03-18*", "2026-04-29", "2026-06-17*", "2026-07-29",
        "2026-09-16*", "2026-10-28", "2026-12-09*",
        "2027-01-27", "2027-03-17*", "2027-04-28", "2027-06-09*", "2027-07-28",
        "2027-09-22*", "2027-11-03", "2027-12-15*"],
    ("EA", "ECB", "ECB monetary policy decision", "high", "ecb.europa.eu meeting calendar"): [
        "2026-02-05", "2026-03-19", "2026-04-30", "2026-06-11", "2026-07-23",
        "2026-09-10", "2026-10-29", "2026-12-17"],
    ("CA", "BoC", "Bank of Canada rate announcement", "med", "bankofcanada.ca schedule"): [
        "2026-01-28", "2026-03-18", "2026-04-29", "2026-06-10", "2026-07-15",
        "2026-09-02", "2026-10-28", "2026-12-09"],
    ("JP", "BoJ", "BoJ monetary policy meeting (decision)", "high", "boj.or.jp MPM schedule"): [
        "2026-01-23", "2026-03-19", "2026-04-28", "2026-06-16", "2026-07-31",
        "2026-09-18", "2026-10-30", "2026-12-18"],
    ("UK", "BoE", "BoE MPC announcement", "med", "bankofengland.co.uk MPC dates"): [
        "2026-02-05", "2026-03-19", "2026-04-30", "2026-06-18", "2026-07-30",
        "2026-09-17", "2026-11-05", "2026-12-17"],
}
CB_TENTATIVE_FROM = {"Fed": "2027-01-01", "ECB": "2027-01-01", "BoC": "2027-01-01",
                     "BoJ": "2027-01-01", "BoE": "2027-01-01"}

# Treasury quarterly refunding: (date, label, tentative)
REFUNDING = [
    ("2026-11-02", "Treasury financing estimates (marketable borrowing, Q4/Q1)", True),
    ("2026-11-04", "Treasury quarterly refunding statement (coupon sizes, buybacks)", True),
    ("2027-02-01", "Treasury financing estimates (marketable borrowing, Q1/Q2)", True),
    ("2027-02-03", "Treasury quarterly refunding statement (coupon sizes, buybacks)", True),
]

# US debt ceiling: OBBBA (Jul 2025) raised the limit by $5tn; no X-date
# expected inside the calendar horizon. Set x_date to emit an event.
DEBT_CEILING = {
    "relevant": False, "x_date": None, "as_of": "2026-10-09", "source": "manual",
    "note": "Limit raised $5tn by the One Big Beautiful Bill Act (Jul 2025); "
            "binding date not expected before 2027. Update if CBO/Treasury publish an X-date.",
}

# Sovereign rating reviews: (date, country, agency)
RATING_REVIEWS = []
RATING_REVIEWS_NOTE = "EU/UK CRA regulation calendars; dates hand-entered"

# Elections: (date, country, label, severity, tentative)
ELECTIONS = [
    ("2026-11-03", "US", "US midterm elections (House, 1/3 Senate)", "high", False),
]

TAX_DATES = [  # (month, day, label, severity)
    (1, 15, "US Q4 individual estimated tax payments", "low"),
    (4, 15, "US individual tax day (TGA inflow, reserve drain)", "high"),
    (6, 15, "US Q2 corporate & estimated tax payments", "med"),
    (9, 15, "US Q3 corporate & estimated tax payments", "med"),
    (12, 15, "US Q4 corporate estimated tax payments", "med"),
]

US_HOLIDAYS = {  # federal holidays affecting tax-date / settlement roll
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-05-25", "2026-06-19", "2026-07-03",
    "2026-09-07", "2026-10-12", "2026-11-11", "2026-11-26", "2026-12-25",
    "2027-01-01", "2027-01-18", "2027-02-15", "2027-05-31", "2027-06-18", "2027-07-05",
    "2027-09-06", "2027-10-11", "2027-11-11", "2027-11-25", "2027-12-24",
}


def _ev(d, typ, country, label, severity, src, **extra) -> dict:
    e = {"date": d if isinstance(d, str) else d.isoformat(), "type": typ, "country": country,
         "label": label, "severity": severity, "source": src}
    e.update({k: v for k, v in extra.items() if v is not None})
    return e


def _next_bday(d: date) -> date:
    while d.weekday() >= 5 or d.isoformat() in US_HOLIDAYS:
        d += timedelta(days=1)
    return d


def _months(start: date, end: date):
    y, m = start.year, start.month
    while date(y, m, 1) <= end:
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def _month_end(y: int, m: int) -> date:
    nxt = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)
    return nxt - timedelta(days=1)


# --------------------------------------------------------------------------
def generated_events(start: date, end: date) -> list:
    out = []
    for y, m in _months(start - timedelta(days=31), end + timedelta(days=31)):
        me = _month_end(y, m)
        if m == 12:
            out.append(_ev(me, "period_end", "GLOBAL", "Year-end (balance-sheet window dressing, "
                           "G-SIB scores, repo funding pressure)", "high", "calendar"))
        elif m in (3, 6, 9):
            out.append(_ev(me, "period_end", "GLOBAL", f"Quarter-end Q{m // 3} (repo/SRF usage, "
                           "bank balance-sheet constraints)", "med", "calendar"))
        else:
            out.append(_ev(me, "period_end", "GLOBAL", "Month-end (coupon settlement, index extension)",
                           "low", "calendar"))
        if m == 3:
            out.append(_ev(me, "period_end", "JP", "Japan fiscal year-end (JGB/repatriation flows)",
                           "med", "calendar"))
        for mm, dd, label, sev in TAX_DATES:
            if mm == m:
                d = _next_bday(date(y, mm, dd))
                out.append(_ev(d, "tax_date", "US", label, sev, "IRS calendar (business-day adjusted)"))
        mid = _next_bday(date(y, m, 15))
        out.append(_ev(mid, "settlement", "US", "Mid-month Treasury coupon settlement", "low",
                       "Treasury auction cycle"))
    return out


def manual_events() -> list:
    out = []
    for (cc, bank, label, sev, src), dates in CB_MEETINGS.items():
        for raw in dates:
            sep = raw.endswith("*")
            d = raw.rstrip("*")
            lab = label + (" + SEP/dot plot" if sep and bank == "Fed" else "")
            out.append(_ev(d, "central_bank", cc, lab, sev, src, bank=bank,
                           tentative=True if d >= CB_TENTATIVE_FROM[bank] else None))
    for d, label, tent in REFUNDING:
        out.append(_ev(d, "refunding", "US", label, "med", "US Treasury (manual)",
                       tentative=tent or None))
    if DEBT_CEILING.get("relevant") and DEBT_CEILING.get("x_date"):
        out.append(_ev(DEBT_CEILING["x_date"], "debt_ceiling", "US", "Projected debt-limit X-date",
                       "high", DEBT_CEILING["source"], note=DEBT_CEILING["note"]))
    for row in RATING_REVIEWS:
        d, cc, agency = row[:3]
        sev = row[3] if len(row) > 3 else "med"
        out.append(_ev(d, "rating_review", cc, f"{agency} scheduled sovereign rating review",
                       sev, "agency calendar (manual)", agency=agency,
                       note="Usually published after the close (Friday evening CET)"))
    for d, cc, label, sev, tent in ELECTIONS:
        out.append(_ev(d, "election", cc, label, sev, "manual", tentative=tent or None))
    return out


# --------------------------------------------------------------------------
# Heavy auction weeks from the parallel auctions collector (defensive)
# --------------------------------------------------------------------------
def _rec_date(r: dict):
    for k in ("date", "auction_date", "auctionDate"):
        v = r.get(k)
        if isinstance(v, str) and len(v) >= 10:
            try:
                return date.fromisoformat(v[:10])
            except ValueError:
                pass
    return None


def _rec_size(r: dict):
    for k in ("size_usd", "allotted", "offered", "size_local", "size"):
        v = r.get(k)
        if isinstance(v, (int, float)) and v > 0:
            return float(v) if k == "size_usd" or r.get("currency", "USD") == "USD" else None
    return None


def _week(d: date) -> date:
    return d - timedelta(days=d.weekday())


def auction_weeks(start: date, end: date) -> tuple[list, list, dict]:
    path = DATA_DIR / "auctions.json"
    if not path.exists():
        return [], [], source("auctions.json (heavy weeks)", "stub", None,
                              "auctions.json not present yet; heavy-week flags skipped")
    try:
        d = json.loads(path.read_text())
        upcoming = d.get("upcoming") if isinstance(d, dict) else None
        recent = d.get("recent") if isinstance(d, dict) else None
        if not isinstance(upcoming, list) or not upcoming:
            return [], [], source("auctions.json (heavy weeks)", "stub", d.get("generated_at"),
                                  "no 'upcoming' list")
        recent = recent if isinstance(recent, list) else []
        # Last known size per (country, tenor) to impute not-yet-announced sizes
        last_size = {}
        for r in sorted((r for r in recent if isinstance(r, dict) and _rec_date(r)), key=_rec_date):
            sz = _rec_size(r)
            if sz:
                last_size[(r.get("country"), r.get("tenor"))] = sz
        imputed, missing = defaultdict(int), defaultdict(int)

        def weekly(rows, impute=False):
            tot, cpn, n = defaultdict(float), defaultdict(float), defaultdict(int)
            for r in rows:
                if not isinstance(r, dict):
                    continue
                dt, sz = _rec_date(r), _rec_size(r)
                if dt is None:
                    continue
                if sz is None and impute:
                    sz = last_size.get((r.get("country"), r.get("tenor")))
                    if sz is None:
                        missing[_week(dt)] += 1
                        continue
                    imputed[_week(dt)] += 1
                if sz is None:
                    continue
                w = _week(dt)
                tot[w] += sz
                n[w] += 1
                if str(r.get("instrument", "")).lower() not in ("bill", "bills", "cmb"):
                    cpn[w] += sz
            return tot, cpn, n

        rt, rc, _ = weekly(recent)
        ut, uc, un = weekly(upcoming, impute=True)
        # Baseline: median of completed past weeks (exclude current partial week)
        this_week = _week(date.today())
        past = [w for w in rt if w < this_week]
        med_t = statistics.median([rt[w] for w in past]) if len(past) >= 3 else None
        med_c = statistics.median([rc[w] for w in past if rc[w] > 0]) if len([w for w in past if rc[w] > 0]) >= 3 else None
        weeks, events = [], []
        for w in sorted(ut):
            row = {"week_start": w.isoformat(), "total_usd_bn": round(ut[w] / 1e9, 1),
                   "coupon_usd_bn": round(uc[w] / 1e9, 1), "n_auctions": un[w],
                   "baseline_total_usd_bn": round(med_t / 1e9, 1) if med_t else None,
                   "baseline_coupon_usd_bn": round(med_c / 1e9, 1) if med_c else None}

            rt_ratio = ut[w] / med_t if med_t else None
            rc_ratio = uc[w] / med_c if med_c and uc[w] else None
            row["ratio_total"] = round(rt_ratio, 2) if rt_ratio else None
            row["ratio_coupon"] = round(rc_ratio, 2) if rc_ratio else None
            # a week cut off by the end of the announced list can only be flagged on coupons
            heavy = ((rt_ratio or 0) >= 1.15 and not row["partial"]) or (rc_ratio or 0) >= 1.25
            row["heavy"] = heavy
            weeks.append(row)
            if heavy and start <= w + timedelta(days=4) and w <= end:
                sev = "high" if (rt_ratio or 0) >= 1.4 or (rc_ratio or 0) >= 1.6 else "med"
                events.append(_ev(w, "auction_week", "US" if all(
                    (r.get("country") == "US") for r in upcoming if _rec_date(r) and _week(_rec_date(r)) == w) else "MULTI",
                    f"Heavy auction week: ${row['total_usd_bn']:.0f}bn total "
                    f"(${row['coupon_usd_bn']:.0f}bn coupons) vs ${(med_t or 0) / 1e9:.0f}bn typical"
                    + (f"; {imputed[w]} sizes estimated" if imputed[w] else ""),
                    sev, "auctions.json (stress_auctions.py)", end_date=(w + timedelta(days=4)).isoformat()))
        return events, weeks, source("auctions.json (heavy weeks)", "ok", d.get("generated_at"),
                                     f"{len(upcoming)} upcoming, baseline from {len(past)} past weeks")
    except Exception as e:  # noqa: BLE001
        logger.warning("auctions.json parse failed: %s", e)
        return [], [], source("auctions.json (heavy weeks)", "error", None, str(e)[:200])


def structural_auction_weeks(start: date, end: date) -> list:
    """US coupon-cycle pattern, always emitted (low severity) so the calendar
    is useful even before auctions.json announces sizes: refunding week
    (3y/10y/30y, 2nd week of Feb/May/Aug/Nov) and the late-month 2y/5y/7y week."""
    out = []
    for y, m in _months(start, end + timedelta(days=31)):
        if m in (2, 5, 8, 11):
            first = date(y, m, 1)
            # refunding auctions: Tue-Thu of the week containing the 2nd Tuesday-ish (~8th-14th)
            wk = _week(first + timedelta(days=7))
            out.append(_ev(wk, "auction_week", "US", "US refunding auctions (3y/10y/30y)", "med",
                           "Treasury auction pattern (structural)", end_date=(wk + timedelta(days=4)).isoformat(),
                           tentative=True))
        me = _month_end(y, m)
        wk = _week(me - timedelta(days=6))
        out.append(_ev(wk, "auction_week", "US", "US 2y/5y/7y coupon auctions + month-end", "low",
                       "Treasury auction pattern (structural)", end_date=(wk + timedelta(days=4)).isoformat(),
                       tentative=True))
    return out


# --------------------------------------------------------------------------
def build_calendar() -> dict:
    today = date.today()
    start, end = today - timedelta(days=PAST_DAYS), today + timedelta(days=AHEAD_DAYS)
    events, sources = [], []
    for name, fn in (("Generated (period-ends, tax, settlement)", lambda: generated_events(start, end)),
                     ("Manual tables (CB, refunding, ratings, elections)", manual_events),
                     ("US auction-cycle pattern", lambda: structural_auction_weeks(start, end))):
        try:
            evs = fn()
            events += evs
            sources.append(source(name, "ok", today.isoformat(), f"{len(evs)} events before window filter"))
        except Exception as e:  # noqa: BLE001
            logger.exception("%s failed", name)
            sources.append(source(name, "error", None, str(e)[:200]))
    heavy_events, heavy_weeks, s = auction_weeks(start, end)
    sources.append(s)
    events += heavy_events

    def in_window(e):
        d0 = date.fromisoformat(e["date"])
        d1 = date.fromisoformat(e.get("end_date", e["date"]))
        return d1 >= start and d0 <= end
    sev_rank = {"high": 0, "med": 1, "low": 2}
    events = sorted((e for e in events if in_window(e)),
                    key=lambda e: (e["date"], sev_rank.get(e["severity"], 3), e["type"]))
    # de-duplicate exact repeats
    seen, uniq = set(), []
    for e in events:
        k = (e["date"], e["type"], e["country"], e["label"])
        if k not in seen:
            seen.add(k)
            uniq.append(e)
    for e in uniq:
        e["past"] = e["date"] < today.isoformat()

    # weekly stress score: high=3, med=2, low=1
    score = defaultdict(int)
    for e in uniq:
        if not e["past"]:
            score[_week(date.fromisoformat(e["date"])).isoformat()] += 4 - (sev_rank[e["severity"]] + 1)
    sources += [source("Central bank schedules", "ok", "2026-10-09", "manual table in stress_calendar.py"),
                source("Rating review calendars", "ok", "2026-10-09", RATING_REVIEWS_NOTE),
                source("Elections", "ok", "2026-10-09", "manual table"),
                source("Debt ceiling", "ok", DEBT_CEILING["as_of"], DEBT_CEILING["note"])]
    return {
        "generated_at": now_iso(), "sources": sources,
        "window": {"start": start.isoformat(), "end": end.isoformat(), "today": today.isoformat()},
        "events": uniq,
        "auction_weeks": heavy_weeks,
        "week_scores": [{"week_start": w, "score": s} for w, s in sorted(score.items())],
        "debt_ceiling": DEBT_CEILING,
        "types": ["period_end", "tax_date", "settlement", "refunding", "debt_ceiling", "auction_week",
                  "central_bank", "rating_review", "election"],
    }


if __name__ == "__main__":
    import time
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    t0 = time.time()
    p = build_calendar()
    dump("calendar.json", p)
    for e in p["events"]:
        print(e["date"], e["severity"].ljust(4), e["type"].ljust(13), e["country"].ljust(6), e["label"],
              "(tentative)" if e.get("tentative") else "")
    print("auction weeks:", p["auction_weeks"])
    print(f"{len(p['events'])} events, {sum(s['status'] == 'error' for s in p['sources'])} source errors, "
          f"{time.time() - t0:.2f}s")
