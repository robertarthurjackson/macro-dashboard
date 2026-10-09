"""Global sovereign auction monitor -> site/data/stress/auctions.json

Countries: US, CA, DE, FR, IT, JP, UK (+ "EZ" aggregate view of DE/FR/IT).

Schema (top level):
  generated_at   ISO UTC timestamp
  fx_usd         {ccy: {"usd_per_unit": float, "as_of": date}} used for size_usd
  upcoming       [ {country, instrument, tenor, security, date, size_local,
                    currency, size_usd, size_note, source} ]  next ~14 days
  recent         [ auction record ]  last ~42 days, newest first
  history        {"<country>|<tenor>": [[date, btc, tail_bp, yield], ...]}
                 compact per-(country, tenor) series, oldest first, ~2y
  eurozone       {recent: [records for DE/FR/IT], summary: {country: {n,
                    avg_btc, avg_z_btc, strong, neutral, weak}}}
  tail_method    text: how tail_bp is computed (per-country proxy series)
  rating_method  text: how rating strong/neutral/weak is computed
  sources        [ {name, status ok|error|stub, as_of, note} ]

Auction record:
  country, instrument (Bill|Note|Bond|Linker|FRN|...), tenor ("10Y",
  "3M bill", "10Y TIPS"...), tenor_years, security (name), id (ISIN/CUSIP),
  date, currency, offered, allotted, retention (DE only, Bund retained by the
  Finanzagentur for secondary market ops), tendered, size_usd, btc
  (bid-to-cover), yield (clearing: US high yield, JP/DE/FR/IT avg or
  marginal - see yield_type), yield_type, prev_yield, yield_chg_bp (vs the
  previous auction of the same country+tenor), pre_yield (proxy pre-auction
  level), tail_bp (yield - pre_yield, bp; positive = tailed/weak), buyers
  ({indirect_pct, direct_pct, dealer_pct} for US), rating, z_btc, z_tail,
  n_hist, source.

History is accumulated in seed/stress/auctions_history.json (merged by
"country|date|id|tenor") because several sources only expose recent results.
"""
from __future__ import annotations

import io
import logging
import math
import re
import statistics
import struct
import time
import warnings
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta

import pandas as pd
import requests

from stress_common import (UA, dump, fred_series, http_get, load_seed, now_iso,
                           save_seed, source)

logger = logging.getLogger("stress.auctions")

HIST_SEED = "auctions_history.json"
TODAY = date.today()
RECENT_DAYS = 42
UPCOMING_DAYS = 14
HIST_YEARS = 2.2
RATING_MIN_N = 6

TAIL_METHOD = (
    "tail_bp = auction clearing yield minus the prior business day's close of "
    "the same country's benchmark yield at the nearest tenor (proxy for the "
    "pre-auction/when-issued level, which is not freely available). Proxies: "
    "US FRED DGS*/DFII* (constant-maturity); others listed per source note. "
    "Intraday moves on auction day add noise; ratings use z-scores vs the "
    "same proxy's own history, so systematic bias largely cancels. null = no "
    "free daily yield source."
)
RATING_METHOD = (
    "For each auction, compare against that country's own auctions of the "
    "same tenor over the trailing ~2y (prior auctions only, min 6). "
    "z_btc = (btc - mean)/sd, z_tail = (tail - mean)/sd. score = mean of "
    "available [z_btc, -z_tail]. strong if score >= +0.6, weak if <= -0.6, "
    "else neutral; null if < 6 comparable auctions."
)


# ----------------------------------------------------------------- helpers
def fnum(x):
    try:
        if x is None:
            return None
        if isinstance(x, str):
            x = x.strip().replace(",", "")
            if x in ("", "null", "-", "n/a", "N/A", "NaN"):
                return None
        v = float(x)
        return None if math.isnan(v) else v
    except (TypeError, ValueError):
        return None


def iso(d) -> str | None:
    if d is None:
        return None
    if isinstance(d, str):
        d = pd.to_datetime(d, errors="coerce", dayfirst=False)
        if pd.isna(d):
            return None
    return pd.Timestamp(d).strftime("%Y-%m-%d")


def rec_key(r: dict) -> str:
    return f"{r['country']}|{r['date']}|{r.get('id') or ''}|{r['tenor']}"


def tenor_label(years: float, bill: bool = False) -> str:
    if bill or years < 1:
        m = round(years * 12)
        if m <= 0:
            return f"{round(years * 52)}W bill"
        return f"{m}M bill"
    if years >= 1 and abs(years - round(years)) < 0.01:
        return f"{int(round(years))}Y"
    return f"{years:g}Y"


def prior_close(series: pd.Series | None, d: str):
    """Last observation strictly before date d."""
    if series is None or series.empty:
        return None
    s = series[series.index < pd.Timestamp(d)]
    if s.empty or (pd.Timestamp(d) - s.index[-1]).days > 7:
        return None
    return float(s.iloc[-1])


def nearest_curve(curve: dict, years: float | None):
    """curve: {tenor_years: Series}. Pick nearest tenor (log distance)."""
    if not curve or not years:
        return None, None
    k = min(curve, key=lambda t: abs(math.log(t) - math.log(max(years, 0.02))))
    return k, curve[k]


# --------------------------------------------------------------------- FX
FX_FRED = {"CAD": ("DEXCAUS", True), "JPY": ("DEXJPUS", True),
           "GBP": ("DEXUSUK", False), "EUR": ("DEXUSEU", False)}


def get_fx() -> dict:
    out = {"USD": {"usd_per_unit": 1.0, "as_of": iso(TODAY)}}
    for ccy, (sid, inverse) in FX_FRED.items():
        try:
            s = fred_series(sid, iso(TODAY - timedelta(days=30)))
            v = float(s.iloc[-1])
            out[ccy] = {"usd_per_unit": round(1 / v if inverse else v, 6),
                        "as_of": iso(s.index[-1]), "source": f"FRED {sid}"}
        except Exception as e:  # noqa: BLE001
            logger.warning("fx %s: %s", ccy, e)
    return out


def to_usd(amount, ccy, fx):
    if amount is None or ccy not in fx:
        return None
    return round(amount * fx[ccy]["usd_per_unit"])


# ===================================================================== US
FISCAL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
US_CURVE = {1 / 12: "DGS1MO", 0.25: "DGS3MO", 0.5: "DGS6MO", 1: "DGS1",
            2: "DGS2", 3: "DGS3", 5: "DGS5", 7: "DGS7", 10: "DGS10",
            20: "DGS20", 30: "DGS30"}
US_TIPS = {5: "DFII5", 10: "DFII10", 30: "DFII30"}


def _us_years(term: str) -> float | None:
    term = term or ""
    m = re.match(r"(\d+)-Week", term)
    if m:
        return int(m.group(1)) / 52
    m = re.match(r"(\d+)-Day", term)
    if m:
        return int(m.group(1)) / 365
    y = re.search(r"(\d+)-Year", term)
    mo = re.search(r"(\d+)-Month", term)
    if y or mo:
        return (int(y.group(1)) if y else 0) + (int(mo.group(1)) / 12 if mo else 0)
    return None


def _fred_curve(spec: dict, start: str) -> dict:
    out = {}
    def get(item):
        t, sid = item
        try:
            return t, fred_series(sid, start)
        except Exception as e:  # noqa: BLE001
            logger.warning("fred %s: %s", sid, e)
            return t, None
    with ThreadPoolExecutor(6) as ex:
        for t, s in ex.map(get, spec.items()):
            if s is not None and not s.empty:
                out[t] = s
    return out


def collect_us(fx) -> dict:
    start = iso(TODAY - timedelta(days=int(365 * HIST_YEARS)))
    fields = ("auction_date,security_type,security_term,original_security_term,"
              "cusip,reopening,offering_amt,total_accepted,total_tendered,"
              "comp_accepted,comp_tendered,bid_to_cover_ratio,high_yield,"
              "high_investment_rate,high_discnt_margin,indirect_bidder_accepted,"
              "direct_bidder_accepted,primary_dealer_accepted,"
              "inflation_index_security,floating_rate,cash_management_bill_cmb")
    r = http_get(f"{FISCAL}/v1/accounting/od/auctions_query",
                 params={"filter": f"auction_date:gte:{start}", "fields": fields,
                         "page[size]": 10000, "sort": "-auction_date"}, timeout=60)
    rows = r.json()["data"]
    curve = _fred_curve(US_CURVE, start)
    tips = _fred_curve(US_TIPS, start)

    recs, upcoming = [], []
    for x in rows:
        if x.get("cash_management_bill_cmb") == "Yes":
            continue
        typ = x["security_type"]
        orig = x.get("original_security_term") or x["security_term"]
        if x.get("reopening") == "Yes" and typ != "Bill":
            term = orig
        else:
            term = x["security_term"]
        yrs = _us_years(term)
        if yrs is None:
            continue
        tips_flag = x.get("inflation_index_security") == "Yes"
        frn = x.get("floating_rate") == "Yes"
        if typ == "Bill":
            weeks = re.match(r"(\d+)", term)
            tenor = f"{int(weeks.group(1))}W bill" if "Week" in term and weeks else None
            if tenor is None:  # odd-dated bills (n-Day): skip from history
                continue
            instrument = "Bill"
            yld = fnum(x.get("high_investment_rate"))
        else:
            tenor = tenor_label(round(yrs))
            instrument = "TIPS" if tips_flag else ("FRN" if frn else typ)
            if tips_flag:
                tenor += " TIPS"
            if frn:
                tenor += " FRN"
            yld = None if frn else fnum(x.get("high_yield"))
        btc = fnum(x.get("bid_to_cover_ratio"))
        d = x["auction_date"]
        offered = fnum(x.get("offering_amt"))
        if btc is None:  # not yet auctioned -> upcoming
            if d >= iso(TODAY):
                upcoming.append(dict(country="US", instrument=instrument, tenor=tenor,
                                     security=f"{x['security_term']} {typ}" + (" (reopening)" if x.get("reopening") == "Yes" else ""),
                                     id=x["cusip"], date=d, size_local=offered, currency="USD",
                                     size_usd=offered, source="US Treasury FiscalData auctions_query"))
            continue
        comp = fnum(x.get("comp_accepted"))
        buyers = None
        if comp:
            ind, dirc, pd_ = (fnum(x.get(k)) for k in ("indirect_bidder_accepted",
                              "direct_bidder_accepted", "primary_dealer_accepted"))
            buyers = {"indirect_pct": round(100 * ind / comp, 1) if ind is not None else None,
                      "direct_pct": round(100 * dirc / comp, 1) if dirc is not None else None,
                      "dealer_pct": round(100 * pd_ / comp, 1) if pd_ is not None else None}
        pre = None
        proxy = None
        if yld is not None:
            src = tips if tips_flag else curve
            k, s = nearest_curve(src, round(yrs) if typ != "Bill" else yrs)
            pre = prior_close(s, d)
            proxy = (US_TIPS if tips_flag else US_CURVE).get(k)
        recs.append(dict(
            country="US", instrument=instrument, tenor=tenor, tenor_years=round(yrs, 3),
            security=f"{x['security_term']} {typ}" + (" (reopening)" if x.get("reopening") == "Yes" else ""),
            id=x["cusip"], date=d, currency="USD", offered=offered,
            allotted=fnum(x.get("total_accepted")), tendered=fnum(x.get("total_tendered")),
            retention=None, btc=btc, yield_=yld,
            yield_type="high investment rate" if typ == "Bill" else ("high discount margin" if frn else "high yield"),
            pre_yield=pre, pre_yield_series=f"FRED {proxy}" if proxy else None,
            buyers=buyers, source="US Treasury FiscalData auctions_query"))
    # upcoming_auctions endpoint covers announced-later auctions w/o CUSIP size
    try:
        r2 = http_get(f"{FISCAL}/v1/accounting/od/upcoming_auctions",
                      params={"page[size]": 200, "sort": "auction_date"})
        seen = {(u["id"], u["date"]) for u in upcoming}
        for x in r2.json()["data"]:
            if (x["cusip"], x["auction_date"]) in seen or x["auction_date"] < iso(TODAY):
                continue
            yrs = _us_years(x["security_term"]) or 0
            typ = x["security_type"]
            tenor = (x["security_term"].replace("-Week", "W bill") if typ == "Bill"
                     else tenor_label(round(yrs)) if yrs else x["security_term"])
            off = fnum(x.get("offering_amt"))
            upcoming.append(dict(country="US", instrument=typ, tenor=tenor,
                                 security=f"{x['security_term']} {typ}" + (" (reopening)" if x.get("reopening") == "Yes" else ""),
                                 id=x["cusip"], date=x["auction_date"], size_local=off, currency="USD",
                                 size_usd=off, size_note=None if off else "size announced ~1 week before",
                                 source="US Treasury FiscalData upcoming_auctions"))
    except Exception as e:  # noqa: BLE001
        logger.warning("us upcoming: %s", e)
    last = max((r["date"] for r in recs), default=None)
    return {"records": recs, "upcoming": upcoming,
            "source": source("US Treasury (FiscalData)", "ok", last,
                             f"{len(recs)} auctions since {start}; tail proxy FRED DGS*/DFII* prior close")}


# ============================================================ shared (non-US)
def _rec(**kw) -> dict:
    """Auction record with the full schema (missing fields -> None)."""
    base = dict(country=None, instrument=None, tenor=None, tenor_years=None,
                security=None, id=None, date=None, currency=None, offered=None,
                allotted=None, tendered=None, retention=None, btc=None, yield_=None,
                yield_type=None, pre_yield=None, pre_yield_series=None,
                auction_tail_bp=None, buyers=None, source=None)
    base.update(kw)
    return base


def _ratio(a, b):
    return round(a / b, 3) if a is not None and b else None


def _mul(x, k):
    return None if x is None else round(x * k)


def _xl_date(v) -> str | None:
    """Excel serial / datetime / 'dd-mm-yyyy' / 'dd.mm.yyyy' -> ISO date."""
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        if math.isnan(v):
            return None
        return iso(datetime(1899, 12, 30) + timedelta(days=int(v)))
    if isinstance(v, (datetime, date, pd.Timestamp)):
        return iso(v)
    s = str(v).strip()
    m = re.match(r"(\d{1,2})[./-](\d{1,2})[./-](\d{4})$", s)
    if m:
        return f"{m.group(3)}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"
    return iso(s)


def _pre(curve: dict, years, d, label_fn, min_years=0.0):
    """(pre_yield, series label) from the nearest-tenor proxy curve."""
    if not curve or not years or years < min_years:
        return None, None
    k, s = nearest_curve(curve, years)
    v = prior_close(s, d)
    return (round(v, 4), label_fn(k)) if v is not None else (None, None)


def _ole_stream(data: bytes, names=("Workbook", "Book")) -> bytes:
    """Minimal OLE2 compound-file reader: return the workbook stream."""
    if data[:8] != b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
        raise ValueError("not an OLE2 file")
    ssz = 1 << struct.unpack_from("<H", data, 0x1E)[0]
    n_fat, dir_start = struct.unpack_from("<II", data, 0x2C)
    cutoff, = struct.unpack_from("<I", data, 0x38)
    difat_start, n_difat = struct.unpack_from("<II", data, 0x44)

    def sec(i):
        return data[512 + i * ssz: 512 + (i + 1) * ssz]
    difat = list(struct.unpack_from("<109I", data, 0x4C))
    s = difat_start
    for _ in range(n_difat):
        blk = struct.unpack(f"<{ssz // 4}I", sec(s))
        difat += blk[:-1]
        s = blk[-1]
    fat = []
    for i in difat[:n_fat]:
        fat += struct.unpack(f"<{ssz // 4}I", sec(i))

    def chain(start):
        out, s, seen = [], start, set()
        while s < 0xFFFFFFFA and s not in seen:
            seen.add(s)
            out.append(sec(s))
            s = fat[s]
        return b"".join(out)

    d = chain(dir_start)
    for off in range(0, len(d), 128):
        e = d[off:off + 128]
        nlen = struct.unpack_from("<H", e, 0x40)[0]
        if e[:max(nlen - 2, 0)].decode("utf-16-le", "ignore") in names:
            start, size = struct.unpack_from("<II", e, 0x74)
            if size < cutoff:
                raise ValueError("workbook in mini-stream not supported")
            return chain(start)[:size]
    raise ValueError("no Workbook stream")


def read_xls(data: bytes) -> dict[str, list[list]]:
    """Minimal BIFF8 (.xls) reader -> {sheet: rows}. Used because xlrd is not a
    dependency; handles SST/LABELSST, LABEL, NUMBER, RK, MULRK, FORMULA."""
    wb = _ole_stream(data)
    recs, p = [], 0
    while p + 4 <= len(wb):
        typ, ln = struct.unpack_from("<HH", wb, p)
        recs.append((typ, wb[p + 4:p + 4 + ln]))
        p += 4 + ln

    def rk(v):
        if v & 2:
            x = float((v >> 2) - (1 << 30) if v & 0x80000000 else v >> 2)
        else:
            x = struct.unpack("<d", struct.pack("<Q", (v & 0xFFFFFFFC) << 32))[0]
        return x / 100 if v & 1 else x

    def ustr(b, off, cch, flg):
        n = cch * (2 if flg & 1 else 1)
        return b[off:off + n].decode("utf-16-le" if flg & 1 else "latin-1")

    sst, sheets, names, cells, pend, i = [], [], [], None, None, 0
    while i < len(recs):
        typ, b = recs[i]
        if typ == 0x0809:  # BOF: first is workbook globals, then one per sheet
            cells = {} if names else None
            if cells is not None:
                sheets.append(cells)
        elif typ == 0x0085:  # BOUNDSHEET
            names.append(ustr(b, 8, b[6], b[7]))
        elif typ == 0x00FC:  # SST (+ CONTINUE fragments)
            frags = [b[8:]]
            n = struct.unpack_from("<I", b, 4)[0]
            while i + 1 < len(recs) and recs[i + 1][0] == 0x003C:
                i += 1
                frags.append(recs[i][1])
            st = {"f": 0, "p": 0}

            def take(k):
                out = b""
                while k > 0:
                    if st["p"] >= len(frags[st["f"]]):
                        st["f"], st["p"] = st["f"] + 1, 0
                    c = frags[st["f"]][st["p"]:st["p"] + k]
                    out += c
                    st["p"] += len(c)
                    k -= len(c)
                return out

            try:
                for _ in range(n):
                    cch, = struct.unpack("<H", take(2))
                    flg = take(1)[0]
                    runs = struct.unpack("<H", take(2))[0] if flg & 8 else 0
                    ext = struct.unpack("<I", take(4))[0] if flg & 4 else 0
                    chars, wide, left = [], flg & 1, cch
                    while left > 0:
                        fr = frags[st["f"]]
                        if st["p"] >= len(fr):  # CONTINUE: fresh option byte
                            st["f"], st["p"] = st["f"] + 1, 0
                            fr = frags[st["f"]]
                            wide = fr[0] & 1
                            st["p"] = 1
                        w = 2 if wide else 1
                        k = min(left, (len(fr) - st["p"]) // w)
                        raw = fr[st["p"]:st["p"] + k * w]
                        st["p"] += len(raw)
                        chars.append(raw.decode("utf-16-le" if wide else "latin-1"))
                        left -= k
                    take(4 * runs + ext)
                    sst.append("".join(chars))
            except (IndexError, struct.error):
                pass
        elif cells is not None:
            if typ == 0x00FD:  # LABELSST
                r, c, _, k = struct.unpack_from("<HHHI", b)
                cells[(r, c)] = sst[k] if k < len(sst) else None
            elif typ == 0x0203:  # NUMBER
                r, c, _, v = struct.unpack_from("<HHHd", b)
                cells[(r, c)] = v
            elif typ == 0x027E:  # RK
                r, c, _, v = struct.unpack_from("<HHHI", b)
                cells[(r, c)] = rk(v)
            elif typ == 0x00BD:  # MULRK
                r, c0 = struct.unpack_from("<HH", b)
                for j in range((len(b) - 6) // 6):
                    cells[(r, c0 + j)] = rk(struct.unpack_from("<I", b, 6 + j * 6)[0])
            elif typ == 0x0204:  # LABEL
                r, c, _, cch, flg = struct.unpack_from("<HHHHB", b)
                cells[(r, c)] = ustr(b, 9, cch, flg)
            elif typ == 0x0006:  # FORMULA (cached result)
                r, c = struct.unpack_from("<HH", b)
                res = b[6:14]
                if res[6:8] == b"\xff\xff":
                    cells[(r, c)] = None
                    pend = (r, c) if res[0] == 0 else None
                else:
                    cells[(r, c)] = struct.unpack("<d", res)[0]
            elif typ == 0x0207 and pend:  # STRING (formula result)
                cch, flg = struct.unpack_from("<HB", b)
                cells[pend] = ustr(b, 3, cch, flg)
                pend = None
        i += 1
    out = {}
    for k, cl in enumerate(sheets):
        name = (names[k] if k < len(names) else f"Sheet{k + 1}").strip()
        rows = []
        if cl:
            nr, nc = max(r for r, _ in cl) + 1, max(c for _, c in cl) + 1
            rows = [[None] * nc for _ in range(nr)]
            for (r, c), v in cl.items():
                rows[r][c] = v
        out[name] = rows
    return out


# ===================================================================== CA
VALET = "https://www.bankofcanada.ca/valet"
CA_CURVE = {0.25: "TB.CDN.90D.MID", 0.5: "TB.CDN.180D.MID", 1: "TB.CDN.1Y.MID",
            2: "BD.CDN.2YR.DQ.YLD", 3: "BD.CDN.3YR.DQ.YLD", 5: "BD.CDN.5YR.DQ.YLD",
            7: "BD.CDN.7YR.DQ.YLD", 10: "BD.CDN.10YR.DQ.YLD", 30: "BD.CDN.LONG.DQ.YLD"}
CA_RRB = {30: "BD.CDN.RRB.DQ.YLD"}


def _valet_group(g: str) -> list[dict]:
    obs = http_get(f"{VALET}/observations/group/{g}/json", timeout=40).json()["observations"]
    pre = g.rsplit("_RESULTS", 1)[0].rsplit("_UPCOMING", 1)[0] + "_"
    return [{k[len(pre):] if k.startswith(pre) else k: (v.get("v") if isinstance(v, dict) else v)
             for k, v in o.items()} for o in obs]


def _valet_curve(spec: dict, start: str) -> dict:
    ids = ",".join(spec.values())
    obs = http_get(f"{VALET}/observations/{ids}/json", params={"start_date": start},
                   timeout=40).json()["observations"]
    out = {}
    for t, sid in spec.items():
        s = pd.Series({o["d"]: fnum(o.get(sid, {}).get("v")) for o in obs if sid in o}).dropna()
        if not s.empty:
            s.index = pd.to_datetime(s.index)
            out[t] = s.sort_index()
    return out


def _ca_bill_tenor(days: float) -> str:
    return ("1M bill" if days < 45 else "3M bill" if days < 130 else
            "6M bill" if days < 250 else "12M bill")


def collect_ca(fx) -> dict:
    start = iso(TODAY - timedelta(days=int(365 * HIST_YEARS) + 30))
    groups = {}
    with ThreadPoolExecutor(6) as ex:
        futs = {g: ex.submit(_valet_group, g) for g in
                ("AUC_BOND_RESULTS", "AUC_TBILL_RESULTS", "AUC_BOND_RR_RESULTS",
                 "AUC_BOND_U_RESULTS", "AUC_BOND_UPCOMING_DETAILS",
                 "AUC_TBILL_UPCOMING_DETAILS")}
        fcurve = ex.submit(_valet_curve, CA_CURVE, start)
        frrb = ex.submit(_valet_curve, CA_RRB, start)
        for g, f in futs.items():
            try:
                groups[g] = f.result()
            except Exception as e:  # noqa: BLE001
                logger.warning("ca %s: %s", g, e)
                groups[g] = []
        try:
            curve = fcurve.result()
        except Exception as e:  # noqa: BLE001
            logger.warning("ca curve: %s", e)
            curve = {}
        try:
            rrb = frrb.result()
        except Exception as e:  # noqa: BLE001
            logger.warning("ca rrb curve: %s", e)
            rrb = {}
    if not any(groups.get(g) for g in ("AUC_BOND_RESULTS", "AUC_TBILL_RESULTS")):
        raise RuntimeError("Valet auction groups empty")

    def lab(prefix):
        return lambda k: f"BoC Valet {prefix.get(k)}"
    src = "Bank of Canada Valet (AUC_* groups)"
    recs, upcoming = [], []

    def buyers(x):
        d, c, f = (fnum(x.get(k)) for k in ("PERCENT_ALLOTTED_TO_DISTRIBUTORS",
                   "PERCENT_ALLOTTED_TO_CUSTOMERS", "PERCENT_ALLOTTED_TO_FOREIGN"))
        if d is None and c is None and f is None:
            return None
        return {"dealer_pct": d, "customer_pct": c, "foreign_pct": f}

    # nominal + ultra-long + real-return bonds
    for g, kind in (("AUC_BOND_RESULTS", "Bond"), ("AUC_BOND_U_RESULTS", "Ultra"),
                    ("AUC_BOND_RR_RESULTS", "RRB")):
        for x in groups.get(g, []):
            d = x.get("AUCTION_DATE")
            if not d or d < start:
                continue
            yrs = fnum(x.get("TERM_YEARS"))
            btc = fnum(x.get("COVERAGE"))
            amt = fnum(x.get("AMOUNT"))
            if kind == "RRB":
                yld = fnum(x.get("ALLOTMENT_YIELD"))
                ytype, tail = "allotment (single-price) real yield", None
            elif kind == "Ultra":
                yld = fnum(x.get("ALLOTMENT_YIELD"))
                ytype, tail = "allotment (single-price) yield", None
            else:
                yld = fnum(x.get("AVG_YIELD"))
                ytype = "average yield"
                hi, av = fnum(x.get("HIGH_YIELD")), fnum(x.get("AVG_YIELD"))
                tail = fnum(x.get("TAIL"))
                if tail is None and hi is not None and av is not None:
                    tail = round((hi - av) * 100, 2)
            if btc is None and yld is None:
                continue
            tenor = tenor_label(round(yrs)) if yrs else "?"
            if kind == "RRB":
                tenor += " RRB"
            pre, ser = (_pre(rrb, 30, d, lab(CA_RRB)) if kind == "RRB"
                        else _pre(curve, yrs, d, lab(CA_CURVE)))
            cpn = x.get("COUPON_RATE")
            mat = x.get("MATURITY_DATE")
            recs.append(_rec(
                country="CA", instrument="Linker" if kind == "RRB" else "Bond",
                tenor=tenor, tenor_years=yrs,
                security=f"Canada {cpn or ''}% {mat or ''}" + (" RRB" if kind == "RRB" else ""),
                id=x.get("ISIN"), date=d, currency="CAD", offered=_mul(amt, 1e6),
                allotted=_mul(amt, 1e6), tendered=_mul(fnum(x.get("TOTAL_SUBMITTED")), 1e6),
                btc=btc, yield_=yld, yield_type=ytype, pre_yield=pre, pre_yield_series=ser,
                auction_tail_bp=tail, buyers=buyers(x), source=src))
    for x in groups.get("AUC_TBILL_RESULTS", []):
        d = x.get("AUCTION_DATE")
        if not d or d < start:
            continue
        days = fnum(x.get("TERM_DAYS"))
        btc, yld = fnum(x.get("COVERAGE")), fnum(x.get("AVG_YIELD"))
        if btc is None and yld is None or not days:
            continue
        yrs = days / 365
        amt = fnum(x.get("AMOUNT"))
        pre, ser = _pre(curve, yrs, d, lab(CA_CURVE))
        recs.append(_rec(
            country="CA", instrument="Bill", tenor=_ca_bill_tenor(days),
            tenor_years=round(yrs, 3), security=f"Canada T-bill {int(days)}d {x.get('MATURITY_DATE') or ''}",
            id=x.get("ISIN"), date=d, currency="CAD", offered=_mul(amt, 1e6),
            allotted=_mul(amt, 1e6), tendered=_mul(fnum(x.get("TOTAL_SUBMITTED")), 1e6),
            btc=btc, yield_=yld, yield_type="average yield", pre_yield=pre,
            pre_yield_series=ser, auction_tail_bp=fnum(x.get("TAIL")),
            buyers=buyers(x), source=src))
    # upcoming (call for tenders published ~1 week ahead, incl. amounts)
    done = {(r["id"], r["date"]) for r in recs}
    for g, bill in (("AUC_BOND_UPCOMING_DETAILS", False), ("AUC_TBILL_UPCOMING_DETAILS", True)):
        for x in groups.get(g, []):
            d = x.get("AUCTION_DATE")
            if not d or d < iso(TODAY) or (x.get("ISIN"), d) in done:
                continue
            amt = fnum(x.get("AMOUNT"))
            if bill:
                days = fnum(x.get("TERM_DAYS"))
                if not days:  # overview row without line details
                    continue
                tenor = _ca_bill_tenor(days)
                sec_ = f"Canada T-bill {int(days)}d {x.get('MATURITY_DATE') or ''}".strip()
            else:
                yrs = fnum(x.get("TERM_YEARS"))
                if not yrs:
                    continue
                tenor = tenor_label(round(yrs))
                cpn = x.get("COUPON_RATE")
                sec_ = (f"Canada {cpn}% {x.get('MATURITY_DATE') or ''}" if cpn else
                        f"Canada new {tenor} {x.get('MATURITY_DATE') or ''}").strip()
            upcoming.append(dict(country="CA", instrument="Bill" if bill else "Bond",
                                 tenor=tenor, security=sec_, id=x.get("ISIN"), date=d,
                                 size_local=_mul(amt, 1e6), currency="CAD", size_usd=None,
                                 size_note=None if amt else "size in call for tenders",
                                 source=src))
    last = max((r["date"] for r in recs), default=None)
    return {"records": recs, "upcoming": upcoming,
            "source": source("Bank of Canada (Valet auction results)", "ok", last,
                             f"{len(recs)} auctions since {start}; yield=avg (nominal/bills), "
                             "allotment (RRB/ultra); auction_tail_bp=BoC tail (high-avg); "
                             "tail proxy BoC Valet benchmark yields prior close. "
                             f"{VALET}/observations/group/AUC_BOND_RESULTS/json")}


# ===================================================================== DE
DFA = "https://www.deutsche-finanzagentur.de"
DFA_HIST = f"{DFA}/fileadmin/user_upload/Institutionelle-investoren/auktionen/emissionshistorie_en.xlsx"
DFA_CAL = f"{DFA}/en/federal-securities/issuances/issuance-calendar"
BBK_URL = ("https://api.statistiken.bundesbank.de/rest/data/BBSIS/"
           "D.I.ZST.ZI.EUR.S1311.B.A604.{keys}.R.A.A._Z._Z.A")
DE_CURVE = {1: "R01XX", 2: "R02XX", 5: "R05XX", 7: "R07XX", 10: "R10XX",
            15: "R15XX", 20: "R20XX", 30: "R30XX"}
DE_INSTR = {"Bubill": "Bill", "Schatz": "Note", "Bobl": "Note", "Bund": "Bond",
            "ILB": "Linker", "Green": "Bond"}


def _bbk_curve(start: str) -> dict:
    r = http_get(BBK_URL.format(keys="+".join(DE_CURVE.values())),
                 params={"startPeriod": start}, headers={"Accept": "text/csv"}, timeout=40)
    df = pd.read_csv(io.StringIO(r.content.decode("utf-8-sig")), sep=";", dtype=str)
    df["v"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
    df = df.dropna(subset=["v"])
    out = {}
    for t, key in DE_CURVE.items():
        sub = df[df["BBK_SEIS_MATURITY"] == key]
        if not sub.empty:
            out[t] = pd.Series(sub["v"].values, index=pd.to_datetime(sub["TIME_PERIOD"])).sort_index()
    return out


def _seg_years(seg: str):
    m = re.match(r"\s*(\d+)\s*([YM])", str(seg or ""))
    if not m:
        return None
    return int(m.group(1)) / (12 if m.group(2) == "M" else 1)


def collect_de(fx) -> dict:
    start = iso(TODAY - timedelta(days=int(365 * HIST_YEARS) + 30))
    with ThreadPoolExecutor(3) as ex:
        fh = ex.submit(http_get, DFA_HIST, timeout=60)
        fc = ex.submit(_bbk_curve, start)
        fcal = ex.submit(http_get, DFA_CAL, timeout=30)
        raw = fh.result().content
        try:
            curve = fc.result()
        except Exception as e:  # noqa: BLE001
            logger.warning("de bundesbank curve: %s", e)
            curve = {}
        try:
            cal_html = fcal.result().text
        except Exception as e:  # noqa: BLE001
            logger.warning("de calendar: %s", e)
            cal_html = None
    df = pd.read_excel(io.BytesIO(raw), header=None, skiprows=11)
    src = "Deutsche Finanzagentur auction results (emissionshistorie_en.xlsx)"
    recs = []
    for row in df.itertuples(index=False):
        row = list(row)
        isin = str(row[2] or "")
        if not isin.startswith("DE") or fnum(row[0]) is None:
            continue
        d = _xl_date(row[1])
        if not d or d < start:
            continue
        kind = str(row[3]).strip()
        proc = str(row[9]).strip()
        if proc not in ("Auc", "M-A"):  # skip syndicates / own-holding taps
            continue
        mat = _xl_date(row[5])
        resid = (pd.Timestamp(mat) - pd.Timestamp(d)).days / 365.25 if mat else None
        vol, bids, allot, ret = (fnum(row[k]) for k in (7, 10, 13, 17))
        yld = fnum(row[16])
        if kind == "Bubill":
            yrs = resid or _seg_years(row[6])
            tenor = tenor_label(round(yrs * 4) / 4 if yrs else 1, bill=True)
        else:
            yrs = _seg_years(row[6]) or (round(resid) if resid else None)
            tenor = tenor_label(yrs) if yrs else "?"
            if kind == "ILB":
                tenor += " ILB"
            elif kind == "Green":
                tenor += " Green"
        pre, ser = (None, None)
        if kind != "ILB":
            pre, ser = _pre(curve, resid if kind == "Bubill" else yrs, d,
                            lambda k: f"Bundesbank Svensson {DE_CURVE.get(k)} ({k}Y)",
                            min_years=0.75)
        cpn = fnum(row[4])
        recs.append(_rec(
            country="DE", instrument=DE_INSTR.get(kind, kind), tenor=tenor,
            tenor_years=round(yrs, 3) if yrs else None,
            security=f"{kind} {'' if cpn is None else f'{cpn * 100:.2f}% '}{mat or ''}".strip()
                     + (" (multi-ISIN)" if proc == "M-A" else ""),
            id=isin, date=d, currency="EUR", offered=_mul(vol, 1e6),
            allotted=_mul(allot, 1e6), retention=_mul(ret, 1e6), tendered=_mul(bids, 1e6),
            btc=_ratio(bids, allot), yield_=yld,
            yield_type="weighted average yield", pre_yield=pre, pre_yield_series=ser,
            source=src))
    upcoming = []
    by_isin = {r["id"]: r["tenor"] for r in sorted(recs, key=lambda r: r["date"])}
    if cal_html:
        try:
            for t in pd.read_html(io.StringIO(cal_html)):
                if "Date" not in t.columns or "Issuance" not in t.columns:
                    continue
                for _, x in t.iterrows():
                    d = _xl_date(str(x["Date"]).strip())
                    if not d or d < iso(TODAY):
                        continue
                    iss = str(x["Issuance"])
                    m = re.search(r"(DE[0-9A-Z]{10})", iss)
                    kind = iss.split()[0] if iss.split() else "?"
                    v = re.search(r"([\d.,]+)\s*€\s*bn", str(x.get("Volume", "")))
                    size = fnum(v.group(1)) * 1e9 if v else None
                    mat = _xl_date(str(x.get("Maturity", "")).strip())
                    if m and m.group(1) in by_isin and kind != "Bubill":
                        tenor = by_isin[m.group(1)]
                    elif kind == "Bubill" and mat:
                        tenor = tenor_label(round((pd.Timestamp(mat) - pd.Timestamp(d)).days / 365.25 * 4) / 4, bill=True)
                    else:
                        sm = re.search(r"(\d+)\s*Y", iss)
                        yrs = (int(sm.group(1)) if sm else
                               {"Schatz": 2, "Bobl": 5}.get(kind) or
                               (min((7, 10, 15, 20, 30), key=lambda b: abs(
                                   b - (pd.Timestamp(mat) - pd.Timestamp(d)).days / 365.25)) if mat else None))
                        tenor = tenor_label(yrs) if yrs else kind
                    upcoming.append(dict(country="DE", instrument=DE_INSTR.get(kind, kind),
                                         tenor=tenor, security=re.sub(r"\s+", " ", iss).strip(),
                                         id=m.group(1) if m else None, date=d,
                                         size_local=round(size) if size else None, currency="EUR",
                                         size_usd=None, size_note=None,
                                         source="Finanzagentur issuance calendar"))
        except Exception as e:  # noqa: BLE001
            logger.warning("de calendar parse: %s", e)
    last = max((r["date"] for r in recs), default=None)
    return {"records": recs, "upcoming": upcoming,
            "source": source("Deutsche Finanzagentur (auction results xlsx)", "ok", last,
                             f"{len(recs)} auctions since {start}; btc=bids/allotted (ex-retention); "
                             "yield=weighted avg; tail proxy Bundesbank Svensson par curve prior "
                             "close (bills <9M and ILB: none). " + DFA_HIST)}


# ===================================================================== IT
BDI = "https://www.bancaditalia.it/compiti/operazioni-mef/risultati-aste"
IT_BUCKETS = (2, 3, 5, 7, 10, 15, 20, 30, 50)


def _bdi_rows(url: str) -> list[list]:
    zf = zipfile.ZipFile(io.BytesIO(http_get(url, timeout=60).content))
    name = zf.namelist()[0]
    data = zf.read(name)
    if data[:2] == b"PK":  # xlsx
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return pd.read_excel(io.BytesIO(data), header=None).values.tolist()
    return next(iter(read_xls(data).values()))


def collect_it(fx) -> dict:
    start = iso(TODAY - timedelta(days=int(365 * HIST_YEARS) + 30))
    rows, notes = [], []
    with ThreadPoolExecutor(2) as ex:
        futs = {n: ex.submit(_bdi_rows, f"{BDI}/{n}.zip") for n in ("Aste_anno_corrente", "storico_aste")}
        for n, f in futs.items():
            try:
                rows += f.result()
            except Exception as e:  # noqa: BLE001
                logger.warning("it %s: %s", n, e)
                notes.append(f"{n}: {type(e).__name__}")
    src = "Banca d'Italia auction results (Aste_anno_corrente / storico_aste)"
    recs = []
    for r in rows:
        if len(r) < 17 or not str(r[2] or "").startswith("IT"):
            continue
        d = _xl_date(r[0])
        if not d or d < start:
            continue
        if str(r[4]).strip().upper() != "O":  # skip supplementary (specialists-only) tranches
            continue
        typ = str(r[8] or "").strip().upper()
        desc = str(r[5] or "").strip()
        offered, req, assigned = fnum(r[9]), fnum(r[12]), fnum(r[13])
        if not assigned:
            continue
        mat = _xl_date(r[6])
        if typ == "BOT":
            m = re.search(r"(\d+)\s*gg", desc)
            days = int(m.group(1)) if m else ((pd.Timestamp(mat) - pd.Timestamp(d)).days if mat else 0)
            yrs = days / 365
            tenor, instr = tenor_label(yrs, bill=True), "Bill"
            yld, ytype = fnum(r[15]), "weighted average yield"
        else:
            dates = re.findall(r"(\d{1,2}\.\d{1,2}\.\d{4})", desc)
            first = _xl_date(dates[0]) if len(dates) >= 2 else d
            orig = (pd.Timestamp(mat) - pd.Timestamp(first)).days / 365.25 if mat else None
            yrs = min(IT_BUCKETS, key=lambda b: abs(b - orig)) if orig else None
            if typ == "BTPI":
                instr, suffix = "Linker", " BTP€i"
            elif typ.startswith("CCT"):
                instr, suffix = "FRN", " CCTeu"
            elif typ == "CTZ":
                instr, suffix = "Note", " CTZ"
            else:
                instr, suffix = "Bond", ""
            tenor = (tenor_label(yrs) if yrs else "?") + suffix
            yld, ytype = fnum(r[16]), "marginal (uniform-price) gross yield"
        recs.append(_rec(
            country="IT", instrument=instr, tenor=tenor,
            tenor_years=round(yrs, 3) if yrs else None, security=desc, id=r[2], date=d,
            currency="EUR", offered=_mul(offered, 1e6), allotted=_mul(assigned, 1e6),
            tendered=_mul(req, 1e6), btc=_ratio(req, assigned), yield_=yld,
            yield_type=ytype, source=src))
    if not recs:
        raise RuntimeError("no Italian auction rows parsed " + "; ".join(notes))
    last = max(r["date"] for r in recs)
    return {"records": recs, "upcoming": [],
            "source": source("Banca d'Italia / MEF (auction results files)", "ok", last,
                             f"{len(recs)} ordinary-tranche auctions since {start}; btc=requested/"
                             "allotted; tail_bp null (no free daily BTP curve); file refreshed "
                             "~monthly so latest weeks may be missing; upcoming not available "
                             "(MEF announcements are PDF only). " + f"{BDI}/index.html"
                             + (" [" + "; ".join(notes) + "]" if notes else ""))}


# ===================================================================== JP
MOF = "https://www.mof.go.jp/english/policy/jgbs"
MOF_CAL = f"{MOF}/auction/calendar/"
MOF_XLS = f"{MOF}/auction/past_auction_results/"
JP_SHEETS = {"40年債": (40, "Bond", ""), "30年債": (30, "Bond", ""), "20年債": (20, "Bond", ""),
             "10年債": (10, "Bond", ""), "GX10年債": (10, "Bond", " GX"),
             "10年物価連動": (10, "Linker", " Linker"), "5年債": (5, "Note", ""),
             "GX5年債": (5, "Note", " GX"), "2年債": (2, "Note", "")}
JP_BILLS = {"3-month": 0.25, "6-month": 0.5, "1-year": 1.0}


def _mof_curve() -> dict:
    frames = []
    for u in (f"{MOF}/reference/interest_rate/historical/jgbcme_all.csv",
              f"{MOF}/reference/interest_rate/jgbcme.csv"):
        try:
            txt = http_get(u, timeout=40).content.decode("utf-8", "ignore")
            frames.append(pd.read_csv(io.StringIO(txt), skiprows=1))
        except Exception as e:  # noqa: BLE001
            logger.warning("jp curve %s: %s", u, e)
    if not frames:
        return {}
    df = pd.concat(frames)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", format="%Y/%m/%d")
    df = df.dropna(subset=["Date"]).drop_duplicates("Date", keep="last").set_index("Date").sort_index()
    df = df[df.index >= pd.Timestamp(TODAY - timedelta(days=int(365 * HIST_YEARS) + 60))]
    out = {}
    for c in df.columns:
        m = re.match(r"(\d+)Y", str(c))
        if m:
            s = pd.to_numeric(df[c], errors="coerce").dropna()
            if not s.empty:
                out[int(m.group(1))] = s
    return out


def _jp_tenor_from_label(lbl: str):
    """Calendar/result 'Security' label -> (years, instrument, suffix, bill?)."""
    s = lbl.lower()
    m = re.search(r"\((\d+)-(month|year)\)", s)
    if "discount bill" in s or "t-bill" in s:
        if m:
            return (int(m.group(1)) / (12 if m.group(2) == "month" else 1), "Bill", "", True)
        return None
    m = re.search(r"(\d+)-year", s)
    if not m or "liquidity" in s:
        return None
    yrs = int(m.group(1))
    if "inflation" in s:
        return yrs, "Linker", " Linker", False
    if "gx" in s or "climate" in s:
        return yrs, "Bond" if yrs >= 10 else "Note", " GX", False
    return yrs, "Bond" if yrs >= 10 else "Note", "", False


def _jp_rec(*, yrs, instr, suffix, bill, issue, d, mat, cpn, offered, bids, accepted,
            y_low, y_avg, curve, src):
    tenor = tenor_label(yrs, bill=True) if bill else tenor_label(yrs) + suffix
    pre, ser = (None, None)
    yld = y_avg if bill else (y_low if y_low is not None else y_avg)
    if instr != "Linker":
        pre, ser = _pre(curve, yrs, d, lambda k: f"MOF JGB curve {k}Y", min_years=0.75)
    tail = (round((y_low - y_avg) * 100, 2)
            if y_low is not None and y_avg is not None else None)
    return _rec(
        country="JP", instrument=instr, tenor=tenor, tenor_years=round(yrs, 3),
        security=(f"T-Bill #{issue} {mat or ''}" if bill else
                  f"JGB {tenor} #{issue} {'' if cpn is None else f'{cpn:g}% '}{mat or ''}").strip(),
        id=f"{'TB' if bill else 'JGB'}#{issue}", date=d, currency="JPY",
        offered=offered, allotted=accepted, tendered=bids, btc=_ratio(bids, accepted),
        yield_=yld, yield_type="yield at average price" if bill else "highest accepted (lowest price) yield",
        pre_yield=pre, pre_yield_series=ser, auction_tail_bp=tail, source=src)


def _jp_page_result(url: str, label: str, curve: dict):
    r = http_get(url, timeout=20)
    r.encoding = "utf-8"
    t = pd.read_html(io.StringIO(r.text))[0]
    if t.shape[0] >= 1 and "Auction Date" in t.columns:
        x = {str(k).strip(): v for k, v in t.iloc[0].items()}
    else:
        return None
    lab = str(x.get("Security") or label)
    info = _jp_tenor_from_label(lab if "Security" in x else label)
    if info is None:
        return None
    yrs, instr, suffix, bill = info

    def pct(v):
        return fnum(str(v).replace("%", "")) if v is not None else None

    def g(*keys):
        for k, v in x.items():
            if all(kk.lower() in k.lower() for kk in keys):
                return v
        return None
    d = iso(pd.to_datetime(x["Auction Date"], format="%m/%d/%Y"))
    mat = iso(pd.to_datetime(x.get("Maturity Date"), format="%m/%d/%Y", errors="coerce"))
    bids = fnum(g("Competitive Bids"))
    acc = fnum(g("Bids Accepted (billion"))
    return _jp_rec(yrs=yrs, instr=instr, suffix=suffix, bill=bill,
                   issue=int(fnum(x.get("Issue Number")) or 0), d=d, mat=mat,
                   cpn=pct(x.get("Nominal Coupon")), offered=None,
                   bids=_mul(bids, 1e9), accepted=_mul(acc, 1e9),
                   y_low=pct(g("Yield at the Lowest")), y_avg=pct(g("Yield at the Average")),
                   curve=curve, src="MOF Japan auction result page")


def _jp_announce_size(url: str):
    r = http_get(url, timeout=20)
    r.encoding = "utf-8"
    m = re.search(r"About\s*([\d,\.]+)\s*billion yen", re.sub(r"<[^>]+>", " ", r.text))
    return fnum(m.group(1)) * 1e9 if m else None


def collect_jp(fx) -> dict:
    from bs4 import BeautifulSoup
    from urllib.parse import urljoin
    start = iso(TODAY - timedelta(days=int(365 * HIST_YEARS) + 30))
    months = sorted({(TODAY.replace(day=1) - timedelta(days=k * 28)).strftime("%y%m") for k in range(3)}
                    | {(TODAY + timedelta(days=UPCOMING_DAYS)).strftime("%y%m")})
    with ThreadPoolExecutor(8) as ex:
        fj = ex.submit(http_get, MOF_XLS + "Auction_Results_for_JGBs.xls", timeout=60)
        ft = ex.submit(http_get, MOF_XLS + "Auction_Results_for_T-bills.xls", timeout=60)
        fc = ex.submit(_mof_curve)
        fcal = {m: ex.submit(http_get, f"{MOF_CAL}{m}e.htm", timeout=20) for m in months}
        curve = fc.result()
        src = "MOF Japan historical auction results (xls)"
        recs, notes = [], []
        try:
            sheets = read_xls(fj.result().content)
            for name, (yrs, instr, suffix) in JP_SHEETS.items():
                rows = sheets.get(name) or []
                hdr = next((r for r in rows[:8] if r and r[0] == "Issue Number"), None)
                if not hdr:
                    continue
                col = {str(h).strip(): i for i, h in enumerate(hdr) if h}

                def c(row, key):
                    i = next((v for k, v in col.items() if k.startswith(key)), None)
                    return fnum(row[i]) if i is not None and i < len(row) and not isinstance(row[i], str) else None
                for row in rows:
                    if not row or not isinstance(row[1], float) or fnum(row[0]) is None:
                        continue
                    d = _xl_date(row[1])
                    if d < start:
                        continue
                    y_low = c(row, "Yield at the Lowest") or c(row, "Highest Accepted Yield")
                    recs.append(_jp_rec(
                        yrs=yrs, instr=instr, suffix=suffix, bill=False, issue=int(row[0]), d=d,
                        mat=_xl_date(row[3]), cpn=c(row, "Nominal Coupon"),
                        offered=_mul(c(row, "Offering Amount"), 1e8),
                        bids=_mul(c(row, "Amounts of Competitive Bids"), 1e8),
                        accepted=_mul(c(row, "Amounts of Bids Accepted"), 1e8),
                        y_low=y_low, y_avg=c(row, "Yield at the Average"), curve=curve, src=src))
        except Exception as e:  # noqa: BLE001
            logger.warning("jp jgb xls: %s", e)
            notes.append(f"JGB xls: {type(e).__name__}")
        try:
            for name, rows in read_xls(ft.result().content).items():
                for row in rows:
                    if len(row) < 12 or not isinstance(row[2], float) or row[1] not in JP_BILLS:
                        continue
                    d = _xl_date(row[2])
                    if d < start:
                        continue
                    recs.append(_jp_rec(
                        yrs=JP_BILLS[row[1]], instr="Bill", suffix="", bill=True, issue=int(row[0]),
                        d=d, mat=_xl_date(row[4]), cpn=None, offered=_mul(fnum(row[5]), 1e9),
                        bids=_mul(fnum(row[6]), 1e9), accepted=_mul(fnum(row[7]), 1e9),
                        y_low=fnum(row[11]), y_avg=fnum(row[9]), curve=curve, src=src))
        except Exception as e:  # noqa: BLE001
            logger.warning("jp tbill xls: %s", e)
            notes.append(f"T-bill xls: {type(e).__name__}")
        # monthly calendars: fresh results (xls lags ~1 month) + upcoming
        have = {(r["id"], r["date"]) for r in recs}
        res_jobs, upcoming, ann_jobs = [], [], []
        for m, f in fcal.items():
            try:
                soup = BeautifulSoup(f.result().content, "lxml")
            except Exception as e:  # noqa: BLE001
                logger.debug("jp calendar %s: %s", m, e)
                continue
            base = f"{MOF_CAL}{m}e.htm"
            for tr in soup.find_all("tr"):
                cells = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
                if len(cells) < 2:
                    continue
                try:
                    d = iso(pd.to_datetime(cells[0].replace(".", ""), format="%b %d, %Y"))
                except (ValueError, TypeError):
                    continue
                label = cells[1]
                info = _jp_tenor_from_label(label)
                if info is None:
                    continue
                links = [urljoin(base, a["href"]) for a in tr.find_all("a", href=True)]
                res = [u for u in links if re.search(r"eresul\d{8}\.htm$", u)]
                ann = [u for u in links if "/auct" in u]
                num = re.search(r"\((\d+)\)\s*$", label)
                if res and d >= start:
                    if not (num and (f"{'TB' if info[3] else 'JGB'}#{int(num.group(1))}", d) in have):
                        res_jobs.append((res[0], label))
                elif d >= iso(TODAY):
                    yrs, instr, suffix, bill = info
                    u = dict(country="JP", instrument=instr,
                             tenor=tenor_label(yrs, bill=True) if bill else tenor_label(yrs) + suffix,
                             security=label, id=None, date=d, size_local=None, currency="JPY",
                             size_usd=None, size_note="size announced ~1 week before",
                             source="MOF Japan auction calendar")
                    upcoming.append(u)
                    if ann:
                        ann_jobs.append((u, ann[0]))
        rfut = [(ex.submit(_jp_page_result, u, lbl, curve), u) for u, lbl in res_jobs]
        afut = [(ex.submit(_jp_announce_size, a), u) for u, a in ann_jobs]
        n_pages = 0
        for f, u in rfut:
            try:
                r = f.result()
                if r and (r["id"], r["date"]) not in have:
                    recs.append(r)
                    have.add((r["id"], r["date"]))
                    n_pages += 1
            except Exception as e:  # noqa: BLE001
                logger.warning("jp result %s: %s", u, e)
        for f, u in afut:
            try:
                sz = f.result()
                if sz:
                    u["size_local"], u["size_note"] = round(sz), "approx. (announcement)"
            except Exception as e:  # noqa: BLE001
                logger.debug("jp announce: %s", e)
    if not recs:
        raise RuntimeError("no JGB auctions parsed " + "; ".join(notes))
    last = max(r["date"] for r in recs)
    return {"records": recs, "upcoming": upcoming,
            "source": source("MOF Japan (JGB/T-bill auction results)", "ok", last,
                             f"{len(recs)} auctions since {start} ({n_pages} from result pages "
                             "newer than the monthly xls); btc=competitive bids/accepted; yield="
                             "highest accepted (bonds), average (bills); auction_tail_bp=lowest-"
                             "price yield minus average yield; tail proxy MOF JGB curve prior "
                             "close (bills <9M, linkers: none). " + MOF_XLS
                             + (" [" + "; ".join(notes) + "]" if notes else ""))}


# ================================================================ FR / UK
def _blocked_probe(cc, name, url, why):
    status_code = None
    try:
        r = requests.get(url, headers=UA, timeout=10, allow_redirects=False)
        status_code = r.status_code
        blocked = r.status_code in (301, 302, 403, 429, 503) or "perfdrive" in r.headers.get("Location", "")
    except Exception as e:  # noqa: BLE001
        blocked, status_code = True, type(e).__name__
    note = (f"stub: {why}; probe {url} -> {status_code}. No free mirror of auction results found; "
            "paid sources not used." if blocked else
            f"stub: {url} reachable ({status_code}) but no parser implemented yet.")
    return {"records": [], "upcoming": [], "source": source(name, "stub", None, note)}


def collect_fr(fx) -> dict:
    return _blocked_probe("FR", "Agence France Trésor (AFT)", "https://www.aft.gouv.fr/en/auction-results",
                          "aft.gouv.fr returns HTTP 403 to automated requests")


def collect_uk(fx) -> dict:
    return _blocked_probe("UK", "UK Debt Management Office (DMO)",
                          "https://www.dmo.gov.uk/data/gilt-market/gilt-auctions/",
                          "dmo.gov.uk redirects automated requests to a ShieldSquare/perfdrive bot challenge")


COLLECTORS = {"US": collect_us, "CA": collect_ca, "DE": collect_de, "FR": collect_fr,
              "IT": collect_it, "JP": collect_jp, "UK": collect_uk}


# ================================================================ ratings
def enrich(records: list[dict]) -> list[dict]:
    """Sort, compute tail, change vs previous same-tenor auction and ratings."""
    by = {}
    for r in records:
        if r.get("yield_") is not None and r.get("pre_yield") is not None:
            r["tail_bp"] = round((r["yield_"] - r["pre_yield"]) * 100, 1)
        elif r.get("tail_bp") is None:
            r["tail_bp"] = None
        by.setdefault((r["country"], r["tenor"]), []).append(r)
    for (_, _), lst in by.items():
        lst.sort(key=lambda r: r["date"])
        for i, r in enumerate(lst):
            prev = lst[i - 1] if i else None
            r["prev_yield"] = prev.get("yield_") if prev else None
            r["yield_chg_bp"] = (round((r["yield_"] - r["prev_yield"]) * 100, 1)
                                 if r.get("yield_") is not None and r.get("prev_yield") is not None else None)
            cutoff = iso(pd.Timestamp(r["date"]) - pd.Timedelta(days=730))
            hist = [h for h in lst[:i] if h["date"] >= cutoff]
            z_btc = _z(r.get("btc"), [h.get("btc") for h in hist])
            z_tail = _z(r.get("tail_bp"), [h.get("tail_bp") for h in hist])
            r["z_btc"], r["z_tail"] = z_btc, z_tail
            r["n_hist"] = len(hist)
            parts = [p for p in (z_btc, -z_tail if z_tail is not None else None) if p is not None]
            if parts:
                score = sum(parts) / len(parts)
                r["rating"] = "strong" if score >= 0.6 else "weak" if score <= -0.6 else "neutral"
            else:
                r["rating"] = None
    return records


def _z(v, hist):
    vals = [h for h in hist if h is not None]
    if v is None or len(vals) < RATING_MIN_N:
        return None
    sd = statistics.pstdev(vals)
    if sd <= 1e-9:
        return 0.0
    return round((v - statistics.fmean(vals)) / sd, 2)


# ================================================================== build
def _public(r: dict) -> dict:
    out = {k: v for k, v in r.items() if k != "yield_"}
    out["yield"] = r.get("yield_")
    return out


def build_auctions() -> dict:
    t0 = time.time()
    fx = {}
    try:
        fx = get_fx()
    except Exception as e:  # noqa: BLE001
        logger.warning("fx: %s", e)
    seed = load_seed(HIST_SEED, {}) or {}
    stored = seed.get("records", {})
    sources, upcoming, fresh = [], [], []

    def run(item):
        cc, fn = item
        t = time.time()
        try:
            res = fn(fx)
        except Exception as e:  # noqa: BLE001
            logger.exception("auctions %s failed", cc)
            res = {"records": [], "upcoming": [],
                   "source": source(cc, "error", None, f"{type(e).__name__}: {e}"[:300])}
        res["source"]["note"] = (res["source"].get("note") or "") + f" [{time.time() - t:.1f}s]"
        res["source"]["country"] = cc
        return res

    with ThreadPoolExecutor(len(COLLECTORS)) as ex:
        for res in ex.map(run, COLLECTORS.items()):
            sources.append(res["source"])
            upcoming += res.get("upcoming", [])
            fresh += res.get("records", [])

    # merge with accumulated history (fresh wins); keep ~HIST_YEARS
    merged = dict(stored)
    for r in fresh:
        if r.get("size_usd") is None:
            r["size_usd"] = to_usd(r.get("allotted") or r.get("offered"), r.get("currency"), fx)
        merged[rec_key(r)] = r
    cutoff = iso(TODAY - timedelta(days=int(365 * HIST_YEARS)))
    merged = {k: v for k, v in merged.items() if v.get("date", "") >= cutoff}
    try:
        save_seed(HIST_SEED, {"fetched_at": now_iso(), "records": merged})
    except Exception as e:  # noqa: BLE001
        logger.warning("save seed: %s", e)

    records = enrich([dict(v) for v in merged.values()])
    records.sort(key=lambda r: (r["date"], r["country"]), reverse=True)
    rc = iso(TODAY - timedelta(days=RECENT_DAYS))
    recent = [_public(r) for r in records if r["date"] >= rc]

    history = {}
    for r in sorted(records, key=lambda r: r["date"]):
        history.setdefault(f"{r['country']}|{r['tenor']}", []).append(
            [r["date"], r.get("btc"), r.get("tail_bp"), r.get("yield_")])

    hi = iso(TODAY + timedelta(days=UPCOMING_DAYS))
    for u in upcoming:
        if u.get("size_usd") is None:
            u["size_usd"] = to_usd(u.get("size_local"), u.get("currency"), fx)
    upcoming = sorted([u for u in upcoming if iso(TODAY) <= u["date"] <= hi],
                      key=lambda u: (u["date"], u["country"]))

    ez = [r for r in recent if r["country"] in ("DE", "FR", "IT")]
    summary = {}
    for cc in ("DE", "FR", "IT"):
        lst = [r for r in ez if r["country"] == cc]
        btcs = [r["btc"] for r in lst if r.get("btc") is not None]
        zs = [r["z_btc"] for r in lst if r.get("z_btc") is not None]
        summary[cc] = {"n": len(lst),
                       "avg_btc": round(statistics.fmean(btcs), 2) if btcs else None,
                       "avg_z_btc": round(statistics.fmean(zs), 2) if zs else None,
                       **{k: sum(1 for r in lst if r.get("rating") == k)
                          for k in ("strong", "neutral", "weak")}}

    logger.info("auctions built in %.1fs", time.time() - t0)
    return {"generated_at": now_iso(), "fx_usd": fx, "upcoming": upcoming,
            "recent": recent, "history": history,
            "eurozone": {"recent": ez, "summary": summary},
            "tail_method": TAIL_METHOD, "rating_method": RATING_METHOD,
            "sources": sources, "runtime_s": round(time.time() - t0, 1)}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    out = build_auctions()
    dump("auctions.json", out)
    for s in out["sources"]:
        print(f"{s['status']:6} {s['name']}: {s['as_of']} {s['note']}")
    print(f"upcoming={len(out['upcoming'])} recent={len(out['recent'])} "
          f"series={len(out['history'])} runtime={out['runtime_s']}s")
