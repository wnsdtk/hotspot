#!/usr/bin/env python3
"""fetch_loop.py — GitHub Actions loop version of the hotspot data pipeline.

Runs inside a scheduled Actions job (every 5 min). No user token needed
(uses the job's GITHUB_TOKEN); fully independent of Muse.

Lanes:
  SLOW (once at job start): trenches(initial)+consensus+tags+daily-summary
      check+douyin(official, xxapi fallback)+binance+translations
  FAST (every 30s until ~275s job budget): gmgn trenches, debot liquidity,
      t.me tweets

Writes only files whose content actually changed (hash compare excluding
timestamps). Every source is wrapped in try/except — a dead source emits
an honest empty structure, never fabricated data, never a fatal exit.

State (persisted in --state-dir, committed to the data branch):
  tweet-store.json        2-day rolling tweet store
  translation-cache.json  MyMemory cache keyed by stable id
"""
import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote as urlquote
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

SH = ZoneInfo("Asia/Shanghai")
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
UA_IPHONE = ("Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
             "AppleWebKit/605.1.15")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "data"))
DEFAULT_CACHE = os.path.join(SCRIPT_DIR, "translation-cache.json")

# Total NEW MyMemory translations allowed per run (tweets + binance share it).
TRANSLATE_BUDGET = 20
_translate_used = 0

http = requests.Session()
http.headers.update({"User-Agent": UA})


# ---------------------------------------------------------------- utils
def now_sh():
    return datetime.now(SH)


def iso_now():
    return now_sh().isoformat()


def atomic_write(path, obj):
    d = os.path.dirname(os.path.abspath(path))
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def load_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def num(v, default=0.0):
    try:
        if v is None:
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def has_cjk(s):
    return bool(re.search(r"[\u4e00-\u9fff]", s or ""))


# ------------------------------------------------------- translation
def translate(text, cache_key, cache):
    """Chinese translation for `text`.

    Returns "" when: text empty / already Chinese / cached miss quota /
    MyMemory quota fails. Results (incl. "") are cached by cache_key.
    """
    global _translate_used
    text = (text or "").strip()
    if not text or has_cjk(text):
        return ""
    if len(text) > 1500:
        text = text[:1500]
    if cache_key in cache:
        return cache[cache_key]
    if _translate_used >= TRANSLATE_BUDGET:
        return ""
    _translate_used += 1
    try:
        r = http.get(
            "https://api.mymemory.translated.net/get",
            params={"q": text, "langpair": "en|zh-CN"},
            timeout=15,
        )
        d = r.json()
        if str(d.get("responseStatus")) != "200":
            cache[cache_key] = ""
            return ""
        out = (d.get("responseData") or {}).get("translatedText", "")
        if not out or "MYMEMORY WARNING" in out or "QUERY LENGTH LIMIT" in out:
            cache[cache_key] = ""
            return ""
        out = out.strip()
        cache[cache_key] = out
        return out
    except Exception as e:
        print(f"  [translate] failed: {e}", file=sys.stderr)
        cache[cache_key] = ""
        return ""


# ---------------------------------------------------------------- tweets
# Verified 2026-11-02: t.me/s/<channel> preview pages, ~20 posts each.
# Post text layout (lines):
#   [0] emoji  [1] action(引用推文/转推/回复/原创/新增关注/取消关注)
#   [2] 👤  [3] person line ("Yi He @heyibinance" / "CZ" / "Elon Musk @elonmusk")
#   ... "🌐" / "中文翻译 ——" section: person's own words, Chinese first,
#       English original inside ( ... ) parens ...
#   ... "💬" + "原推："/"原推翻译：" section: quoted/replied/retweeted text
#   ... "🕒" / "推文时间: YYYY-MM-DD HH:MM:SS" (Asia/Shanghai) / "🔗" / "查看原文"
# "查看原文" anchor href = https://x.com/<handle>/status/<id>  (tweet id source)
# Media: a.tgme_widget_message_photo_wrap — prefer anchor href (pbs.twimg.com
# original), fall back to style background-image url; force name=orig.
CHANNELS = [
    ("elon", "TechMelonElonMusk", "elon"),
    ("czheyi", "TechMelonCzHeyi", "cz"),
]
PERSON_INFO = {
    "elon": ("马斯克", "@elonmusk"),
    "cz": ("赵长鹏", "@cz_binance"),
    "heyi": ("何一", "@heyibinance"),
}
KIND_MAP = {"引用推文": "quote", "转推": "retweet", "回复": "reply", "原创": "original"}


def detect_person(person_line, default):
    pl = person_line or ""
    if "@heyibinance" in pl or "Yi He" in pl or "何一" in pl:
        return "heyi"
    if "@cz_binance" in pl or pl.strip().startswith("CZ") or "赵长鹏" in pl:
        return "cz"
    if "@elonmusk" in pl or "Elon Musk" in pl or "马斯克" in pl:
        return "elon"
    return default


def tweet_time_iso(lines, msg):
    for l in lines:
        if l.startswith("推文时间:"):
            ts = l.split("推文时间:", 1)[1].strip()
            try:
                dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=SH)
                return dt.isoformat()
            except ValueError:
                pass
    tm = msg.select_one("time")
    if tm and tm.get("datetime"):
        try:
            dt = datetime.fromisoformat(tm["datetime"].replace("Z", "+00:00"))
            return dt.astimezone(SH).isoformat()
        except ValueError:
            pass
    return iso_now()


def tweet_media(msg):
    media = []

    def add(url):
        url = (url or "").strip()
        if url.startswith("//"):
            url = "https:" + url
        if not url.startswith("http"):
            return
        if "pbs.twimg.com" in url:
            # prefer the X original-size image
            url = re.sub(r"([?&])name=[^&]*", r"\1name=orig", url)
            if "name=" not in url:
                url += ("&" if "?" in url else "?") + "name=orig"
        media.append({"url": url, "type": "img"})

    def bg(el):
        st = (el.get("style") or "") if el else ""
        m = re.search(r"background-image\s*:\s*url\(['\"]?([^'\")]+)['\"]?\)", st, re.I)
        return m.group(1) if m else ""

    # 1) 引用推文的配图：t.me 渲染成 link preview 的背景图
    for a in msg.select("a.tgme_widget_message_link_preview"):
        add(bg(a.select_one("i.link_preview_image")))
        for v in a.select("i.link_preview_video, video"):
            add(bg(v) or (v.get("src") or ""))

    # 2) 原生相册
    for a in msg.select("a.tgme_widget_message_photo_wrap"):
        href = (a.get("href") or "").strip()
        add(href if href.startswith("http") else bg(a))

    # 3) 直接 <video>
    for v in msg.select("video"):
        src_el = v.select_one("source")
        add((src_el.get("src") if src_el else "") or v.get("src") or "")

    # dedupe, keep order
    seen, out = set(), []
    for m in media:
        if m["url"] not in seen:
            seen.add(m["url"])
            out.append(m)
    return out[:4]


def parse_post(msg, default_person):
    t = msg.select_one("div.tgme_widget_message_text")
    if not t:
        return None
    lines = [l for l in t.get_text("\n", strip=True).split("\n")]
    if len(lines) < 4:
        return None
    kind = KIND_MAP.get(lines[1])
    if not kind:
        return None  # skip 新增关注 / 取消关注 — not tweet kinds
    person = detect_person(lines[3], default_person)
    name, handle = PERSON_INFO[person]

    # --- person's own words: "中文翻译 ——" section (Chinese, then English in parens)
    zh_parts, en_parts = [], []
    try:
        ti = lines.index("中文翻译 ——")
    except ValueError:
        ti = -1
    if ti >= 0:
        end = len(lines)
        for j in range(ti + 1, len(lines)):
            if lines[j] == "💬":
                end = j
                break
        in_en = False
        for s in lines[ti + 1:end]:
            if s == "(":
                in_en = True
                continue
            if s == ")":
                in_en = False
                continue
            (en_parts if in_en else zh_parts).append(s)

    # --- quoted / replied-to / retweeted original: "原推："/"原推翻译：" section
    orig_text = ""
    for j, l in enumerate(lines):
        if l in ("原推：", "原推翻译：", "原推:", "原文："):
            buf = []
            for s in lines[j + 1:]:
                if s == "🕒" or s.startswith("推文时间:"):
                    break
                buf.append(s)
            orig_text = "\n".join(buf).strip()
            break

    zh = "\n".join(zh_parts).strip()
    en = "\n".join(en_parts).strip()
    if kind in ("reply", "quote"):
        text = en or zh
        text_zh = zh if en else ""
    elif kind == "retweet":
        text = orig_text
        text_zh = ""
    else:  # original
        buf = []
        for s in lines[4:]:
            if s == "🕒" or s.startswith("推文时间:"):
                break
            if s in ("🌐", "中文翻译 ——"):
                continue
            buf.append(s)
        text = "\n".join(buf).strip() or zh or en
        text_zh = ""

    # --- x.com link + tweet id from "查看原文"
    url, tid = "", ""
    for a in t.select("a"):
        if (a.get_text(strip=True) == "查看原文") and a.get("href"):
            url = a["href"].strip()
            break
    if not url:
        for a in t.select("a"):
            href = a.get("href", "")
            if "/status/" in href and "x.com" in href:
                url = href.strip()
                break
    m = re.search(r"/status/(\d+)", url or "")
    if m:
        tid = m.group(1)
    if not tid:
        inner = msg.select_one("div.tgme_widget_message")
        tid = (inner.get("data-post", "") if inner else "").replace("/", "_") or f"tg_{(hash(text) & 0xffff):x}"

    return {
        "id": tid,
        "person": person,
        "name": name,
        "handle": handle,
        "time": tweet_time_iso(lines, msg),
        "text": text,
        "_zh_channel": zh if (kind in ("reply", "quote") and en) else "",
        "kind": kind,
        "media": tweet_media(msg),
        "url": url,
    }


def fetch_tweets(state_dir, tr_cache):
    store_path = os.path.join(state_dir, "tweet-store.json")
    store = load_json(store_path, {})
    if not isinstance(store, dict):
        store = {}
    new_count = 0
    for key, channel, default_person in CHANNELS:
        try:
            r = http.get(
                f"https://snowy-mud-3037.rj982946925.workers.dev/?u=https://t.me/s/{channel}",
                headers={"User-Agent": UA_IPHONE},
                timeout=30,
            )
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            msgs = soup.select("div.tgme_widget_message_wrap")
            if not msgs:
                print(f"  [tweets] {channel}: no posts parsed, keeping store")
                continue
            for msg in msgs:
                try:
                    p = parse_post(msg, default_person)
                except Exception as e:
                    print(f"  [tweets] parse error in {channel}: {e}", file=sys.stderr)
                    continue
                if not p or not p["text"]:
                    continue
                if p["id"] in store:
                    # 旧条目之前是直接跳过的，导致"图片/翻译"永远补不上；
                    # 这里用本次解析结果回填缺失字段（媒体优先）。
                    old = store[p["id"]]
                    if (not old.get("media")) and p.get("media"):
                        old["media"] = p["media"]
                    if (not old.get("text_zh")) and p.get("_zh_channel"):
                        old["text_zh"] = p["_zh_channel"]
                    if (not old.get("url")) and p.get("url"):
                        old["url"] = p["url"]
                    continue
                # text_zh: channel's own Chinese translation first (free & exact),
                # else MyMemory for new tweets.
                zh = p.pop("_zh_channel") or translate(p["text"], f"tw:{p['id']}", tr_cache)
                p["text_zh"] = zh
                store[p["id"]] = p
                new_count += 1
            print(f"  [tweets] {channel}: {len(msgs)} posts parsed, {new_count} new so far")
        except Exception as e:
            print(f"  [tweets] {channel} FAILED: {e}", file=sys.stderr)

    # 2-day rolling window
    cutoff = now_sh() - timedelta(days=2)
    pruned = {}
    for tid, p in store.items():
        try:
            dt = datetime.fromisoformat(p["time"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=SH)
        except Exception:
            continue
        if dt >= cutoff:
            pruned[tid] = p
    tweets = sorted(pruned.values(), key=lambda p: p["time"], reverse=True)
    atomic_write(store_path, pruned)
    return {"updated_at": iso_now(), "tweets": tweets}

# ---------------------------------------------------------------- trenches
# gmgn (primary, verified 2026-11-02):
#   GET https://gmgn.ai/defi/quotation/v1/rank/{sol,bsc,robinhood}/swaps/1h
#   -> {"code":0,"data":{"rank":[{address,symbol,name,logo,market_cap,
#       liquidity,volume,holder_count,creation_timestamp,smart_degen_count,
#       history_highest_market_cap,twitter_username,website,...}]}}  (100 items)
# debot fallback (verified 2026-11-02):
#   GET https://debot.ai/api/dashboard/chain/recommend/hot_token?chain={bsc|solana|robinhood}
#   -> {"code":0,"data":[{address,symbol,name,logo,chain,kol_count,
#       creation_timestamp(sec),market_info:{mkt_cap,volume,...},
#       pair_summary_info:{liquidity},social_info:{dx_twitter,...}}]}
TRENCH_CHAINS = [
    # (file key, gmgn chain slug, token "chain" value per contract)
    ("bsc", "bsc", "bsc"),
    ("sol", "sol", "sol"),
    ("rb", "robinhood", "robinhood"),
]
DEBOT_CHAIN = {"bsc": "bsc", "sol": "solana", "rb": "robinhood"}


def age_text(ts):
    try:
        age_s = (now_sh() - datetime.fromtimestamp(float(ts), tz=SH)).total_seconds()
    except Exception:
        return ""
    if age_s < 0:
        age_s = 0
    if age_s < 3600:
        return f"{max(1, int(age_s // 60))}分钟前"
    if age_s < 86400:
        return f"{int(age_s // 3600)}小时前"
    return f"{int(age_s // 86400)}天前"


def narrative(symbol, name):
    s = f"{symbol or ''} {name or ''}".lower()
    if "ai" in s.split() or "agent" in s or re.search(r"(^|[^a-z])ai([^a-z]|$)", s):
        return "AI/Agent概念"
    if "gta" in s:
        return "GTA概念"
    if any(k in s for k in ("trump", "maga", "elon", "musk", "cz", "heyi", "何一", "马斯克")):
        return "人物概念"
    if any(k in s for k in ("doge", "shib", "pepe", "frog", "cat", "dog", "inu", "bonk", "wif")):
        return "动物Meme"
    if any(k in s for k in ("game", "gaming", "play")):
        return "游戏概念"
    if "rwa" in s:
        return "RWA概念"
    if "depin" in s:
        return "DePIN概念"
    return ""


def norm_gmgn(t, chain):
    symbol = (t.get("symbol") or "").strip()
    if symbol and not symbol.startswith("$"):
        symbol = "$" + symbol
    tw = (t.get("twitter_username") or "").strip()
    web = (t.get("website") or "").strip()
    x_url = ""
    if "x.com" in tw or "twitter.com" in tw:
        x_url = tw
    elif "x.com" in web or "twitter.com" in web:
        x_url = web
    return {
        "symbol": symbol or "$?",
        "name": (t.get("name") or "").strip(),
        "ca": (t.get("address") or "").strip(),
        "chain": chain,
        "icon": (t.get("logo") or "").strip(),
        "age_text": age_text(t.get("creation_timestamp")),
        "_age_h": (now_sh() - datetime.fromtimestamp(float(t.get("creation_timestamp") or 0), tz=SH)).total_seconds() / 3600 if t.get("creation_timestamp") else 9999,
        "market_cap": num(t.get("market_cap")),
        "liquidity": num(t.get("liquidity")),
        "vol_1h": num(t.get("volume")),
        "smart_count": int(num(t.get("smart_degen_count"))),
        "smart_label": "聪明钱",
        "narrative": narrative(t.get("symbol"), t.get("name")),
        "x_url": x_url,
        "_hist_high": num(t.get("history_highest_market_cap")),
        "_ts": num(t.get("creation_timestamp")),
    }


def norm_debot(t, chain):
    mi = t.get("market_info") or {}
    ps = t.get("pair_summary_info") or {}
    si = t.get("social_info") or {}
    symbol = (t.get("symbol") or "").strip()
    if symbol and not symbol.startswith("$"):
        symbol = "$" + symbol
    x_url = ""
    for k in ("dx_twitter", "twitter", "twitter_url"):
        v = (si.get(k) or "").strip()
        if "x.com" in v or "twitter.com" in v:
            x_url = v
            break
    return {
        "symbol": symbol or "$?",
        "name": (t.get("name") or "").strip(),
        "ca": (t.get("address") or "").strip(),
        "chain": chain,
        "icon": (t.get("logo") or "").strip(),
        "age_text": age_text(t.get("creation_timestamp")),
        "_age_h": (now_sh() - datetime.fromtimestamp(float(t.get("creation_timestamp") or 0), tz=SH)).total_seconds() / 3600 if t.get("creation_timestamp") else 9999,
        "market_cap": num(mi.get("mkt_cap")),
        "liquidity": num(ps.get("liquidity")),
        "vol_1h": num(mi.get("volume")),
        "smart_count": int(num(t.get("kol_count"))),
        "smart_label": "KOL",
        "narrative": narrative(t.get("symbol"), t.get("name")),
        "x_url": x_url,
        "_hist_high": 0.0,
        "_ts": num(t.get("creation_timestamp")),
    }


def fetch_gmgn(gmgn_chain):
    r = http.get(
        f"https://gmgn.ai/defi/quotation/v1/rank/{gmgn_chain}/swaps/1h",
        timeout=25,
    )
    r.raise_for_status()
    d = r.json()
    rank = (d.get("data") or {}).get("rank") or []
    if not isinstance(rank, list) or not rank:
        raise ValueError("gmgn: empty rank")
    return rank


def fetch_debot_chain(chain_key):
    dc = DEBOT_CHAIN[chain_key]
    r = http.get(
        "https://debot.ai/api/dashboard/chain/recommend/hot_token",
        params={"chain": dc},
        headers={"Referer": "https://debot.ai/"},
        timeout=25,
    )
    r.raise_for_status()
    d = r.json()
    data = d.get("data")
    if d.get("code") != 0 or not isinstance(data, list) or not data:
        raise ValueError(f"debot {chain_key}: bad response")
    return data


def build_trench(normed):
    """~10 tokens: first 4 = age<=2h by volume desc; last 6 = rest by smart desc."""
    pool = [t for t in normed if t["_age_h"] <= 120 and t["market_cap"] <= 300_000_000 and t["ca"]]
    fresh = sorted([t for t in pool if t["_age_h"] <= 2], key=lambda t: t["vol_1h"], reverse=True)[:4]
    picked = {id(t) for t in fresh}
    rest = sorted([t for t in pool if id(t) not in picked], key=lambda t: t["smart_count"], reverse=True)[:6]
    out = []
    for t in fresh + rest:
        t = dict(t)
        out.append({k: t[k] for k in (
            "symbol", "name", "ca", "chain", "icon", "age_text", "market_cap",
            "liquidity", "vol_1h", "smart_count", "smart_label", "narrative", "x_url")})
    return out


def fetch_trenches():
    """Returns (files, pool_for_consensus). pool = gmgn raw per chain (for consensus)."""
    files = {}
    pool = {}  # chain_key -> list of normed gmgn tokens (consensus source)
    for chain_key, gmgn_chain, token_chain in TRENCH_CHAINS:
        source, normed = "gmgn", []
        try:
            raw = fetch_gmgn(gmgn_chain)
            normed = [norm_gmgn(t, token_chain) for t in raw]
            pool[chain_key] = normed
            print(f"  [trench-{chain_key}] gmgn: {len(normed)} raw")
        except Exception as e:
            print(f"  [trench-{chain_key}] gmgn FAILED ({e}); trying debot", file=sys.stderr)
            try:
                raw = fetch_debot_chain(chain_key)
                normed = [norm_debot(t, token_chain) for t in raw]
                source = "debot"
                pool[chain_key] = normed
                print(f"  [trench-{chain_key}] debot: {len(normed)} raw")
            except Exception as e2:
                print(f"  [trench-{chain_key}] debot FAILED too: {e2}", file=sys.stderr)
        files[f"trench-{chain_key}.json"] = {
            "updated_at": iso_now(),
            "source": source,
            "tokens": build_trench(normed),
        }
    return files, pool


# --------------------------------------------------------------- consensus
def build_consensus(pool):
    """smart>=5 AND created within 72h. lowcap: mc<=30k top5 by smart;
    rest: exclude lowcap picks, top5 by smart."""
    cands = []
    for chain_key, normed in pool.items():
        for t in normed:
            if t["smart_count"] >= 5 and t["_age_h"] <= 72 and t["ca"]:
                cands.append(t)
    cands.sort(key=lambda t: t["smart_count"], reverse=True)
    low = [t for t in cands if t["market_cap"] <= 30000][:5]
    low_ids = {id(t) for t in low}
    rest = [t for t in cands if id(t) not in low_ids][:5]

    def emit(t):
        return {
            "symbol": t["symbol"], "name": t["name"], "ca": t["ca"],
            "chain": t["chain"], "icon": t["icon"], "age_text": t["age_text"],
            "market_cap": t["market_cap"], "liquidity": t["liquidity"],
            "vol_1h": t["vol_1h"], "smart_count": t["smart_count"],
            "smart_label": t["smart_label"], "narrative": t["narrative"],
            "x_url": t["x_url"],
            "history_highest_mcap": t["_hist_high"],
        }

    return {
        "updated_at": iso_now(),
        "lowcap": [emit(t) for t in low],
        "rest": [emit(t) for t in rest],
    }

# --------------------------------------------------------------- liquidity
# Verified 2026-11-02: GET https://debot.ai/api/edge/launchpad/stats/latest?interval=1h
# -> {"code":0,"data":{"interval":"1h","chains":[{chain,summary:{tx_count,
#    wallet_count,volume,token_count,buy_count,buy_volume,sell_count,
#    sell_volume,token_created_count,token_migrated_count},prev_summary:{...},
#    top_launchpads}]}}. debot names OKX's chain "xlayer", Ethereum "eth".
STAT_CHAINS = [
    ("BSC", "bsc"), ("Solana", "solana"), ("Robinhood", "robinhood"),
    ("OKX", "xlayer"), ("Base", "base"), ("Arc", "arc"), ("Ethereum", "eth"),
]


def fetch_liquidity(trench_files=None):
    """Build liquidity.json. trench_files (optional) = dict from fetch_trenches()
    used to attach top_tokens (top 3 by 1h volume) per chain."""
    chains = []
    # map liquidity chain key -> trench file key
    _TRENCH_MAP = {"bsc": "trench-bsc.json", "solana": "trench-sol.json",
                   "robinhood": "trench-rb.json"}
    try:
        r = http.get(
            "https://debot.ai/api/edge/launchpad/stats/latest",
            params={"interval": "1h"},
            headers={"Referer": "https://debot.ai/"},
            timeout=25,
        )
        r.raise_for_status()
        d = r.json()
        if d.get("code") != 0:
            raise ValueError(f"debot stats code={d.get('code')}")
        by_key = {}
        for c in (d.get("data") or {}).get("chains") or []:
            if isinstance(c, dict) and c.get("chain"):
                by_key[c["chain"]] = c
        for label, key in STAT_CHAINS:
            c = by_key.get(key)
            if not c:
                continue
            s = c.get("summary") or {}
            if not isinstance(s, dict) or not s:
                continue  # omit chains with no data
            p = c.get("prev_summary") or {}
            buy, sell = num(s.get("buy_volume")), num(s.get("sell_volume"))
            vol, pvol = num(s.get("volume")), num(p.get("volume"))
            metrics = {
                "volume": vol,
                "buy_volume": buy,
                "sell_volume": sell,
                "wallet_count": num(s.get("wallet_count")),
                "token_count": num(s.get("token_count")),
                "token_created_count": num(s.get("token_created_count")),
                "token_migrated_count": num(s.get("token_migrated_count")),
                "tx_count": num(s.get("tx_count")),
            }
            if pvol > 0:
                metrics["volume_change_pct"] = round((vol - pvol) / pvol * 100, 2)
            if num(s.get("wallet_count")) > 0:
                metrics["avg_buy_per_wallet"] = round(buy / num(s.get("wallet_count")), 2)
            # top_tokens: 暂时留空（trench 数据没有真实的流动性字段，硬填是乱码）
            chains.append({
                "chain": label,
                "net_inflow": buy - sell,
                "metrics": metrics,
                "top_tokens": top_tokens,
            })
        print(f"  [liquidity] {len(chains)} chains")
    except Exception as e:
        print(f"  [liquidity] FAILED: {e}", file=sys.stderr)
    return {"updated_at": iso_now(), "chains": chains}


# ------------------------------------------------------------------ douyin
# Verified 2026-11-02: GET https://www.douyin.com/aweme/v1/web/hot/search/list/
# (Chrome UA + Referer, no cookie) -> {"status_code":0,"data":{
#   "trending_list":[5 x {...}], "word_list":[50 x {word,hot_value,label,position}] }}
def fetch_douyin():
    out = {"updated_at": iso_now(), "trending": [], "hotlist": []}
    try:
        r = http.get(
            "https://www.douyin.com/aweme/v1/web/hot/search/list/",
            headers={"Referer": "https://www.douyin.com/"},
            timeout=25,
        )
        r.raise_for_status()
        d = r.json()
        data = d.get("data") or {}
        for it in (data.get("trending_list") or [])[:5]:
            w = (it.get("word") or "").strip()
            if not w:
                continue
            out["trending"].append({
                "word": w,
                "url": "https://www.douyin.com/search/" + urlquote(w),
            })
        for it in (data.get("word_list") or [])[:50]:
            w = (it.get("word") or "").strip()
            if not w:
                continue
            out["hotlist"].append({
                "word": w,
                "hot_value": num(it.get("hot_value")),
                "url": "https://www.douyin.com/search/" + urlquote(w),
            })
        print(f"  [douyin] trending={len(out['trending'])} hotlist={len(out['hotlist'])}")
    except Exception as e:
        print(f"  [douyin] FAILED: {e}", file=sys.stderr)
    return out


# ------------------------------------------------- douyin xxapi fallback
def fetch_douyin_xxapi():
    """Fallback when douyin official API is unreachable.

    xxapi returns only the hot word list (no official trending_list),
    so trending stays empty; the page's client-side diff still works.
    """
    out = {"updated_at": iso_now(), "trending": [], "hotlist": []}
    try:
        r = http.get("https://v2.xxapi.cn/api/douyinhot", timeout=25)
        r.raise_for_status()
        d = r.json()
        if d.get("code") != 200 or not isinstance(d.get("data"), list):
            raise ValueError("xxapi bad response")
        for it in d["data"][:50]:
            w = (it.get("word") or "").strip()
            if not w:
                continue
            out["hotlist"].append({
                "word": w,
                "hot_value": num(it.get("hot_value")),
                "url": "https://www.douyin.com/search/" + urlquote(w),
            })
        print(f"  [douyin] xxapi fallback: hotlist={len(out['hotlist'])}")
    except Exception as e:
        print(f"  [douyin] xxapi fallback FAILED: {e}", file=sys.stderr)
    return out


def build_douyin_rising(hotlist, state_dir):
    """Compare current douyin word_list against the previous snapshot.

    Snapshot (word -> hot_value) persists in <state_dir>/douyin-snapshot.json,
    which the workflow commits to the data branch, so it survives across jobs.

    Returns [{word, url, tag}] where tag is '新上榜' (appeared this round)
    or '飙升' (top-5 hot_value gainers). ~10 items max.
    First run (no snapshot): returns [] without error.
    """
    snap_path = os.path.join(state_dir, "douyin-snapshot.json")
    prev = load_json(snap_path, None)
    prev_words = {}
    if isinstance(prev, dict) and isinstance(prev.get("words"), dict):
        prev_words = prev["words"]
    rising = []
    if prev_words and hotlist:
        # 新上榜: words in current list but absent from previous snapshot
        for it in hotlist:
            w = (it.get("word") or "").strip()
            if w and w not in prev_words:
                rising.append({
                    "word": w,
                    "url": it.get("url") or ("https://www.douyin.com/search/" + urlquote(w)),
                    "tag": "新上榜",
                })
        # 飙升: top 5 by hot_value increase (positive delta only)
        deltas = []
        for it in hotlist:
            w = (it.get("word") or "").strip()
            if not w or w not in prev_words:
                continue
            d = num(it.get("hot_value")) - num(prev_words.get(w))
            if d > 0:
                deltas.append((d, it))
        deltas.sort(key=lambda x: x[0], reverse=True)
        for _, it in deltas[:5]:
            w = (it.get("word") or "").strip()
            rising.append({
                "word": w,
                "url": it.get("url") or ("https://www.douyin.com/search/" + urlquote(w)),
                "tag": "飙升",
            })
        rising = rising[:10]
    # Refresh snapshot for the next run. Only overwrite when we actually got
    # data; never wipe a good snapshot with an empty/failed fetch.
    if hotlist:
        try:
            snap = {
                "updated_at": iso_now(),
                "words": {(it.get("word") or "").strip(): num(it.get("hot_value"))
                          for it in hotlist if (it.get("word") or "").strip()},
            }
            atomic_write(snap_path, snap)
        except Exception as e:
            print(f"  [douyin] snapshot save FAILED: {e}", file=sys.stderr)
    if not prev_words:
        print("  [douyin] no previous snapshot -> rising=[] (first run)")
    else:
        n_new = sum(1 for r in rising if r["tag"] == "新上榜")
        n_up = sum(1 for r in rising if r["tag"] == "飙升")
        print(f"  [douyin] rising={len(rising)} (新上榜={n_new}, 飙升={n_up})")
    return rising


# ------------------------------------------------------- change detection
def content_hash(obj):
    """Hash of payload content, excluding volatile timestamp fields."""
    c = obj
    if isinstance(obj, dict):
        c = {k: v for k, v in obj.items() if k not in ("updated_at", "generated_at")}
    return hashlib.sha256(
        json.dumps(c, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()


def write_if_changed(path, obj):
    """Atomic-write obj to path only when content actually changed.

    Returns True when the file was (re)written.
    """
    try:
        if os.path.exists(path):
            old = load_json(path, None)
            if old is not None and content_hash(old) == content_hash(obj):
                return False
    except Exception:
        pass
    atomic_write(path, obj)
    return True


# ---------------------------------------------------------------- halfhour tags
# Narrative-only tags, never pure-gain tags.
def build_tags(tokens):
    """Half-hour narrative tags, App-format:
    #$SYM 赵长鹏提及发酵 / #$SYM 同名代币 / #AI/Agent概念持续发酵 /
    #BSC链新币最密集 / #$SYM 聪明钱扎堆 / #新币发行加速
    """
    tags = []

    def add(text, coin=None):
        tags.append({
            "text": text,
            "coin_symbol": (coin or {}).get("symbol", ""),
            "coin_ca": (coin or {}).get("ca", ""),
            "coin_chain": (coin or {}).get("chain", ""),
        })

    # 1. 人物提及: #${symbol} ${人名}提及发酵
    for t in tokens:
        sym = t.get('symbol', '')
        s = f"{sym} {t.get('name','')}"
        low = s.lower()
        if "何一" in s or "heyibinance" in low:
            add(f"#{sym} 何一提及发酵", t)
        elif "赵长鹏" in s or "cz_binance" in low:
            add(f"#{sym} 赵长鹏提及发酵", t)
        elif "马斯克" in s or re.search(r"(^|[^a-z])elon([^a-z]|$)", low):
            add(f"#{sym} 马斯克提及发酵", t)

    # 2. 同名代币 (>=2 chains): #${symbol} 同名代币
    by_sym = {}
    for t in tokens:
        by_sym.setdefault(t.get("symbol", "").lower(), []).append(t)
    for sym, lst in by_sym.items():
        if sym and len({t.get("chain") for t in lst}) >= 2:
            coin = max(lst, key=lambda t: t.get("smart_count", 0))
            add(f"#{coin.get('symbol','')} 同名代币", coin)

    # 3. 概念发酵: #${concept}持续发酵 (>=2 coins per concept)
    concept_groups = {}
    for t in tokens:
        concepts = set()
        nar = (t.get("narrative") or "").strip()
        if nar:
            concepts.add(nar)
        # name-based concept detection (Meimei, Tole, etc.)
        nm = f"{t.get('symbol','')} {t.get('name','')}".lower()
        if "meimei" in nm:
            concepts.add("Meimei概念")
        if "tole" in nm:
            concepts.add("Tole概念")
        for c in concepts:
            concept_groups.setdefault(c, []).append(t)
    for concept, lst in concept_groups.items():
        if len(lst) >= 2:
            coin = max(lst, key=lambda t: t.get("smart_count", 0))
            add(f"#{concept}持续发酵", coin)

    # 4. BSC链新币最密集
    def age_h(t):
        m = re.match(r"(\d+)分钟前", t.get("age_text", ""))
        if m:
            return int(m.group(1)) / 60
        m = re.match(r"(\d+)小时前", t.get("age_text", ""))
        if m:
            return int(m.group(1))
        return 999
    new_count = {}
    for t in tokens:
        if age_h(t) <= 2:
            new_count[t.get("chain")] = new_count.get(t.get("chain"), 0) + 1
    total_new = sum(new_count.values())
    if new_count.get("bsc", 0) >= 2 and new_count["bsc"] >= max(new_count.values()):
        bsc_new = [t for t in tokens if t.get("chain") == "bsc" and age_h(t) <= 2]
        add("#BSC链新币最密集", max(bsc_new, key=lambda t: t.get("vol_1h", 0)) if bsc_new else None)

    # 5. 聪明钱扎堆: #${symbol} 聪明钱扎堆 (smart_count >= 50)
    for t in tokens:
        if t.get("smart_count", 0) >= 50:
            add(f"#{t.get('symbol','')} 聪明钱扎堆", t)

    # 6. 新币发行加速 (new coins this round >= 5)
    if total_new >= 5:
        new_coins = [t for t in tokens if age_h(t) <= 2]
        add("#新币发行加速", max(new_coins, key=lambda t: t.get("vol_1h", 0)) if new_coins else None)

    # dedupe by (text, coin_ca), cap ~12
    seen, out = set(), []
    for tg in tags:
        k = (tg["text"], tg["coin_ca"])
        if k in seen:
            continue
        seen.add(k)
        out.append(tg)
        if len(out) >= 12:
            break
    return out


# ------------------------------------------------------------ daily summary
# ------------------------------------------------------------ tag log + daily summary
TAG_LOG_KEEP_DAYS = 2


def tag_log_path(state_dir):
    return os.path.join(state_dir, "tag-log.json")


def append_tag_log(state_dir, tags):
    """Store one tag snapshot per half-hour slot (feeds the 21:00 daily summary)."""
    path = tag_log_path(state_dir)
    log = load_json(path, {})
    if not isinstance(log, dict):
        log = {}
    now = now_sh()
    today = now.strftime("%Y-%m-%d")
    slot = "%02d:%s" % (now.hour, "00" if now.minute < 30 else "30")
    entry = {
        "t": slot,
        "tags": [
            {
                "text": t.get("text", ""),
                "symbol": t.get("coin_symbol", ""),
                "chain": t.get("coin_chain", ""),
                "ca": t.get("coin_ca", ""),
            }
            for t in (tags or [])
            if t.get("text")
        ],
    }
    day = [e for e in (log.get(today) or []) if e.get("t") != slot]
    day.append(entry)
    day.sort(key=lambda e: e.get("t", ""))
    log[today] = day[-60:]
    log = {k: log[k] for k in sorted(log.keys())[-TAG_LOG_KEEP_DAYS:]}
    atomic_write(path, log)
    return len(log[today])


def summarize_period(entries, start_hour, end_hour):
    """Rank narrative tags / coins inside one time window."""
    from collections import Counter
    themes = Counter()
    coin_hits = {}
    for e in entries or []:
        try:
            hh = int(str(e.get("t", "00:00")).split(":")[0])
        except Exception:
            continue
        if not (start_hour <= hh < end_hour):
            continue
        for tg in e.get("tags") or []:
            txt = (tg.get("text") or "").strip()
            if txt:
                themes[txt] += 1
            sym = (tg.get("symbol") or "").strip()
            if sym:
                key = (sym, tg.get("chain") or "")
                c = coin_hits.setdefault(key, {
                    "symbol": sym,
                    "chain": tg.get("chain") or "",
                    "ca": tg.get("ca") or "",
                    "hits": 0,
                })
                c["hits"] += 1
    return {
        "themes": [{"text": t, "hits": n} for t, n in themes.most_common(6)],
        "coins": sorted(coin_hits.values(), key=lambda c: c["hits"], reverse=True)[:3],
    }


def maybe_summary_daily(out_dir, state_dir, tags):
    """Beijing 21:00-21:05: what the trenches played in the morning vs the afternoon."""
    path = os.path.join(out_dir, "summary-daily.json")
    now = now_sh()
    today = now.strftime("%Y-%m-%d")
    if now.hour != 21 or now.minute >= 6:
        return False, "outside 21:00-21:05 window"
    existing = load_json(path, None)
    if isinstance(existing, dict) and existing.get("date") == today:
        return False, "already generated today"
    log = load_json(tag_log_path(state_dir), {})
    entries = (log or {}).get(today) or []
    morning = summarize_period(entries, 0, 12)
    afternoon = summarize_period(entries, 12, 21)
    if not morning["themes"] and not afternoon["themes"]:
        # never publish an empty block: fall back to the current snapshot
        afternoon = {
            "themes": [{"text": t.get("text", ""), "hits": 1}
                       for t in (tags or []) if t.get("text")][:6],
            "coins": [{"symbol": t.get("coin_symbol", ""),
                       "chain": t.get("coin_chain", ""),
                       "ca": t.get("coin_ca", ""), "hits": 1}
                      for t in (tags or []) if t.get("coin_symbol")][:3],
        }
    history = {}
    if isinstance(existing, dict) and isinstance(existing.get("history"), dict):
        history = existing["history"]
    history[today] = {
        "tags": tags,
        "morning": morning["themes"],
        "afternoon": afternoon["themes"],
    }
    for d in sorted(history.keys())[:-30]:
        del history[d]
    atomic_write(path, {
        "date": today,
        "generated_at": iso_now(),
        "source": "tag-log",
        "periods": {"morning": morning, "afternoon": afternoon},
        "tags": tags,
        "history": history,
    })
    return True, ("generated %s (morning themes=%d, afternoon themes=%d)"
                  % (today, len(morning["themes"]), len(afternoon["themes"])))


# ----------------------------------------------------------------- binance
# Verified 2026-11-02 (keyless bapi):
#   listings:  GET https://www.binance.com/bapi/composite/v1/public/cms/article/list/query?type=1&catalogId=48&pageNo=1&pageSize=15
#   delistings: same with catalogId=161
#   -> {"success":true,"data":{"catalogs":[{catalogId,catalogName,articles:[
#      {id,code,title,releaseDate(ms)}]}]}}
#   alpha: GET https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list
#   -> {"data":[{symbol,name,chainName,contractAddress,listingTime(ms),offline,...}]}
# Article url: https://www.binance.com/en/support/announcement/detail/<code>
ALPHA_CHAIN_SLUG = {"bsc": "bsc", "base": "base", "solana": "solana"}


def gloss_title(title):
    """Deterministic Chinese gloss for Binance's formulaic titles (zero cost).
    Returns "" when the title matches no known pattern."""
    m = None
    if (m := re.match(r"^Binance Will List (.+?) with Seed Tag Applied$", title)):
        return f"币安将上线 {m.group(1)}（种子标签）"
    if (m := re.match(r"^Binance Will List (.+?) and Introduce\b.*$", title)):
        return f"币安将上线 {m.group(1)}"
    if (m := re.match(r"^Binance Will List (.+)$", title)):
        return f"币安将上线 {m.group(1)}"
    if (m := re.match(r"^Binance Futures Will Launch (.+?) Perpetual Contracts?\b.*$", title)):
        return f"币安合约将上线 {m.group(1)} 永续合约"
    if (m := re.match(r"^Binance Futures Will Launch (.+)$", title)):
        return f"币安合约将上线 {m.group(1)}"
    if (m := re.match(r"^Binance Futures Will Delist (.+?) Perpetual Contracts?\b.*$", title)):
        return f"币安合约将下架 {m.group(1)} 永续合约"
    if (m := re.match(r"^Binance Alpha Will Remove (.+?)(?:\s*\(.*\))?$", title)):
        return f"Binance Alpha 将移除 {m.group(1)}"
    if (m := re.match(r"^Binance Alpha Will List (.+)$", title)):
        return f"Binance Alpha 将上线 {m.group(1)}"
    if re.match(r"^Notice of Removal of Spot Trading Pairs", title):
        return "币安将移除部分现货交易对"
    if re.match(r"^Notice of Removal of Margin Trading Pairs", title):
        return "币安将移除部分杠杆交易对"
    if (m := re.match(r"^Binance Will Delist (.+?)(?:\s+on\s+\d{4}-\d{2}-\d{2})?\s*(?:\(.*\))?$", title)):
        return f"币安将下架 {m.group(1)}"
    if (m := re.match(r"^Binance Exchange Adds (.+?) bStocks Trading Pairs\b.*$", title)):
        return f"币安现货新增 bStocks 交易对：{m.group(1)}"
    if (m := re.match(r"^Binance Will Add (.+?) as Collateral Assets?\b.*$", title)):
        return f"币安将新增抵押资产：{m.group(1)}"
    if (m := re.match(r"^Binance (?:Will Add|Adds) (.+?) on (?:Cross|Isolated) Margin\b.*$", title)):
        return f"币安杠杆新增 {m.group(1)}"
    if (m := re.match(r"^Binance Will Add (.+)$", title)):
        return f"币安将新增 {m.group(1)}"
    return ""


def fetch_binance(tr_cache):
    """Binance 公告走 TG 官方频道 binance_announcements（bapi 被墙）"""
    items = []
    cutoff = now_sh() - timedelta(days=7)
    WORKER = "https://snowy-mud-3037.rj982946925.workers.dev"
    try:
        r = http.get(f"{WORKER}/?u=https://t.me/s/binance_announcements", timeout=25)
        r.raise_for_status()
        html = r.text
        blocks = re.findall(
            r'<div class="tgme_widget_message_wrap[^"]*"[^>]*>.*?'
            r'<time[^>]+datetime="([^"]+)"[^>]*>.*?</time>.*?'
            r'<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>',
            html, re.S)
        links = re.findall(
            r'href="(https://t\.me/binance_announcements/\d+)"',
            html)
        n = 0
        for i, (dt_str, txt_html) in enumerate(blocks):
            try:
                dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00')).astimezone(SH)
            except Exception:
                continue
            if dt < cutoff:
                continue
            text = re.sub(r'<br\s*/?>', '\n', txt_html)
            text = re.sub(r'<[^>]+>', '', text)
            text = re.sub(r'\s+', ' ', text).strip()
            if not text:
                continue
            title = text.split('\n')[0].strip()[:150]
            low = title.lower()
            if 'will list' in low or 'will add' in low or 'listing' in low:
                itype = 'listing'
            elif 'delist' in low or 'removal' in low or 'remove' in low:
                itype = 'delisting'
            else:
                itype = 'other'
            title_zh = gloss_title(title) or translate(
                title, "bn:" + hashlib.md5(title.encode()).hexdigest(), tr_cache)
            url = links[i] if i < len(links) else "https://t.me/s/binance_announcements"
            items.append({
                "type": itype,
                "title_zh": title_zh,
                "title_en": title,
                "time": dt.isoformat(),
                "url": url,
            })
            n += 1
        print(f"  [binance] TG channel: {n} items (7d)")
    except Exception as e:
        print(f"  [binance] TG FAILED: {e}", file=sys.stderr)
    items.sort(key=lambda i: i["time"], reverse=True)
    return {"updated_at": iso_now(), "items": items}


# -------------------------------------------------------------------- main
# Job budget: keep the whole run comfortably under the 5-minute cron slot.
JOB_BUDGET_S = 275
FAST_TICK_S = 10


def slow_lane(out_dir, state_dir, tr_cache, cache_path):
    """Runs once at job start. Returns (trench_tokens_all, tags)."""
    print("=== slow lane ===")
    # trenches first: also seeds this job's initial trench data; pool reused
    trench_files, pool = fetch_trenches()
    all_tokens = []
    for fname, payload in trench_files.items():
        if write_if_changed(os.path.join(out_dir, fname), payload):
            print(f"  [slow] wrote {fname} (source={payload.get('source')})")
        all_tokens.extend(payload["tokens"])
    # consensus (from gmgn pool)
    consensus = build_consensus(pool)
    if write_if_changed(os.path.join(out_dir, "consensus.json"), consensus):
        print(f"  [slow] wrote consensus.json lowcap={len(consensus['lowcap'])} rest={len(consensus['rest'])}")
    # 30-minute narrative tags: only refresh on :00 / :30 slots (5-min retry window)
    tags = build_tags(all_tokens)
    now = now_sh()
    if now.minute % 30 < 6:
        if write_if_changed(os.path.join(out_dir, "tags-halfhour.json"),
                            {"updated_at": iso_now(), "tags": tags}):
            print(f"  [slow] wrote tags-halfhour.json ({len(tags)} tags)")
        slots = append_tag_log(state_dir, tags)
        made, why = maybe_summary_daily(out_dir, state_dir, tags)
        print(f"  [slow] tag-log slots={slots}")
        print(f"  [slow] summary-daily {'WROTE' if made else 'skip'}: {why}")
    else:
        print("  [slow] tags/summary skip: outside :00/:30 slot")
    # douyin: official first, xxapi fallback
    try:
        dy = fetch_douyin()
        if not dy["trending"] and not dy["hotlist"]:
            print("  [slow] douyin official empty, trying xxapi", file=sys.stderr)
            dy = fetch_douyin_xxapi()
        dy["rising"] = build_douyin_rising(dy.get("hotlist") or [], state_dir)
        if write_if_changed(os.path.join(out_dir, "douyin.json"), dy):
            print(f"  [slow] wrote douyin.json trending={len(dy['trending'])} hotlist={len(dy['hotlist'])} rising={len(dy.get('rising', []))}")
    except Exception as e:
        print(f"  [slow] douyin FAILED: {e}", file=sys.stderr)
    # binance (+translations share the budget/cache)
    try:
        bn = fetch_binance(tr_cache)
        if write_if_changed(os.path.join(out_dir, "binance.json"), bn):
            print(f"  [slow] wrote binance.json ({len(bn['items'])} items)")
    except Exception as e:
        print(f"  [slow] binance FAILED: {e}", file=sys.stderr)
    write_if_changed(cache_path, tr_cache)
    return all_tokens, tags


def fast_tick(out_dir, state_dir, tr_cache, cache_path):
    """One 10s fast-lane tick. Never raises."""
    trench_files = {}
    pool = {}
    try:
        trench_files, pool = fetch_trenches()
        for fname, payload in trench_files.items():
            if write_if_changed(os.path.join(out_dir, fname), payload):
                print(f"  [fast] wrote {fname} (source={payload.get('source')})")
    except Exception as e:
        print(f"  [fast] trenches FAILED: {e}", file=sys.stderr)
    try:
        consensus = build_consensus(pool)
        if write_if_changed(os.path.join(out_dir, "consensus.json"), consensus):
            print(f"  [fast] wrote consensus.json lowcap={len(consensus['lowcap'])} rest={len(consensus['rest'])}")
    except Exception as e:
        print(f"  [fast] consensus FAILED: {e}", file=sys.stderr)
    try:
        liq = fetch_liquidity(trench_files)
        if write_if_changed(os.path.join(out_dir, "liquidity.json"), liq):
            print(f"  [fast] wrote liquidity.json ({len(liq.get('chains', []))} chains)")
    except Exception as e:
        print(f"  [fast] liquidity FAILED: {e}", file=sys.stderr)
    try:
        tweets = fetch_tweets(state_dir, tr_cache)
        if write_if_changed(os.path.join(out_dir, "tweets.json"), tweets):
            print(f"  [fast] wrote tweets.json ({len(tweets['tweets'])} tweets)")
        write_if_changed(cache_path, tr_cache)
    except Exception as e:
        print(f"  [fast] tweets FAILED: {e}", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser(description="GitHub Actions fetch loop (slow+fast lanes)")
    ap.add_argument("--out-dir", default=os.path.join(SCRIPT_DIR, "data"))
    ap.add_argument("--state-dir", default=None,
                    help="dir for tweet-store.json / translation-cache.json (default: out-dir)")
    args = ap.parse_args()
    out_dir = os.path.abspath(args.out_dir)
    state_dir = os.path.abspath(args.state_dir) if args.state_dir else out_dir
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(state_dir, exist_ok=True)
    cache_path = os.path.join(state_dir, "translation-cache.json")

    tr_cache = load_json(cache_path, {})
    if not isinstance(tr_cache, dict):
        tr_cache = {}

    job_start = time.time()
    print(f"[{iso_now()}] fetch_loop start -> {out_dir} (state: {state_dir})")

    slow_lane(out_dir, state_dir, tr_cache, cache_path)

    print("=== fast lane ===")
    deadline = job_start + JOB_BUDGET_S
    tick = 0
    while time.time() < deadline:
        tick += 1
        t0 = time.time()
        print(f"  [fast] tick {tick}")
        fast_tick(out_dir, state_dir, tr_cache, cache_path)
        elapsed = time.time() - t0
        time.sleep(max(0, FAST_TICK_S - elapsed))

    write_if_changed(cache_path, tr_cache)
    print(f"[{iso_now()}] fetch_loop done "
          f"({time.time() - job_start:.0f}s, new translations this job: {_translate_used})")


if __name__ == "__main__":
    main()
