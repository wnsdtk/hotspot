# scripts/gen-narr.py — 为战壕里的新币生成「叙事分析」，写入 data/narr.json
# 复用 GitHub Secrets 里已有的 DEEPSEEK_API_KEY（与每日推文总结同一把）
# 成本控制：只处理 7 天内新币、每次最多 N 条、已生成过的跳过
import json, os, sys, time, urllib.request, urllib.error

OUT = "data/narr.json"
MAX_PER_RUN = int(os.environ.get("NARR_MAX", "6"))
AGE_LIMIT_MIN = 7 * 1440          # 7 天
CHAIN_FILES = ["data/trench-sol.json", "data/trench-bsc.json", "data/trench-rb.json"]

PROMPT = """你是一个加密 Meme 币研究员。请基于下面信息，写一段中文叙事分析。

代币名称：{name}
符号：{symbol}
链：{chain}
标签：{tag}
市值：${mcap}
流动性：${liq}
上线时间：{age}
相关推文：{x}

要求：
1. 只根据名称、符号、标签做合理推断，明确标注不确定的地方，不要编造具体团队信息。
2. 分四段，每段 60-120 字：
   background（名字来历 / 梗的出处）
   team（团队与来源，若不明就直说匿名/无法确认）
   risk（风险提示：流动性、竞争、跑路风险等）
   summary（一句话结论）
3. 输出必须是一个 JSON 对象，键为 background / team / risk / summary，值为中文字符串，不要任何多余内容、不要 markdown 代码块。"""


def load(p):
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def age_minutes(text):
    s = str(text or "")
    import re
    m = re.search(r"(\d+)\s*(分钟|小时|天|周)", s)
    if not m:
        return 10 ** 9
    n, u = int(m.group(1)), m.group(2)
    return n * {"分钟": 1, "小时": 60, "天": 1440, "周": 10080}[u]


def call_llm(key, info):
    body = json.dumps({
        "model": "deepseek-chat",
        "messages": [{"role": "user", "content": PROMPT.format(**info)}],
        "temperature": 0.5,
        "max_tokens": 900,
    }).encode()
    req = urllib.request.Request(
        "https://api.deepseek.com/chat/completions",
        data=body,
        headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=90) as r:
        raw = json.loads(r.read().decode())
    txt = raw["choices"][0]["message"]["content"].strip()
    if txt.startswith("```"):
        txt = txt.strip("`")
        if txt.startswith("json"):
            txt = txt[4:]
    return json.loads(txt)


def main():
    key = (os.environ.get("DEEPSEEK_API_KEY") or "").strip()
    if not key:
        print("no DEEPSEEK_API_KEY, skip")
        return

    cur = load(OUT) or {"updated_at": None, "items": {}}
    items = cur.get("items") or {}

    # 收集候选
    cand = []
    for f in CHAIN_FILES:
        d = load(f)
        if not d:
            continue
        for t in d.get("tokens") or []:
            ca = (t.get("ca") or "").strip()
            if not ca or ca.lower() in items:
                continue
            if age_minutes(t.get("age_text")) > AGE_LIMIT_MIN:
                continue
            cand.append(t)

    if not cand:
        print("nothing to generate")
        return

    # 优先聪明钱多的
    cand.sort(key=lambda x: -(int(x.get("smart_count") or 0)))
    todo = cand[:MAX_PER_RUN]
    print(f"candidates={len(cand)} todo={len(todo)}")

    ok = 0
    for t in todo:
        info = {
            "name": t.get("name") or "",
            "symbol": t.get("symbol") or "",
            "chain": t.get("chain") or "",
            "tag": t.get("narrative") or t.get("smart_label") or "",
            "mcap": round(float(t.get("market_cap") or 0)),
            "liq": round(float(t.get("liquidity") or 0)),
            "age": t.get("age_text") or "",
            "x": t.get("x_url") or "无",
        }
        try:
            data = call_llm(key, info)
            if not isinstance(data, dict):
                continue
            items[(t.get("ca") or "").lower()] = {
                "symbol": info["symbol"],
                "name": info["name"],
                "chain": info["chain"],
                "ts": int(time.time()),
                "background": str(data.get("background") or ""),
                "team": str(data.get("team") or ""),
                "risk": str(data.get("risk") or ""),
                "summary": str(data.get("summary") or ""),
            }
            ok += 1
            print("ok", info["symbol"])
        except urllib.error.HTTPError as e:
            print("http", e.code, info["symbol"])
            if e.code in (401, 402, 429):
                break
        except Exception as e:
            print("fail", info["symbol"], str(e)[:80])
        time.sleep(1.5)

    cur["items"] = items
    cur["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%S+08:00")
    os.makedirs("data", exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(cur, f, ensure_ascii=False, indent=1)
    print(f"written {OUT} generated={ok} total={len(items)}")


if __name__ == "__main__":
    main()
