// scripts/fetch-trending.mjs — 各公链「在玩什么」
// 主源：Dexscreener 公开 API（token-boosts / token-profiles 发现 + /latest/dex/tokens 取详情）
// 兜底：GeckoTerminal trending_pools（Dexscreener 覆盖不足的链）
// 输出 data/trending.json：{ updated_at, chains: { BSC: [...], Solana: [...], ... } }
import { writeFileSync, mkdirSync } from 'node:fs';

const OUT = 'data/trending.json';
const TOPN = 8;
const UA = { accept: 'application/json', 'user-agent': 'hotspot-bot/1.0' };

const DS_CHAIN = { BSC: 'bsc', Solana: 'solana', Ethereum: 'ethereum', Base: 'base' };
const GT_NET = { BSC: 'bsc', Solana: 'solana', Ethereum: 'eth', Base: 'base' };

const BLACKLIST = new Set([
  'USDT','USDC','PYUSD','USDS','DAI','TUSD','FDUSD','XAUT','PAXG','WBTC','WETH','STETH','WSTETH',
  'CBBTC','SOL','ETH','BNB','BTC','USDE','SUSDE','USD1','BUSD','USDC.E','USDT.E',
]);

const nowCST = () => new Date(Date.now() + 8 * 3600 * 1000).toISOString().replace('Z', '+08:00');
const num = (v) => (v === null || v === undefined || Number.isNaN(Number(v)) ? null : Number(v));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function jget(url) {
  const r = await fetch(url, { headers: UA });
  if (!r.ok) throw new Error(`http ${r.status} ${url.slice(0, 60)}`);
  return r.json();
}

// ---------- 1. Dexscreener 发现候选 ----------
async function discover() {
  const buckets = {};
  for (const ep of [
    'https://api.dexscreener.com/token-boosts/top/v1',
    'https://api.dexscreener.com/token-boosts/latest/v1',
    'https://api.dexscreener.com/token-profiles/latest/v1',
  ]) {
    try {
      const arr = await jget(ep);
      for (const it of arr || []) {
        const cid = it.chainId;
        const addr = it.tokenAddress;
        if (!cid || !addr) continue;
        if (!buckets[cid]) buckets[cid] = new Set();
        buckets[cid].add(addr);
      }
    } catch (e) {
      console.log('discover fail', ep, e.message);
    }
    await sleep(800);
  }
  return buckets;
}

async function details(addresses) {
  const out = {};
  for (let i = 0; i < addresses.length; i += 25) {
    const batch = addresses.slice(i, i + 25);
    try {
      const j = await jget('https://api.dexscreener.com/latest/dex/tokens/' + batch.join(','));
      for (const p of j.pairs || []) {
        const base = p.baseToken || {};
        const key = (base.address || '').toLowerCase();
        if (!key) continue;
        const vol = num(p.volume && p.volume.h24) || 0;
        const prev = out[key];
        if (!prev || vol > prev.vol24) {
          out[key] = {
            symbol: base.symbol || '',
            name: base.name || '',
            ca: base.address || '',
            price: num(p.priceUsd),
            chg24: num(p.priceChange && p.priceChange.h24),
            vol24: vol,
            mcap: num(p.fdv),
            url: p.url || '',
          };
        }
      }
    } catch (e) {
      console.log('details fail', e.message);
    }
    await sleep(700);
  }
  return out;
}

// ---------- 2. GeckoTerminal 兜底 ----------
async function gecko(network) {
  const url = `https://api.geckoterminal.com/api/v2/networks/${network}/trending_pools?page=1&duration=24h`;
  const j = await jget(url);
  const rows = [];
  for (const p of j.data || []) {
    const a = p.attributes || {};
    const baseId = ((p.relationships && p.relationships.base_token && p.relationships.base_token.data && p.relationships.base_token.data.id) || '').replace(/^[a-z]+_/, '');
    rows.push({
      symbol: (a.name || '').split('/')[0].trim(),
      name: a.name || '',
      ca: baseId,
      price: num(a.base_token_price_usd),
      chg24: num(a.price_change_percentage && a.price_change_percentage.h24),
      vol24: num(a.volume_usd && a.volume_usd.h24) || 0,
      mcap: num(a.fdv_usd),
      url: '',
    });
  }
  return rows;
}

// ---------- 主流程 ----------
const buckets = await discover();
const chains = {};

for (const [label, cid] of Object.entries(DS_CHAIN)) {
  let rows = [];
  const addrs = Array.from(buckets[cid] || []);
  if (addrs.length) {
    const det = await details(addrs);
    rows = Object.values(det).filter((x) => x.symbol && !BLACKLIST.has(x.symbol.toUpperCase()));
  }
  if (rows.length < 4) {
    try {
      const g = (await gecko(GT_NET[label])).filter((x) => x.symbol && !BLACKLIST.has(x.symbol.toUpperCase()));
      const have = new Set(rows.map((r) => (r.symbol || '').toUpperCase()));
      for (const gx of g) {
        if (rows.length >= 6) break;
        if (have.has(gx.symbol.toUpperCase())) continue;
        have.add(gx.symbol.toUpperCase());
        rows.push(gx);
      }
    } catch (e) {
      console.log(`${label} gecko fallback fail`, e.message);
    }
  }
  rows.sort((a, b) => (b.vol24 || 0) - (a.vol24 || 0));
  chains[label] = rows.slice(0, TOPN);
  console.log(`${label}: ${chains[label].length} (candidates ${addrs.length})`);
  await sleep(1200);
}

mkdirSync('data', { recursive: true });
writeFileSync(OUT, JSON.stringify({ updated_at: nowCST(), chains }, null, 1) + '\n', 'utf8');
console.log('written', OUT);
