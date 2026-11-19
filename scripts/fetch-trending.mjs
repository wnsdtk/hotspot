// scripts/fetch-trending.mjs — 各公链今日热门（GeckoTerminal 公开 API，无需 key）
// 输出 data/trending.json：{ updated_at, chains: { BSC: [...], Solana: [...], ... } }
import { writeFileSync, mkdirSync, readFileSync, existsSync } from 'node:fs';

const OUT = 'data/trending.json';
const CHAINS = [
  ['BSC', 'bsc'],
  ['Solana', 'solana'],
  ['Ethereum', 'eth'],
  ['Base', 'base'],
];
const TOPN = 6;
const BLACKLIST = new Set(["USDT","USDC","PYUSD","USDS","DAI","TUSD","FDUSD","XAUT","PAXG","WBTC","WETH","stETH","wstETH","cbBTC","SOL","ETH","BNB","BTC","USDe","sUSDe","USD1","BUSD"]);


function nowCST() {
  return new Date(Date.now() + 8 * 3600 * 1000).toISOString().replace('Z', '+08:00');
}
const num = (v) => (v === null || v === undefined || Number.isNaN(Number(v)) ? null : Number(v));

async function trending(network) {
  const url = `https://api.geckoterminal.com/api/v2/networks/${network}/trending_pools?page=1&duration=24h`;
  const r = await fetch(url, { headers: { accept: 'application/json', 'user-agent': 'hotspot-bot' } });
  if (!r.ok) throw new Error(`${network}: http ${r.status}`);
  const j = await r.json();
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
      vol24: num(a.volume_usd && a.volume_usd.h24),
      txns24: num(a.transactions && a.transactions.h24 && a.transactions.h24.buys) || 0,
      mcap: num(a.fdv_usd),
    });
  }
  return rows
    .filter((x) => x.symbol && !BLACKLIST.has(x.symbol.toUpperCase()))
    .sort((x, y) => (y.vol24 || 0) - (x.vol24 || 0))
    .slice(0, TOPN);
}

const chains = {};
for (const [label, net] of CHAINS) {
  let rows = [];
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      rows = await trending(net);
      break;
    } catch (e) {
      console.log(`${label}: attempt ${attempt + 1} failed ${e.message}`);
      await new Promise((r) => setTimeout(r, 8000));
    }
  }
  chains[label] = rows;
  console.log(`${label}: ${rows.length}`);
  await new Promise((r) => setTimeout(r, 3000));
}

const payload = { updated_at: nowCST(), chains };
mkdirSync('data', { recursive: true });
writeFileSync(OUT, JSON.stringify(payload, null, 1) + '\n', 'utf8');
console.log('written', OUT);
