// scripts/fetch-weibo.mjs — 抓微博热搜，算出「新进榜 / 排名跃升」，写入 data/weibo.json
// 运行环境：GitHub Actions（node 20+），工作目录 = data 分支 checkout
import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'node:fs';

const OUT = 'data/weibo.json';
const API = 'https://v2.xxapi.cn/api/weibohot';
const RANK_JUMP = 8; // 前进 8 名及以上算「跃升」

function nowCST() {
  const d = new Date(Date.now() + 8 * 3600 * 1000);
  return d.toISOString().replace('Z', '+08:00');
}

async function fetchList() {
  const res = await fetch(API, { headers: { 'user-agent': 'Mozilla/5.0' } });
  if (!res.ok) throw new Error('http ' + res.status);
  const j = await res.json();
  const arr = Array.isArray(j?.data) ? j.data : [];
  return arr
    .filter((x) => x && x.title)
    .map((x) => ({
      title: x.title,
      hot: x.hot || '',
      index: Number(x.index) || 0,
      url: x.url || 'https://s.weibo.com/weibo?q=' + encodeURIComponent(x.title),
    }))
    .filter((x) => x.index > 0)
    .sort((a, b) => a.index - b.index);
}

let prevFile = null;
if (existsSync(OUT)) {
  try { prevFile = JSON.parse(readFileSync(OUT, 'utf8')); } catch {}
}

const list = await fetchList();
if (!list.length) {
  console.error('empty list, abort');
  process.exit(1);
}

const prevMap = {};
if (prevFile) {
  const prevItems = Array.isArray(prevFile.list) ? prevFile.list : [];
  for (const it of prevItems) if (it && it.title) prevMap[it.title] = it.index;
}

const rising = [];
for (const it of list) {
  const p = prevMap[it.title];
  if (!p) rising.push({ word: it.title, url: it.url, tag: '新上榜', from: null, to: it.index });
  else if (p - it.index >= RANK_JUMP) rising.push({ word: it.title, url: it.url, tag: '↑' + (p - it.index) + '位', from: p, to: it.index });
}
rising.sort((a, b) => a.to - b.to);

const payload = {
  updated_at: nowCST(),
  count: list.length,
  rising,
  list,
  prev: prevFile ? { updated_at: prevFile.updated_at, list: prevFile.list } : null,
};

mkdirSync('data', { recursive: true });
writeFileSync(OUT, JSON.stringify(payload, null, 1) + '\n', 'utf8');
console.log(`weibo: ${list.length} items, rising ${rising.length}, prev=${!!prevFile}`);
