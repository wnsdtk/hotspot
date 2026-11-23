// Supabase Edge Function: verify-payment
// 作用：校验一笔 BSC 链上的 BNB 转账，确认后给用户延长会员时长
// 部署：Supabase 控制台 → Edge Functions → 新建 verify-payment → 粘贴本文件 → Deploy
//
// 需要在 Edge Function 的 Secrets 里配置：
//   PAY_TO_ADDRESS = 你的收款地址（如 0xabc...）
//   BNB_PRICE_MONTH = 0.05        （每月价格）
//   RPC_URL        = https://bsc-dataseed.binance.org
//
// 前端调用：POST /functions/v1/verify-payment  { txHash: "0x...", months: 1 }
// 需要带 Authorization: Bearer <用户 access_token>

import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

const RPC = Deno.env.get('RPC_URL') || 'https://bsc-dataseed.binance.org';
const PAY_TO = (Deno.env.get('PAY_TO_ADDRESS') || '').toLowerCase();
const PRICE = Number(Deno.env.get('BNB_PRICE_MONTH') || '0.05');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
};

async function rpc(method: string, params: unknown[]) {
  const r = await fetch(RPC, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
  });
  const j = await r.json();
  if (j.error) throw new Error(j.error.message || 'rpc error');
  return j.result;
}

Deno.serve(async (req) => {
  if (req.method === 'OPTIONS') return new Response('ok', { headers: CORS });

  try {
    if (!PAY_TO) throw new Error('PAY_TO_ADDRESS 未配置');

    const auth = req.headers.get('Authorization') || '';
    const jwt = auth.replace('Bearer ', '');
    if (!jwt) throw new Error('缺少登录凭证');

    const supabase = createClient(
      Deno.env.get('SUPABASE_URL')!,
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!,
      { global: { headers: { Authorization: auth } } },
    );

    const { data: userData, error: userErr } = await supabase.auth.getUser(jwt);
    if (userErr || !userData?.user) throw new Error('登录状态无效');
    const uid = userData.user.id;

    const body = await req.json();
    const txHash: string = String(body.txHash || '').toLowerCase();
    const months = Math.max(1, Math.min(36, Number(body.months) || 1));

    if (!/^0x[0-9a-f]{64}$/.test(txHash)) throw new Error('交易哈希格式不正确');

    // 1) 取交易和回执
    const tx = await rpc('eth_getTransactionByHash', [txHash]);
    if (!tx) throw new Error('链上找不到这笔交易（可能还没确认）');

    const receipt = await rpc('eth_getTransactionReceipt', [txHash]);
    if (!receipt) throw new Error('交易尚未确认，请稍后再试');
    if (receipt.status !== '0x1') throw new Error('交易失败（status=0）');

    // 2) 校验收款地址、金额、币种
    const to = (tx.to || '').toLowerCase();
    if (to !== PAY_TO) throw new Error('收款地址不匹配');

    const valueWei = BigInt(tx.value || '0x0');
    const needWei = BigInt(Math.round(PRICE * months * 1e18));
    if (valueWei < needWei) {
      throw new Error(`金额不足：需要 ${PRICE * months} BNB`);
    }

    // 3) 防重放：哈希只能用一次
    const { data: dup } = await supabase
      .from('payments').select('id').eq('tx_hash', txHash).maybeSingle();
    if (dup) throw new Error('这笔交易已经用过了');

    const amountBnb = Number(valueWei) / 1e18;

    // 4) 记账
    const { error: insErr } = await supabase.from('payments').insert({
      user_id: uid, tx_hash: txHash, months, amount_bnb: amountBnb,
    });
    if (insErr) throw new Error('记录失败：' + insErr.message);

    // 5) 延长会员
    const { data: prof } = await supabase
      .from('profiles').select('expires_at').eq('id', uid).maybeSingle();
    const base = prof?.expires_at && new Date(prof.expires_at) > new Date()
      ? new Date(prof.expires_at) : new Date();
    base.setDate(base.getDate() + months * 30);

    const { error: updErr } = await supabase
      .from('profiles').update({ expires_at: base.toISOString() }).eq('id', uid);
    if (updErr) throw new Error('更新会员失败：' + updErr.message);

    return new Response(JSON.stringify({
      ok: true, months, amountBnb, expiresAt: base.toISOString(),
    }), { headers: { ...CORS, 'content-type': 'application/json' } });

  } catch (e) {
    return new Response(JSON.stringify({ ok: false, error: (e as Error).message }), {
      status: 400, headers: { ...CORS, 'content-type': 'application/json' },
    });
  }
});
