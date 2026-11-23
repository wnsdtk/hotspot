-- =====================================================================
-- hotspot 会员系统 · Supabase 数据库结构
-- 在 Supabase 控制台 → SQL Editor → 粘贴运行（只需跑一次）
-- =====================================================================

-- ---------- 1. 用户档案（注册即创建，试用 3 天） ----------
create table if not exists public.profiles (
  id          uuid primary key references auth.users(id) on delete cascade,
  username    text unique not null,
  expires_at  timestamptz not null default now() + interval '3 days',
  is_admin    boolean not null default false,
  created_at  timestamptz not null default now()
);

alter table public.profiles enable row level security;

-- 只能读自己的档案
drop policy if exists "own profile read" on public.profiles;
create policy "own profile read" on public.profiles
  for select using (auth.uid() = id);

-- 用户不能自己改 expires_at / is_admin（无 update 策略 → 一律拒绝）

-- ---------- 2. 支付记录 ----------
create table if not exists public.payments (
  id          bigserial primary key,
  user_id     uuid not null references auth.users(id) on delete cascade,
  tx_hash     text unique not null,
  months      int  not null check (months between 1 and 36),
  amount_bnb  numeric(20,8) not null,
  created_at  timestamptz not null default now()
);

alter table public.payments enable row level security;

drop policy if exists "own payments read" on public.payments;
create policy "own payments read" on public.payments
  for select using (auth.uid() = user_id);

-- ---------- 3. 注册时自动建档 + 送 3 天 ----------
create or replace function public.handle_new_user()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  uname text;
begin
  uname := coalesce(
    nullif(new.raw_user_meta_data->>'username', ''),
    split_part(new.email, '@', 1)
  );
  insert into public.profiles (id, username, expires_at)
  values (new.id, uname, now() + interval '3 days')
  on conflict (id) do nothing;
  return new;
end;
$$;

drop trigger if exists on_auth_user_created on auth.users;
create trigger on_auth_user_created
  after insert on auth.users
  for each row execute function public.handle_new_user();

-- ---------- 4. 后台：给指定用户加时长（仅管理员） ----------
create or replace function public.admin_grant(p_username text, p_days int)
returns timestamptz
language plpgsql
security definer
set search_path = public
as $$
declare
  me uuid := auth.uid();
  am_admin boolean;
  new_exp timestamptz;
begin
  select is_admin into am_admin from public.profiles where id = me;
  if not coalesce(am_admin, false) then
    raise exception 'not admin';
  end if;
  if p_days < 1 or p_days > 3650 then
    raise exception 'bad days';
  end if;

  update public.profiles
     set expires_at = greatest(expires_at, now()) + (p_days || ' days')::interval
   where username = lower(p_username)
  returning expires_at into new_exp;

  if new_exp is null then
    raise exception 'user not found';
  end if;
  return new_exp;
end;
$$;

-- ---------- 5. 后台：用户列表（仅管理员） ----------
create or replace function public.admin_users()
returns table (username text, expires_at timestamptz, is_admin boolean, created_at timestamptz)
language plpgsql
security definer
set search_path = public
as $$
declare
  am_admin boolean;
begin
  select p.is_admin into am_admin from public.profiles p where p.id = auth.uid();
  if not coalesce(am_admin, false) then
    raise exception 'not admin';
  end if;
  return query
    select p.username, p.expires_at, p.is_admin, p.created_at
      from public.profiles p
     order by p.created_at desc
     limit 500;
end;
$$;

-- ---------- 6. 用户名统一小写（避免大小写重复） ----------
create or replace function public.lower_username()
returns trigger language plpgsql as $$
begin
  new.username := lower(new.username);
  return new;
end;
$$;

drop trigger if exists trg_lower_username on public.profiles;
create trigger trg_lower_username
  before insert or update on public.profiles
  for each row execute function public.lower_username();

-- ---------- 7. 把自己设为管理员（建成账号后执行一次） ----------
-- 把下面的 'dajibaya' 换成你自己的账号名：
-- update public.profiles set is_admin = true where username = 'dajibaya';

-- ---------- 8. 显式授权（即使没开"自动授予"也能用） ----------
grant usage on schema public to anon, authenticated;
grant select on public.profiles to authenticated;
grant select on public.payments to authenticated;
grant execute on function public.admin_users() to authenticated;
grant execute on function public.admin_grant(text,int) to authenticated;
