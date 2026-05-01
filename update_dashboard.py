"""
AutobotEx Dashboard Updater
- 매일 장 마감 후(15:40 예정) 실행
- ISA / Pension / IRP 3계좌 잔고 + 오늘 매매내역을 한투 KIS API로 조회
- daily.json에 새 행 추가 (가중치 기반 전략별 잔고 분리)
- git push로 GitHub Pages 자동 갱신
- Discord 웹훅으로 완료/실패 알림
"""

import sys
import io
import os
import re
import json
import subprocess
import datetime
import traceback

import requests

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)

DASHBOARD_DIR = r"C:\AutobotEx\dashboard"
DAILY_JSON    = os.path.join(DASHBOARD_DIR, "data", "daily.json")
PYTHON_EXE    = sys.executable

DISCORD_WEBHOOK = (
    "https://discord.com/api/webhooks/1493967027000443010/"
    "-sqgdVy8BQ-G0LxwX51mHCwV1nuqgJieIznyV8_5Zaq18nqKXQ9VEE-N77oCdQnbHT0D"
)

# 종목코드 → 전략 매핑. 잔고는 가중치로 분배하므로, 매매내역 표시용으로만 사용.
# ISA의 133690은 Hybrid/SmartSplit 양쪽 존재 → Hybrid로 기본 할당.
ACCOUNTS = {
    "ISA": {
        "dir": r"C:\AutobotEx\ISA",
        "weights": {"hybrid": 0.55, "smartsplit": 0.45},
        "stock_map": {
            "122630": "smartsplit",
            "091160": "smartsplit",
            "161510": "smartsplit",
        },
        "default_strategy": "hybrid",
    },
    "Pension": {
        "dir": r"C:\AutobotEx\Pension",
        "weights": {"hybrid": 0.75, "smartsplit": 0.25},
        "stock_map": {
            "091160": "smartsplit",
            "463250": "smartsplit",
        },
        "default_strategy": "hybrid",
    },
    "IRP": {
        "dir": r"C:\AutobotEx\IRP",
        "weights": {"hybrid": 0.70, "safe": 0.30},
        "stock_map": {},
        "default_strategy": "hybrid",
    },
}

STRATEGY_LABEL = {"hybrid": "Hybrid", "smartsplit": "SmartSplit", "safe": "안전자산"}

# 한국거래소 휴장일 (주말 외) — 2026년분 하드코딩.
# 매년 1월 KRX가 다음해 휴장일을 공지하면 갱신할 것.
KR_HOLIDAYS = {
    "2026-01-01",  # 신정
    "2026-02-16", "2026-02-17", "2026-02-18",  # 설날 연휴
    "2026-03-01",  # 삼일절(일요일) — 대체휴일은 별도 조회 필요
    "2026-05-01",  # 근로자의 날
    "2026-05-05",  # 어린이날
    "2026-05-25",  # 부처님오신날
    "2026-06-06",  # 현충일(토)
    "2026-08-15",  # 광복절(토)
    "2026-09-24", "2026-09-25", "2026-09-26",  # 추석 연휴
    "2026-10-03",  # 개천절(토)
    "2026-10-09",  # 한글날(금)
    "2026-12-25",  # 성탄절
    "2026-12-31",  # 연말 휴장
}


def is_market_closed(date_str):
    """주말 또는 한국거래소 휴장일이면 True."""
    try:
        d = datetime.date.fromisoformat(date_str)
    except Exception:
        return False
    if d.weekday() >= 5:
        return True
    return date_str in KR_HOLIDAYS


# ───────────────────────────── 봇 폴더에서 실행할 inline 스크립트
# 출력은 ###JSON_BEGIN###...###JSON_END###로 감싸서, 봇 모듈의 부수 print를 무시하게 함.

BALANCE_SCRIPT = r"""
import sys, io, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
import KIS_Common as Common
import KIS_API_Helper_KR as KR
Common.SetChangeMode("REAL")
b = KR.GetBalance() or {}
stocks = KR.GetMyStockList() or []
def f(x):
    try: return float(x)
    except: return 0.0
def i(x):
    try: return int(float(x))
    except: return 0
out = {
    "total": f(b.get("TotalMoney")),
    "stock_money": f(b.get("StockMoney")),
    "cash": f(b.get("RemainMoney")),
    "stocks": [
        {"code": s.get("StockCode"), "name": s.get("StockName"),
         "value": f(s.get("StockNowMoney")), "qty": i(s.get("StockQty"))}
        for s in stocks if isinstance(s, dict)
    ],
}
print("###JSON_BEGIN###" + json.dumps(out, ensure_ascii=False) + "###JSON_END###")
"""

ORDERS_SCRIPT = r"""
import sys, io, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
import KIS_Common as Common
import KIS_API_Helper_KR as KR
Common.SetChangeMode("REAL")
orders = KR.GetOrderList(side="ALL", status="CLOSE", limit=0) or []
today = Common.GetNowDateStr("KR")
filtered = []
if isinstance(orders, list):
    for o in orders:
        if not isinstance(o, dict):
            continue
        if o.get("OrderDate") != today:
            continue
        if str(o.get("OrderIsCancel", "")).upper() == "Y":
            continue
        try:
            qty = int(float(o.get("OrderResultAmt", 0)))
        except Exception:
            qty = 0
        if qty <= 0:
            continue
        filtered.append(o)
print("###JSON_BEGIN###" + json.dumps(filtered, ensure_ascii=False) + "###JSON_END###")
"""

JSON_RE = re.compile(r"###JSON_BEGIN###(.*?)###JSON_END###", re.DOTALL)


def run_subprocess(cwd, script, label):
    """봇 폴더에서 inline 스크립트 실행, JSON 마커 추출."""
    res = subprocess.run(
        [PYTHON_EXE, "-X", "utf8", "-c", script],
        cwd=cwd, capture_output=True, text=True,
        encoding="utf-8", errors="replace", timeout=90,
    )
    if res.returncode != 0:
        raise RuntimeError(
            f"[{label}] subprocess rc={res.returncode}\n"
            f"STDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}"
        )
    m = JSON_RE.search(res.stdout)
    if not m:
        raise RuntimeError(
            f"[{label}] JSON marker not found in stdout:\n{res.stdout}\nSTDERR:\n{res.stderr}"
        )
    return json.loads(m.group(1))


def split_balance(account_key, balance):
    """총잔고(현금 포함)를 가중치로 전략별 분배."""
    weights = ACCOUNTS[account_key]["weights"]
    total = balance["total"]
    return {strategy: total * w for strategy, w in weights.items()}


def normalize_trades(account_key, raw_orders):
    """KIS GetOrderList 원시 형식 → daily.json trades 표준 dict 리스트."""
    meta = ACCOUNTS[account_key]
    smap, default = meta["stock_map"], meta["default_strategy"]
    out = []
    for o in raw_orders:
        code = str(o.get("OrderStock", "") or "")
        side = "매수" if o.get("OrderSide") == "Buy" else "매도"
        try:
            qty = int(float(o.get("OrderResultAmt", 0)))
        except Exception:
            qty = 0
        try:
            price = int(round(float(o.get("OrderAvgPrice", 0))))
        except Exception:
            price = 0
        strategy_key = smap.get(code, default)
        out.append({
            "account": account_key,
            "strategy": STRATEGY_LABEL.get(strategy_key, strategy_key),
            "action": side,
            "stock_code": code,
            "stock_name": o.get("OrderStockName", "") or "",
            "qty": qty,
            "price": price,
            "amount": qty * price,
        })
    return out


def compute_irp_signal(daily_data, irp_split):
    """
    IRP 시그널 이론값.
    hybrid_irp_state.json이 없거나 last_allocation이 비면 actual=signal로 폴백.
    봇이 실행되면 last_allocation 구조에 맞춰 추후 정밀 계산 추가 가능.
    """
    state_path = os.path.join(ACCOUNTS["IRP"]["dir"], "hybrid_irp_state.json")
    fallback = {
        "hybrid_signal": irp_split["hybrid"],
        "safe_signal":   irp_split["safe"],
    }
    if not os.path.exists(state_path):
        return fallback
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            state = json.load(f)
        alloc = state.get("last_allocation") or {}
        if not alloc:
            return fallback
        # 정밀 계산은 봇의 실제 last_allocation 구조 확인 후 보강.
        # 현재는 안전하게 actual을 signal로 사용.
        return fallback
    except Exception as e:
        print(f"  [IRP signal] 상태파일 읽기 실패, 폴백 사용: {e}")
        return fallback


def load_daily():
    with open(DAILY_JSON, "r", encoding="utf-8") as f:
        return json.load(f)


def save_daily(daily):
    with open(DAILY_JSON, "w", encoding="utf-8") as f:
        json.dump(daily, f, ensure_ascii=False, indent=2)


def upsert_record(daily, new_record):
    """같은 날짜가 있으면 덮어쓰기."""
    today = new_record["date"]
    daily["daily_records"] = [r for r in daily["daily_records"] if r.get("date") != today]
    daily["daily_records"].append(new_record)
    daily["daily_records"].sort(key=lambda r: r.get("date", ""))
    daily["last_updated"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def git_run(args):
    return subprocess.run(
        ["git"] + args, cwd=DASHBOARD_DIR,
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        timeout=60,
    )


def git_push(today):
    add = git_run(["add", "-A"])
    if add.returncode != 0:
        raise RuntimeError(f"git add failed: {add.stderr}")
    status = git_run(["status", "--porcelain"])
    if not status.stdout.strip():
        print("  git: 변경사항 없음 (push 생략)")
        return False
    commit = git_run(["commit", "-m", f"daily update {today}"])
    if commit.returncode != 0:
        raise RuntimeError(f"git commit failed:\n{commit.stdout}\n{commit.stderr}")
    push = git_run(["push", "origin", "main"])
    if push.returncode != 0:
        raise RuntimeError(f"git push failed:\n{push.stdout}\n{push.stderr}")
    print("  git push: ok")
    return True


def discord_notify(msg):
    try:
        requests.post(
            DISCORD_WEBHOOK,
            json={
                "content": "@everyone\n" + msg,
                "allowed_mentions": {"parse": ["everyone"]},
            },
            timeout=10,
        )
    except Exception as e:
        print(f"discord notify failed: {e}")


def fmt_won(v):
    return f"₩{int(round(v)):,}"


def fmt_signed_won(v):
    v = int(round(v))
    if v == 0:
        return "₩0"
    return ("+" if v > 0 else "-") + "₩" + f"{abs(v):,}"


def fmt_pct(v):
    if v == 0:
        return "0.00%"
    return ("+" if v > 0 else "") + f"{v:.2f}%"


def build_daily_alert(daily_pct, daily_amt, total_actual):
    """단계별 일일 수익률 경고 메시지 생성. 해당 없으면 None."""
    if daily_pct >= 5:
        return (
            "🚀 일일 수익률 경고 (급등)\n"
            f"수익률: {fmt_pct(daily_pct)}\n"
            f"수익금: {fmt_signed_won(daily_amt)}\n"
            f"총자산: {fmt_won(total_actual)}\n"
            "즉시 확인 필요!"
        )
    if daily_pct <= -5:
        return (
            "🚨 일일 수익률 경고 (급락)\n"
            f"수익률: {fmt_pct(daily_pct)}\n"
            f"손실금: {fmt_signed_won(daily_amt)}\n"
            f"총자산: {fmt_won(total_actual)}\n"
            "즉시 확인 필요!"
        )
    if daily_pct >= 3:
        return (
            "⚠️ 일일 수익률 주의 (급등)\n"
            f"수익률: {fmt_pct(daily_pct)}\n"
            f"수익금: {fmt_signed_won(daily_amt)}\n"
            f"총자산: {fmt_won(total_actual)}\n"
            "확인 바랍니다."
        )
    if daily_pct <= -3:
        return (
            "⚠️ 일일 수익률 주의 (급락)\n"
            f"수익률: {fmt_pct(daily_pct)}\n"
            f"손실금: {fmt_signed_won(daily_amt)}\n"
            f"총자산: {fmt_won(total_actual)}\n"
            "확인 바랍니다."
        )
    return None


def main():
    today = datetime.date.today().isoformat()
    market_closed = is_market_closed(today)
    mode_tag = "(휴장일 NAV 반영)" if market_closed else "(평일)"
    print(f"[{today}] AutobotEx Dashboard 업데이트 시작 {mode_tag}")

    daily = load_daily()
    existing_record = next(
        (r for r in daily.get("daily_records", []) if r.get("date") == today),
        {},
    )

    # 계좌별 try/except — 1~2개 실패는 부분 갱신, 전 계좌 실패만 raise.
    results = {}
    fetch_status = {}
    fetch_times = {}
    last_errors = {}
    for key, meta in ACCOUNTS.items():
        fetch_start = datetime.datetime.now()
        fetch_times[key] = fetch_start.strftime("%H:%M:%S")
        try:
            print(f"  [{key}] 잔고 조회 중...")
            bal = run_subprocess(meta["dir"], BALANCE_SCRIPT, f"{key}.balance")
            print(f"  [{key}] 매매내역 조회 중...")
            orders = run_subprocess(meta["dir"], ORDERS_SCRIPT, f"{key}.orders")
            results[key] = {
                "balance": bal,
                "orders":  orders,
                "split":   split_balance(key, bal),
            }
            fetch_status[key] = "OK"
            print(
                f"  [{key}] 총잔고 {fmt_won(bal['total'])} "
                f"(현금 {fmt_won(bal['cash'])}) / 매매 {len(orders)}건 / KIS {fetch_times[key]}"
            )
        except Exception as e:
            fetch_status[key] = "FAILED"
            last_errors[key] = (str(e) or repr(e))[:300]
            results[key] = None
            print(f"  [{key}] ❌ KIS 조회 실패: {last_errors[key]}")

    failed_accounts = [k for k, s in fetch_status.items() if s == "FAILED"]
    if len(failed_accounts) == len(ACCOUNTS):
        raise RuntimeError(
            f"전 계좌 KIS 조회 실패. errors={last_errors}"
        )

    # 부분 실패 시 실패 계좌는 기존 same-date 레코드 값을 보존 (캐시 사용을 명시적으로 마킹).
    def split_for(account_key, sub_keys):
        r = results.get(account_key)
        if r is not None:
            return {sub: r["split"][sub] for sub in sub_keys}
        # 기존 same-date 레코드의 같은 필드를 재사용 (전일 비교용 보존).
        # 없으면 0 — kis_fetch_status="FAILED"로 차트에서 식별 가능.
        if account_key == "ISA":
            return {
                "hybrid": existing_record.get("ISA_hybrid", 0),
                "smartsplit": existing_record.get("ISA_smartsplit", 0),
            }
        if account_key == "Pension":
            return {
                "hybrid": existing_record.get("Pension_hybrid", 0),
                "smartsplit": existing_record.get("Pension_smartsplit", 0),
            }
        if account_key == "IRP":
            return {
                "hybrid": existing_record.get("IRP_hybrid_actual", 0),
                "safe": existing_record.get("IRP_safe_actual", 0),
            }
        return {sub: 0 for sub in sub_keys}

    isa_split = split_for("ISA", ["hybrid", "smartsplit"])
    pen_split = split_for("Pension", ["hybrid", "smartsplit"])
    irp_split_eff = split_for("IRP", ["hybrid", "safe"])

    if results.get("IRP"):
        irp_signal = compute_irp_signal(daily, results["IRP"]["split"])
    else:
        irp_signal = {
            "hybrid_signal": existing_record.get("IRP_hybrid_signal", irp_split_eff["hybrid"]),
            "safe_signal":   existing_record.get("IRP_safe_signal",   irp_split_eff["safe"]),
        }

    # 매매내역: 성공한 계좌만 새로 수집, 실패한 계좌는 same-date 레코드의 trades를 보존.
    all_trades = []
    for key in ["ISA", "Pension", "IRP"]:
        if results.get(key):
            all_trades.extend(normalize_trades(key, results[key]["orders"]))
        else:
            all_trades.extend(
                t for t in existing_record.get("trades", []) if t.get("account") == key
            )

    latest_fetch_time = max(fetch_times.values())
    latest_fetch_iso = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    new_record = {
        "date": today,
        "mode": daily.get("mode", "공격"),
        "ISA_hybrid":          int(round(isa_split["hybrid"])),
        "ISA_smartsplit":      int(round(isa_split["smartsplit"])),
        "Pension_hybrid":      int(round(pen_split["hybrid"])),
        "Pension_smartsplit":  int(round(pen_split["smartsplit"])),
        "IRP_hybrid_actual":   int(round(irp_split_eff["hybrid"])),
        "IRP_hybrid_signal":   int(round(irp_signal["hybrid_signal"])),
        "IRP_safe_actual":     int(round(irp_split_eff["safe"])),
        "IRP_safe_signal":     int(round(irp_signal["safe_signal"])),
        "trades": all_trades,
        "kis_fetch_at": latest_fetch_iso,
        "kis_fetch_status": dict(fetch_status),
        "market_closed": market_closed,
    }

    upsert_record(daily, new_record)
    daily["last_kis_fetch"] = latest_fetch_iso
    save_daily(daily)
    print(
        f"  daily.json 저장 완료 (총 {len(daily['daily_records'])} 행, "
        f"trades {len(all_trades)}건, fetch_status={fetch_status})"
    )

    pushed = git_push(today)

    isa_total = new_record["ISA_hybrid"]     + new_record["ISA_smartsplit"]
    pen_total = new_record["Pension_hybrid"] + new_record["Pension_smartsplit"]
    irp_total = new_record["IRP_hybrid_actual"] + new_record["IRP_safe_actual"]
    total_actual = isa_total + pen_total + irp_total
    initial_total = (
        daily["accounts"]["ISA"]["initial"] +
        daily["accounts"]["Pension"]["initial"] +
        daily["accounts"]["IRP"]["initial"]
    )
    cum_amount = total_actual - initial_total

    # 단계별 일일 수익률 경고 (전날 대비 ±3% 주의 / ±5% 경고)
    # date < today 인 레코드 중 가장 최근 것을 prev로 사용 (미래 더미 레코드에 영향 없음).
    # 부분실패가 끼면 노이즈가 생길 수 있으므로 전 계좌 OK일 때만 발송.
    if not failed_accounts:
        prev_recs = [
            r for r in daily.get("daily_records", [])
            if r.get("date", "") < today
        ]
        if prev_recs:
            prev_rec = prev_recs[-1]
            prev_total = (
                prev_rec.get("ISA_hybrid", 0) + prev_rec.get("ISA_smartsplit", 0) +
                prev_rec.get("Pension_hybrid", 0) + prev_rec.get("Pension_smartsplit", 0) +
                prev_rec.get("IRP_hybrid_actual", 0) + prev_rec.get("IRP_safe_actual", 0)
            )
            if prev_total > 0:
                daily_amt = total_actual - prev_total
                daily_pct = (daily_amt / prev_total) * 100
                alert = build_daily_alert(daily_pct, daily_amt, total_actual)
                if alert:
                    discord_notify(alert)
                    print(f"  단계별 알림 전송 (prev={prev_rec.get('date')}): {fmt_pct(daily_pct)}")

    header_suffix = (
        f"(휴장일 NAV 반영, KIS fetch: {latest_fetch_time})"
        if market_closed
        else f"(KIS fetch: {latest_fetch_time})"
    )
    failure_note = ""
    if failed_accounts:
        err_lines = "\n".join(
            f"  · {a}: {last_errors.get(a, '?')[:120]}" for a in failed_accounts
        )
        failure_note = (
            f"\n⚠️ 일부 계좌 KIS 조회 실패: {', '.join(failed_accounts)}\n{err_lines}"
        )

    msg = (
        f"📊 대시보드 업데이트 완료 {header_suffix}\n"
        f"날짜: {today}\n"
        f"총자산: {fmt_won(total_actual)} ({fmt_signed_won(cum_amount)})\n"
        f"ISA: {fmt_won(isa_total)} / 연금: {fmt_won(pen_total)} / IRP: {fmt_won(irp_total)}\n"
        f"오늘 매매: {len(all_trades)}건"
        + failure_note
        + ("" if pushed else "\n(git: 변경사항 없어 push 생략)")
    )
    discord_notify(msg)
    print("✅ 완료")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        tb = traceback.format_exc()
        print(tb)
        try:
            err_brief = (str(e) or repr(e))[:800]
            discord_notify(
                "❌ 대시보드 업데이트 실패\n"
                f"날짜: {datetime.date.today().isoformat()}\n"
                f"```\n{err_brief}\n```"
            )
        except Exception:
            pass
        sys.exit(1)
