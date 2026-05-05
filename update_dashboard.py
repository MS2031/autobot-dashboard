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

# 듀얼 잔고 추정 엔진 (C:\AutobotEx\realtime_estimate.py)
sys.path.insert(0, r"C:\AutobotEx")
try:
    import realtime_estimate as RT
    RT_AVAILABLE = True
except Exception as _rt_err:
    RT = None
    RT_AVAILABLE = False
    _rt_import_err = str(_rt_err)[:200]

DASHBOARD_DIR = r"C:\AutobotEx\dashboard"
DAILY_JSON    = os.path.join(DASHBOARD_DIR, "data", "daily.json")
PYTHON_EXE    = sys.executable

# 누적 손익 base + 자동매매 시작일 (config/initial_capital.py)
from config.initial_capital import (  # noqa: E402
    INITIAL_CAPITAL, TRADING_START_DATE, days_in_operation, annualized_return, calc_cagr
)

# 한투 앱 "계좌 총자산" 일자별 참고값 (사용자 캡처).
# Phase 2 KIS raw 분석 결과 한투 세전평가 = KIS scts_evlu_amt + α (자동 매핑 불가).
# 메인 표시는 KIS 기준, 한투는 "참고" 강등.
HANTOO_OVERRIDE = {
    "2026-05-01": {
        "ISA":     81_800_733,
        "Pension": 48_590_460,
        "IRP":     36_777_923,
    },
}

# D+2 미결제 / 시스템별 손익 모듈
try:
    from utils import d_plus_2 as D2, strategy_classifier as SC
    UTILS_AVAILABLE = True
except Exception as _u_err:
    D2 = None
    SC = None
    UTILS_AVAILABLE = False
    _utils_err = str(_u_err)[:200]

DISCORD_WEBHOOK = (
    "https://discord.com/api/webhooks/1493967027000443010/"
    "-sqgdVy8BQ-G0LxwX51mHCwV1nuqgJieIznyV8_5Zaq18nqKXQ9VEE-N77oCdQnbHT0D"
)

# 매매내역 분류는 utils/strategy_classifier (단일 SoT) 사용. (Phase 3, 2026-05-04 통합)
# 잔고는 weights 기반 분배.
ACCOUNTS = {
    "ISA": {
        "dir": r"C:\AutobotEx\ISA",
        "weights": {"hybrid": 0.55, "smartsplit": 0.45},
    },
    "Pension": {
        "dir": r"C:\AutobotEx\Pension",
        "weights": {"hybrid": 0.75, "smartsplit": 0.25},
    },
    "IRP": {
        "dir": r"C:\AutobotEx\IRP",
        "weights": {"hybrid": 0.70, "safe": 0.30},
    },
}

STRATEGY_LABEL = {"hybrid": "Hybrid", "smartsplit": "SmartSplit", "safe": "안전자산", "safety": "안전자산"}


def _classify_trade(account, code):
    """매매 1건의 전략 라벨. SC(strategy_classifier) 우선, fallback은 hybrid."""
    if SC is not None:
        # classify_holding returns "Hybrid" | "SmartSplit" | "Safety" (Title case)
        label = SC.classify_holding(account, code)
        return label  # already display-ready
    return "Hybrid"

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
         "value": f(s.get("StockNowMoney")),
         "pchs": f(s.get("StockOriMoney")),
         "qty": i(s.get("StockAmt")),
         "avg_price": f(s.get("StockAvgPrice"))}
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
        out.append({
            "account": account_key,
            "strategy": _classify_trade(account_key, code),
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

    # P12: 휴장일 Discord 발송 정책 (옵션 B)
    # - 평일은 모든 호출 발송
    # - 휴장일은 EOD(15:35 이후)만 발송, 인트라데이(10~14)는 daily.json 갱신만
    _now = datetime.datetime.now()
    _is_eod = (_now.hour == 15 and _now.minute >= 35) or _now.hour > 15
    should_notify = (not market_closed) or _is_eod

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

    # git push는 모든 추가 필드 (bot_modes/cagr/canary/strategy_pnl/realtime_estimate) 처리
    # 후 마지막 save_daily 이후로 이동. 여기서는 일단 placeholder만 두고 마지막에 호출.
    pushed = False

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
                    if should_notify:
                        discord_notify(alert)
                        print(f"  단계별 알림 전송 (prev={prev_rec.get('date')}): {fmt_pct(daily_pct)}")
                    else:
                        print(f"  단계별 알림 스킵 (휴장일 인트라데이): {fmt_pct(daily_pct)}")

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

    # ─── 한투 앱 override (참고용) ───
    hantoo_today = HANTOO_OVERRIDE.get(today)
    hantoo_total = sum(hantoo_today.values()) if hantoo_today else None

    # ─── KIS 기준 누적 손익 (메인) ───
    cap = INITIAL_CAPITAL
    isa_pnl = isa_total - cap["ISA"]
    pen_pnl = pen_total - cap["Pension"]
    irp_pnl = irp_total - cap["IRP"]
    total_pnl = total_actual - cap["Total"]

    def _pct(amt, base):
        return (amt / base * 100.0) if base else 0.0

    today_dt = datetime.date.today()
    p_info = annualized_return("Portfolio", total_actual, today_dt, always_show=True)
    isa_info = annualized_return("ISA", isa_total, today_dt, always_show=True)
    pen_info = annualized_return("Pension", pen_total, today_dt, always_show=True)
    irp_info = annualized_return("IRP", irp_total, today_dt, always_show=True)

    def _fmt_cagr(info):
        # 신뢰도 라벨 제거 — 단순 % 표시 (P6)
        sign = "+" if info["cagr_pct"] >= 0 else ""
        return f"{sign}{info['cagr_pct']:.2f}%"

    main_block = [
        "[누적 손익 (KIS 기준 — 메인)]",
        f"  ISA: {fmt_won(isa_total)} / 누적 {fmt_signed_won(isa_pnl)} ({_pct(isa_pnl, cap['ISA']):+.2f}%) / CAGR {_fmt_cagr(isa_info)}",
        f"  연금: {fmt_won(pen_total)} / 누적 {fmt_signed_won(pen_pnl)} ({_pct(pen_pnl, cap['Pension']):+.2f}%) / CAGR {_fmt_cagr(pen_info)}",
        f"  IRP: {fmt_won(irp_total)} / 누적 {fmt_signed_won(irp_pnl)} ({_pct(irp_pnl, cap['IRP']):+.2f}%) / CAGR {_fmt_cagr(irp_info)}",
        f"  합계: {fmt_won(total_actual)} / 누적 {fmt_signed_won(total_pnl)} ({_pct(total_pnl, cap['Total']):+.2f}%) / CAGR {_fmt_cagr(p_info)}",
    ]

    # ─── D+2 미결제 결제 예정 ───
    d2_block = []
    if UTILS_AVAILABLE and not failed_accounts:
        try:
            today_yyyymmdd = today.replace("-", "")
            unsettled = D2.fetch_unsettled_per_account(
                today_str=today_yyyymmdd,
                smartsplit_codes_per_account=SC.SMARTSPLIT_PER_ACCOUNT,
            )
            d2_lines = D2.format_for_message(unsettled, header="[D+2 결제 예정]")
            if d2_lines:
                d2_block = d2_lines
        except Exception as _d2_err:
            print(f"  [D+2] 조회 실패: {_d2_err}")

    # ─── 한투 앱 잔고 (참고) ───
    hantoo_block = []
    if hantoo_today:
        kis_diff = total_actual - hantoo_total
        hantoo_block = [
            f"[참고: 한투 앱 잔고] {fmt_won(hantoo_total)}",
            (
                f"  ISA {fmt_won(hantoo_today['ISA'])} / "
                f"연금 {fmt_won(hantoo_today['Pension'])} / "
                f"IRP {fmt_won(hantoo_today['IRP'])}"
            ),
            f"  KIS와 차이 {fmt_signed_won(-kis_diff)} (메트릭 차이 — D+2 미결제 + α 보정)",
        ]

    # ─── P1: accounts.*.initial 자동 동기화 (매 실행마다 INITIAL_CAPITAL과 정합) ───
    daily.setdefault("accounts", {}).setdefault("ISA", {})["initial"] = INITIAL_CAPITAL["ISA"]
    daily["accounts"].setdefault("Pension", {})["initial"] = INITIAL_CAPITAL["Pension"]
    daily["accounts"].setdefault("IRP", {})["initial"] = INITIAL_CAPITAL["IRP"]

    # ─── P4: mode 자동 동기화 (3계좌 hybrid state 종합) ───
    state_paths = {
        "ISA": r"C:\AutobotEx\ISA\hybrid_isa_state.json",
        "Pension": r"C:\AutobotEx\Pension\hybrid_pension_state.json",
        "IRP": r"C:\AutobotEx\IRP\hybrid_irp_state.json",
    }
    bot_modes = {}
    for acc, path in state_paths.items():
        try:
            with open(path, "r", encoding="utf-8") as f:
                bot_modes[acc] = json.load(f).get("last_signal", "")
        except Exception:
            bot_modes[acc] = "?"
    daily_mode = "방어" if "DEFENSE" in bot_modes.values() else "공격"
    daily["mode"] = daily_mode
    new_record["mode"] = daily_mode
    new_record["bot_modes"] = bot_modes
    print(f"  [P4 mode 동기화] bot_modes={bot_modes} → daily_mode={daily_mode}")

    # ─── P2: CAGR 저장 ───
    new_record["cagr"] = {
        "Portfolio": round(p_info["cagr_pct"], 2),
        "ISA": round(isa_info["cagr_pct"], 2),
        "Pension": round(pen_info["cagr_pct"], 2),
        "IRP": round(irp_info["cagr_pct"], 2),
    }

    # ─── P11: 종목별 실분류 기반 strategy_pnl + strategy_matrix ───
    # 2026-05-04 23:30: split_account_by_strategy_v2 — SmartSplit JSON 차수 데이터로 SS/Hy 분리
    def _normalize_holdings(account_key):
        r = results.get(account_key)
        if not r:
            return []
        return [
            {"ticker": s.get("code"),
             "amt": int(s.get("qty", 0) or 0),
             "avg_price": float(s.get("avg_price", 0) or 0),
             "pchs_amt": s.get("pchs", 0),
             "evlu_amt": s.get("value", 0)}
            for s in r["balance"].get("stocks", []) or []
        ]

    # SmartSplit JSON 캐시 무효화 (재실행 시 최신 데이터)
    SC._SS_CACHE = {}
    isa_split = SC.split_account_by_strategy_v2("ISA", _normalize_holdings("ISA"))
    pen_split = SC.split_account_by_strategy_v2("Pension", _normalize_holdings("Pension"))
    irp_split = SC.split_account_by_strategy_v2("IRP", _normalize_holdings("IRP"))

    # Discrepancy 경고 (SS state amt > KIS amt)
    all_warnings = (isa_split.get("_warnings", []) + pen_split.get("_warnings", [])
                    + irp_split.get("_warnings", []))
    for w in all_warnings:
        print(f"  ⚠️ [Discrepancy] {w['account']} {w['code']}: {w['msg']}")

    print(f"  [P11 분류] ISA Hybrid {isa_split['Hybrid']['tickers']}")
    print(f"  [P11 분류] ISA SmartSplit {isa_split['SmartSplit']['tickers']}")
    print(f"  [P11 분류] 연금 Hybrid {pen_split['Hybrid']['tickers']}")
    print(f"  [P11 분류] 연금 SmartSplit {pen_split['SmartSplit']['tickers']}")
    print(f"  [P11 분류] IRP Hybrid {irp_split['Hybrid']['tickers']}")
    print(f"  [P11 분류] IRP Safety {irp_split['Safety']['tickers']}")

    def _block_from_split(split_data, account_for_days):
        pchs = split_data["pchs"]
        evlu = split_data["evlu"]
        pnl = split_data["pnl"]
        pct = (pnl / pchs * 100.0) if pchs else 0.0
        days = days_in_operation(account_for_days, today_dt)
        cagr_pct = calc_cagr(pchs, evlu, days)
        return {
            "capital": int(round(pchs)),
            "current": int(round(evlu)),
            "pnl": int(round(pnl)),
            "pct": round(pct, 2),
            "cagr": round(cagr_pct, 2),
            "tickers": split_data["tickers"],
        }

    # 전략별 통합 — 3계좌 동일 전략 합산
    def _combine(splits, key, account_for_days):
        combined = {"pchs": 0.0, "evlu": 0.0, "pnl": 0.0, "tickers": []}
        for sp in splits:
            combined["pchs"] += sp[key]["pchs"]
            combined["evlu"] += sp[key]["evlu"]
            combined["tickers"].extend(sp[key]["tickers"])
        combined["pnl"] = combined["evlu"] - combined["pchs"]
        return _block_from_split(combined, account_for_days)

    new_record["strategy_pnl"] = {
        "Hybrid":     _combine([isa_split, pen_split, irp_split], "Hybrid", "Portfolio"),
        "SmartSplit": _combine([isa_split, pen_split], "SmartSplit", "Portfolio"),
        "Safety":     _combine([irp_split], "Safety", "IRP"),
    }

    # 매트릭스: 각 계좌 × 전략 6행 + 현금 (미투입) 행
    isa_cash = float(results["ISA"]["balance"]["cash"]) if results.get("ISA") else 0.0
    pen_cash = float(results["Pension"]["balance"]["cash"]) if results.get("Pension") else 0.0
    irp_cash = float(results["IRP"]["balance"]["cash"]) if results.get("IRP") else 0.0
    total_cash = isa_cash + pen_cash + irp_cash

    new_record["strategy_matrix"] = [
        {"row": "ISA Hybrid",      **_block_from_split(isa_split["Hybrid"], "ISA")},
        {"row": "ISA SmartSplit",  **_block_from_split(isa_split["SmartSplit"], "ISA")},
        {"row": "연금 Hybrid",     **_block_from_split(pen_split["Hybrid"], "Pension")},
        {"row": "연금 SmartSplit", **_block_from_split(pen_split["SmartSplit"], "Pension")},
        {"row": "IRP Hybrid",      **_block_from_split(irp_split["Hybrid"], "IRP")},
        {"row": "IRP 안전자산",    **_block_from_split(irp_split["Safety"], "IRP")},
        {"row": "현금 (미투입)",   "capital": int(round(total_cash)),
         "current": int(round(total_cash)), "pnl": 0, "pct": 0.0, "cagr": 0.0,
         "tickers": []},
    ]

    # ─── (2026-05-05) holdings_detail — 보유 종목별 일일 상세 ───
    # 분류 규칙 (매트릭스와 일관성 유지):
    #   - IRP Safety (0162Z0) → Safety
    #   - IRP 그 외 → Hybrid
    #   - ISA/Pension: SS STOCKS에 있고 SS state JSON에 실제 매수 차수 존재 → SmartSplit
    #     (SS STOCKS에 있어도 SS 매수 0이면 Hybrid 잔여로 보고 Hybrid 분류)
    holdings_detail = []
    if SC is not None:
        def _classify_with_state(account, code):
            if account == "IRP" and code in SC.IRP_SAFETY:
                return "Safety", None
            if account == "IRP":
                return "Hybrid", None
            if code not in SC.SMARTSPLIT_PER_ACCOUNT.get(account, set()):
                return "Hybrid", None
            ss_state = SC._load_smartsplit_state(account)
            rec = ss_state.get(code)
            if rec:
                levels = [m["Number"] for m in rec.get("MagicDataList", [])
                          if m.get("IsBuy") and m.get("EntryAmt", 0) > 0]
                if levels:
                    return "SmartSplit", max(levels)
            return "Hybrid", None

        for account_key in ["ISA", "Pension", "IRP"]:
            r = results.get(account_key)
            if not r:
                continue
            for s in r["balance"].get("stocks", []) or []:
                code = str(s.get("code", "") or "")
                qty = int(s.get("qty", 0) or 0)
                if qty <= 0:
                    continue
                avg_buy = float(s.get("avg_price", 0) or 0)
                eval_amt = float(s.get("value", 0) or 0)
                pchs = float(s.get("pchs", 0) or 0)
                current = eval_amt / qty if qty > 0 else 0.0
                pnl = eval_amt - pchs
                pnl_pct = (pnl / pchs * 100.0) if pchs > 0 else 0.0
                strategy, split_level = _classify_with_state(account_key, code)
                holdings_detail.append({
                    "account": account_key,
                    "strategy": strategy,
                    "stock_code": code,
                    "stock_name": s.get("name", code) or code,
                    "qty": qty,
                    "avg_buy_price": int(round(avg_buy)),
                    "current_price": int(round(current)),
                    "buy_amount": int(round(pchs)),
                    "current_amount": int(round(eval_amt)),
                    "pnl": int(round(pnl)),
                    "pnl_pct": round(pnl_pct, 2),
                    "split_level": split_level,
                })
    new_record["holdings_detail"] = holdings_detail
    print(f"  [holdings_detail] {len(holdings_detail)}종 기록")

    # ─── P5: 카나리아 13612W 저장 (BAA: SPY/VWO/VEA/BND) ───
    try:
        import yfinance as _yf
        canary_scores = {}
        for tk in ["SPY", "VWO", "VEA", "BND"]:
            try:
                d = _yf.download(tk, period="2y", progress=False, auto_adjust=True)
                if d is None or d.empty:
                    canary_scores[tk] = None
                    continue
                close = d["Close"]
                if hasattr(close, "iloc") and hasattr(close, "shape") and len(close.shape) > 1:
                    close = close.iloc[:, 0]
                close = close.dropna()
                if len(close) < 253:
                    canary_scores[tk] = None
                    continue
                cur = float(close.iloc[-1])
                p1 = float(close.iloc[-22]); p3 = float(close.iloc[-64])
                p6 = float(close.iloc[-127]); p12 = float(close.iloc[-253])
                if min(p1, p3, p6, p12) > 0:
                    canary_scores[tk] = round(
                        12 * (cur/p1 - 1) + 4 * (cur/p3 - 1) + 2 * (cur/p6 - 1) + (cur/p12 - 1),
                        4,
                    )
                else:
                    canary_scores[tk] = None
            except Exception as _ce:
                canary_scores[tk] = None
        new_record["canary"] = canary_scores
        print(f"  [P5 canary] {canary_scores}")
    except Exception as _ce_top:
        print(f"  [P5 canary] 계산 실패: {_ce_top}")

    # ─── P13 (2026-05-05): 안전장치 v2 — VIX / 환율 / 통합 모드 / 경고 ───
    try:
        from utils.safety_signals import (
            get_vix_data as _get_vix, get_krw_data as _get_krw,
            determine_mode_v2 as _determine_mode_v2,
        )
        _scores_for_mode = {k: v for k, v in (new_record.get("canary") or {}).items()
                            if isinstance(v, (int, float))}
        _vix = _get_vix()
        _krw = _get_krw()
        if _vix:
            new_record["vix"] = {
                "today": round(_vix["today"], 2),
                "yesterday": round(_vix["yesterday"], 2),
                "change_1d": round(_vix["change_1d"], 4),
                "level": ("위기" if _vix["today"] >= 35.0
                          else ("우려" if _vix["today"] >= 25.0 else "정상")),
            }
        else:
            new_record["vix"] = None

        if _scores_for_mode:
            _mode, _reason, _fx, _warn = _determine_mode_v2(_scores_for_mode, _vix, _krw)
            new_record["mode_reason"] = f"{_mode}: {_reason}"
            new_record["warnings"] = list(_warn)
            new_record["fx"] = _fx
        else:
            new_record["mode_reason"] = None
            new_record["warnings"] = []
            new_record["fx"] = None
        print(f"  [P13 v2] mode={new_record.get('mode_reason')} vix={new_record.get('vix')} warnings={new_record.get('warnings')}")
    except Exception as _v2err:
        print(f"  [P13 v2] 계산 실패: {_v2err}")
        new_record.setdefault("vix", None)
        new_record.setdefault("fx", None)
        new_record.setdefault("mode_reason", None)
        new_record.setdefault("warnings", [])

    # ─── daily.json 기록용 realtime_estimate (생략 가능, 보존) ───
    if RT_AVAILABLE and not failed_accounts:
        try:
            metadata = RT.load_metadata()
            snapshot = RT.fetch_market_snapshot()
            all_holdings = []
            for key in ["ISA", "Pension", "IRP"]:
                r = results.get(key)
                if not r:
                    continue
                for s in r["balance"].get("stocks", []) or []:
                    all_holdings.append({
                        "code": s.get("code"),
                        "name": s.get("name"),
                        "qty":  s.get("qty", 0),
                        "evlu_amt": s.get("value", 0),
                    })
            base_total = hantoo_total if hantoo_total else total_actual
            estimate = RT.estimate_realtime_balance(base_total, all_holdings, metadata, snapshot)
            new_record["realtime_estimate"] = {
                "official_hantoo": hantoo_total,
                "official_kis": int(round(total_actual)),
                "estimated": estimate["estimated"],
                "gap": estimate["gap"],
                "gap_pct": round(estimate["gap_pct"], 4),
                "breakdown": estimate["breakdown"],
                "snapshot_time": snapshot.get("snapshot_time"),
            }
            new_record["pnl_kis"] = {
                "ISA": int(round(isa_pnl)),
                "Pension": int(round(pen_pnl)),
                "IRP": int(round(irp_pnl)),
                "Total": int(round(total_pnl)),
            }
            upsert_record(daily, new_record)
            save_daily(daily)
        except Exception as _est_err:
            print(f"  [추정] daily.json 기록 실패: {_est_err}")

    # 모든 mutation 완료 후 git push (cagr/canary/strategy_pnl 모두 포함)
    pushed = git_push(today)

    # 메시지 조립
    parts = [
        f"📊 대시보드 업데이트 완료 {header_suffix}",
        f"날짜: {today}",
        f"오늘 매매: {len(all_trades)}건",
        "",
        *main_block,
    ]
    if d2_block:
        parts.append("")
        parts.extend(d2_block)
    if hantoo_block:
        parts.append("")
        parts.extend(hantoo_block)
    parts.append("")
    parts.append("✓ SmartSplit 4/30 매수 (SK하이닉스/삼성전자) 정상 작동 — D+2 결제 후 한투-KIS 수렴")
    if failure_note:
        parts.append(failure_note.lstrip("\n"))
    if not pushed:
        parts.append("(git: 변경사항 없어 push 생략)")

    msg = "\n".join(parts)
    if should_notify:
        discord_notify(msg)
        print("✅ 완료")
    else:
        print("[DashboardUpdate] 휴장일 인트라데이 — Discord 발송 스킵 (daily.json은 갱신됨)")
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
