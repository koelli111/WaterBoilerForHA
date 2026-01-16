# /config/pyscript/boiler_price_scheduler.py
#
# Boiler scheduler with dynamic "degrees gained per ON-slot" learning.
#
# Features:
# - Learns how many °C the boiler warms per 15-min ON slot ("deg_per_slot")
# - Uses that learned value to calculate how many slots are needed to reach the target temp
# - Fallback planning when temp sensor is unavailable
#
# Added per your requests:
# - Emergency / instant heat override latch:
#     * ON when temp < EMERGENCY_BELOW_C
#     * OFF when temp >= MANDATORY_TARGET_TEMP_C
# - 1h energy limiter:
#     * If last-hour energy usage (Wh) >= HOUR_WH_LIMIT, boiler is forced OFF
#     * Emergency override still wins (safety)
# - Temp sensor 0.0 handling:
#     * If TEMP_SENSOR reads exactly 0.0, treat as invalid
#     * Use input_number.boiler_slots_15m for planning slots
#     * AND 0.0 ALWAYS disables (clears) emergency override latch
#
from datetime import datetime, timedelta

# -------------------- Entities --------------------
PRICE_SENSOR = "sensor.day_ahead_price"

# Set your boiler water temperature sensor here:
TEMP_SENSOR = "sensor.lvv_temperature_offset_enabled"

# Manual fallback slot input (15-min slots) used when TEMP_SENSOR is 0.0
FALLBACK_SLOTS_INPUT = "input_number.boiler_slots_15m"

# 3-phase boiler switches:
SWITCHES = [
    "switch.shellypro3_ec626089e990_switch_0",
    "switch.shellypro3_ec626089e990_switch_1",
    "switch.shellypro3_ec626089e990_switch_2",
]

PLAN_ENTITY = "pyscript.boiler_plan"
ACTUAL_ENTITY = "pyscript.boiler_actual"
LEARN_ENTITY = "pyscript.boiler_learn"

# -------------------- Tunables --------------------
SLOT_MINUTES = 15
SLOT_HOURS = 0.25

# Target temps
TARGET_TEMP_C = 80.0              # normal target
MANDATORY_TARGET_TEMP_C = 60.0    # target used for mandatory condition and emergency release

# Mandatory condition (example):
MANDATORY_BELOW_C = 60.0
MANDATORY_MAX_DAYS_WITHOUT_HEAT = 2

# Emergency / instant heat override
EMERGENCY_BELOW_C = 50.0

# 1h energy limiter (Wh last hour)
HOUR_WH_SENSOR = "sensor.energiapankki_verkosta_tunti"
HOUR_WH_LIMIT = 8000.0

# Slot selection defaults
DEFAULT_SLOTS = 10                 # fallback if learning unavailable / temp missing
MIN_SLOTS = 0
MAX_SLOTS = 16

# Learning parameters
DEG_PER_SLOT_DEFAULT = 1.2        # fallback estimate if no learned value yet (°C per 15 min)
DEG_PER_SLOT_MIN = 0.2            # sanity lower bound
DEG_PER_SLOT_MAX = 6.0            # sanity upper bound
EMA_ALPHA = 0.25                  # higher = adapt faster, lower = smoother

# Require at least this many consecutive ON slots for a "learning sample"
LEARN_MIN_CONSEC_ON_SLOTS = 2

# -------------------- Persistent stores --------------------
state.persist(
    PLAN_ENTITY,
    default_value="0",
    default_attributes={"records": [], "on_series": [], "updated": None},
)
state.persist(
    ACTUAL_ENTITY,
    default_value="0",
    default_attributes={"events": [], "on_series": [], "updated": None},
)
state.persist(
    LEARN_ENTITY,
    default_value="0",
    default_attributes={
        "deg_per_slot": DEG_PER_SLOT_DEFAULT,
        "samples": [],
        "updated": None,
        # runtime tracking
        "run_active": False,
        "run_start": None,
        "run_start_temp": None,
        "run_on_slots": 0,
        # emergency override latch
        "emergency_active": False,
        "emergency_updated": None,
    },
)

# -------------------- Helpers --------------------
def _now():
    return datetime.now()

def _iso(dt):
    return dt.isoformat()

def _parse_dt(s):
    return datetime.fromisoformat(s)

def _read_float(entity_id):
    v = state.get(entity_id)
    if v in (None, "", "unknown", "unavailable"):
        return None
    try:
        return float(v)
    except Exception:
        return None

def _read_int(entity_id):
    v = state.get(entity_id)
    if v in (None, "", "unknown", "unavailable"):
        return None
    try:
        return int(round(float(v)))
    except Exception:
        return None

def _get_records():
    attrs = state.getattr(PRICE_SENSOR) or {}
    return attrs.get("records") or []

def _current_slot(now, records):
    for r in records:
        try:
            s = _parse_dt(r["Time"])
            e = _parse_dt(r["End"])
            if s <= now < e:
                return r
        except Exception:
            pass
    return None

def _set_boiler(on):
    domain = "switch"
    srv = "turn_on" if on else "turn_off"
    i = 0
    while i < len(SWITCHES):
        service.call(domain, srv, entity_id=SWITCHES[i])
        i += 1

def _learn_attrs():
    return state.getattr(LEARN_ENTITY) or {}

def _set_learn_attrs(attrs):
    state.set(LEARN_ENTITY, value="1", new_attributes=attrs)

def _get_deg_per_slot():
    a = _learn_attrs()
    v = a.get("deg_per_slot")
    try:
        v = float(v)
    except Exception:
        v = DEG_PER_SLOT_DEFAULT
    if v < DEG_PER_SLOT_MIN:
        v = DEG_PER_SLOT_MIN
    if v > DEG_PER_SLOT_MAX:
        v = DEG_PER_SLOT_MAX
    return v

def _ema(old, new, alpha):
    return (1.0 - alpha) * old + alpha * new

def _choose_cheapest_future(records, now, slots_needed, horizon_hours=24):
    end_limit = now + timedelta(hours=horizon_hours)
    future = []
    for r in records:
        try:
            s = _parse_dt(r["Time"])
            e = _parse_dt(r["End"])
            if e > now and s < end_limit:
                p = float(r["Price"])
                future.append((p, r["Time"], r["End"]))
        except Exception:
            pass
    future.sort(key=lambda x: x[0])

    chosen = []
    chosen_map = {}
    k = 0
    while k < slots_needed and k < len(future):
        p, s_iso, e_iso = future[k]
        chosen.append({"Time": s_iso, "End": e_iso, "Price": p})
        chosen_map[s_iso] = True
        k += 1
    return chosen, chosen_map

def _build_on_series(records, chosen_map):
    series = []
    for r in records:
        try:
            t = r["Time"]
            series.append({"Time": t, "On": 1 if chosen_map.get(t) else 0})
        except Exception:
            pass
    return series

# -------------------- 1h energy limiter --------------------
def _hour_energy_blocked(hour_wh):
    """
    Return True if boiler should be blocked due to last-hour energy usage.
    Stateless by design: when the sensor drops below the limit, blocking ends.
    """
    if hour_wh is None:
        return False
    try:
        return float(hour_wh) >= float(HOUR_WH_LIMIT)
    except Exception:
        return False

# -------------------- Learning core --------------------
def _update_learning(now, temp_now, actually_on, in_planned_slot):
    """
    Track an "ON run" and learn deg/slot from start->end temperature rise.
    We only learn when:
      - we were ON for >= LEARN_MIN_CONSEC_ON_SLOTS consecutive planned slots
      - temp sensor available at start and end
      - we turned OFF (run ended)
    """
    a = _learn_attrs()

    run_active = bool(a.get("run_active"))
    run_start = a.get("run_start")
    run_start_temp = a.get("run_start_temp")
    run_on_slots = a.get("run_on_slots")

    # If temp missing OR exactly 0.0, don't start/stop learning; just keep state.
    try:
        if temp_now is None or float(temp_now) == 0.0:
            _set_learn_attrs(a)
            return
    except Exception:
        _set_learn_attrs(a)
        return

    learning_on = bool(actually_on and in_planned_slot)

    if learning_on:
        if not run_active:
            a["run_active"] = True
            a["run_start"] = now.isoformat()
            a["run_start_temp"] = float(temp_now)
            a["run_on_slots"] = 1
        else:
            try:
                a["run_on_slots"] = int(run_on_slots) + 1
            except Exception:
                a["run_on_slots"] = 1
    else:
        if run_active:
            try:
                nslots = int(run_on_slots)
            except Exception:
                nslots = 0

            try:
                t0 = float(run_start_temp) if run_start_temp is not None else None
            except Exception:
                t0 = None

            t1 = float(temp_now) if temp_now is not None else None

            if nslots >= LEARN_MIN_CONSEC_ON_SLOTS and t0 is not None and t1 is not None:
                delta = t1 - t0
                if delta > 0.0:
                    sample = delta / float(nslots)
                    if sample < DEG_PER_SLOT_MIN:
                        sample = DEG_PER_SLOT_MIN
                    if sample > DEG_PER_SLOT_MAX:
                        sample = DEG_PER_SLOT_MAX

                    old = _get_deg_per_slot()
                    newv = _ema(old, sample, EMA_ALPHA)

                    samples = a.get("samples") or []
                    samples.append({
                        "ts": now.isoformat(),
                        "start": run_start,
                        "slots": nslots,
                        "t0": t0,
                        "t1": t1,
                        "deg_per_slot_sample": sample,
                        "deg_per_slot_new": newv
                    })
                    if len(samples) > 30:
                        samples = samples[-30:]

                    a["deg_per_slot"] = float(newv)
                    a["samples"] = samples
                    a["updated"] = now.isoformat()

            a["run_active"] = False
            a["run_start"] = None
            a["run_start_temp"] = None
            a["run_on_slots"] = 0

    _set_learn_attrs(a)

# -------------------- Slot calculation --------------------
def _calc_slots_needed(temp_now, target_temp):
    # If temp missing -> default fallback
    if temp_now is None:
        return int(DEFAULT_SLOTS)

    # If sensor reads 0.0 -> use manual input_number fallback (15-min slots)
    try:
        if float(temp_now) == 0.0:
            fb = _read_int(FALLBACK_SLOTS_INPUT)
            if fb is not None:
                if fb < MIN_SLOTS:
                    fb = MIN_SLOTS
                if fb > 96:
                    fb = 96
                return int(fb)
            return int(DEFAULT_SLOTS)
    except Exception:
        return int(DEFAULT_SLOTS)

    deg_per_slot = _get_deg_per_slot()
    need = target_temp - float(temp_now)
    if need <= 0:
        return 0

    slots = int((need / deg_per_slot) + 0.999999)  # ceil without math import
    if slots < MIN_SLOTS:
        slots = MIN_SLOTS
    if slots > MAX_SLOTS:
        slots = MAX_SLOTS
    return slots

# -------------------- Mandatory logic --------------------
def _mandatory_needed(now, temp_now):
    # temp condition
    temp_cold = False
    if temp_now is not None:
        try:
            temp_cold = float(temp_now) < float(MANDATORY_BELOW_C)
        except Exception:
            temp_cold = False

    # time since last heat (from ACTUAL_ENTITY events)
    attrs = state.getattr(ACTUAL_ENTITY) or {}
    events = attrs.get("events") or []

    last_end = None
    i = 0
    while i < len(events):
        try:
            e = events[i].get("end")
            if e is not None:
                dt = datetime.fromisoformat(e)
                if last_end is None or dt > last_end:
                    last_end = dt
        except Exception:
            pass
        i += 1

    too_long = False
    if last_end is None:
        too_long = True
    else:
        if now - last_end >= timedelta(days=int(MANDATORY_MAX_DAYS_WITHOUT_HEAT)):
            too_long = True

    return bool(temp_cold and too_long)

# -------------------- Emergency / instant override --------------------
def _emergency_override(now, temp_now):
    """Emergency latch with special rule:
    - temp == 0.0 ALWAYS disables (clears) emergency.
    """
    a = _learn_attrs()
    active = bool(a.get("emergency_active"))

    if temp_now is None:
        return active

    try:
        t = float(temp_now)
    except Exception:
        return active

    # IMPORTANT: 0.0 ALWAYS disables emergency (clear latch)
    if t == 0.0:
        if active:
            a["emergency_active"] = False
            a["emergency_updated"] = now.isoformat()
            _set_learn_attrs(a)
        return False

    changed = False
    if (not active) and (t < float(EMERGENCY_BELOW_C)):
        active = True
        a["emergency_active"] = True
        changed = True
    elif active and (t >= float(MANDATORY_TARGET_TEMP_C)):
        active = False
        a["emergency_active"] = False
        changed = True

    if changed:
        a["emergency_updated"] = now.isoformat()
        _set_learn_attrs(a)

    return active

# -------------------- Actual tracking (+-3 days series) --------------------
def _update_actual(now, actually_on):
    attrs = state.getattr(ACTUAL_ENTITY) or {}
    events = attrs.get("events") or []

    keep_from = now - timedelta(days=7)
    kept = []
    for ev in events:
        try:
            end = ev.get("end")
            if end is None:
                kept.append(ev)
            else:
                e = datetime.fromisoformat(end)
                if e >= keep_from:
                    kept.append(ev)
        except Exception:
            pass
    events = kept

    open_idx = -1
    i = 0
    while i < len(events):
        if events[i].get("end") is None:
            open_idx = i
        i += 1

    if actually_on and open_idx == -1:
        events.append({"start": now.isoformat(), "end": None})
    elif (not actually_on) and open_idx != -1:
        events[open_idx]["end"] = now.isoformat()

    start = now - timedelta(days=3)
    minute = (start.minute // 15) * 15
    start = start.replace(minute=minute, second=0, microsecond=0)

    ranges = []
    for ev in events:
        try:
            s = datetime.fromisoformat(ev["start"])
            e_raw = ev.get("end")
            e = datetime.fromisoformat(e_raw) if e_raw else now
            ranges.append((s, e))
        except Exception:
            pass

    series = []
    t = start
    endt = now + timedelta(days=3)
    while t <= endt:
        on = 0
        j = 0
        while j < len(ranges):
            s, e = ranges[j]
            if s <= t < e:
                on = 1
                break
            j += 1
        series.append({"Time": t.isoformat(), "On": on})
        t = t + timedelta(minutes=15)

    state.set(ACTUAL_ENTITY, value=str(len(events)), new_attributes={
        "events": events,
        "on_series": series,
        "updated": now.isoformat()
    })

# -------------------- Main run --------------------
def _run():
    now = _now()

    records = _get_records()
    cur = _current_slot(now, records)

    temp_now = _read_float(TEMP_SENSOR)

    # 1h energy limiter (read + compute)
    hour_wh = _read_float(HOUR_WH_SENSOR)
    hour_blocked = _hour_energy_blocked(hour_wh)

    # Emergency override (temp==0.0 clears it)
    emergency = _emergency_override(now, temp_now)

    mandatory = _mandatory_needed(now, temp_now)

    # If emergency is active, we do NOT price-plan: we force ON until MANDATORY_TARGET_TEMP_C.
    if emergency:
        target = float(MANDATORY_TARGET_TEMP_C)
        slots_needed = 0
        chosen, chosen_map = [], {}
        on_series = _build_on_series(records, chosen_map)
    else:
        target = MANDATORY_TARGET_TEMP_C if mandatory else TARGET_TEMP_C
        slots_needed = _calc_slots_needed(temp_now, target)
        chosen, chosen_map = _choose_cheapest_future(records, now, slots_needed, horizon_hours=24)
        on_series = _build_on_series(records, chosen_map)

    in_planned_slot = False
    if cur is not None:
        tcur = cur.get("Time")
        if tcur is not None and chosen_map.get(tcur):
            in_planned_slot = True

    # Decide ON/OFF
    # Priority:
    # 1) Emergency override ALWAYS wins (forces ON)
    # 2) Otherwise, if 1h energy limiter is active -> force OFF
    # 3) Otherwise, normal logic (mandatory OR planned slot)
    if emergency:
        should_on = True
    elif hour_blocked:
        should_on = False
    else:
        should_on = bool(mandatory or in_planned_slot)

    # Apply
    _set_boiler(should_on)

    # Determine actual state from switches (best-effort):
    actually_on = False
    i = 0
    while i < len(SWITCHES):
        if state.get(SWITCHES[i]) == "on":
            actually_on = True
            break
        i += 1

    # Update learning (avoid learning if temp is missing/0)
    _update_learning(now, temp_now, actually_on, in_planned_slot)

    # For debug: what manual fallback would be (if temp==0)
    manual_fallback_slots = None
    try:
        if temp_now is not None and float(temp_now) == 0.0:
            manual_fallback_slots = _read_int(FALLBACK_SLOTS_INPUT)
    except Exception:
        manual_fallback_slots = None

    # Persist plan
    state.set(PLAN_ENTITY, value=str(len(chosen)), new_attributes={
        "records": chosen,
        "on_series": on_series,
        "updated": now.isoformat(),
        "used_slots": int(slots_needed),
        "mandatory": bool(mandatory),
        "emergency": bool(emergency),
        "temp": temp_now,
        "target_temp": float(target),
        "deg_per_slot": _get_deg_per_slot(),
        "deg_per_slot_default": DEG_PER_SLOT_DEFAULT,

        # 1h limiter debug
        "hour_wh": hour_wh,
        "hour_wh_limit": float(HOUR_WH_LIMIT),
        "hour_blocked": bool(hour_blocked),

        # temp==0 fallback debug
        "temp_zero_fallback_input": FALLBACK_SLOTS_INPUT,
        "temp_zero_fallback_slots": manual_fallback_slots,
    })

    # Update actual tracking
    _update_actual(now, actually_on)

    log.info(
        "Boiler | temp=%s target=%s deg/slot=%s slots=%s mandatory=%s emergency=%s hour_wh=%s blocked=%s on=%s"
        % (
            str(temp_now),
            str(target),
            str(_get_deg_per_slot()),
            str(slots_needed),
            str(mandatory),
            str(emergency),
            str(hour_wh),
            str(hour_blocked),
            str(should_on),
        )
    )

# -------------------- Triggers --------------------
@time_trigger("startup")
def boiler_scheduler_startup():
    _run()

@time_trigger("cron(*/5 * * * *)")
def boiler_scheduler_cron():
    _run()

# Run immediately on temperature changes so the emergency override reacts instantly
@state_trigger(f"{TEMP_SENSOR}")
def boiler_scheduler_temp_change():
    _run()

@service
def boiler_scheduler_run_now():
    _run()
