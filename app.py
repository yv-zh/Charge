from __future__ import annotations
from dataclasses import dataclass
from datetime import date, timedelta
from io import BytesIO
from typing import Dict, List, Optional, Set, Tuple
import base64
import calendar
import hashlib
import json
import os
import pandas as pd
import plotly.express as px
import requests
import streamlit as st
import streamlit.components.v1 as components


# ============================================================
# ✅ Mini-Planyway macro (TEAM / SharePoint via Power Automate)
# - Source: Power Automate endpoint (HTTP trigger) -> returns XLSX bytes OR JSON{contentBytes}
# - Structure Excel (human readable):
#     Sheet "Config"                 (global) + column people="Alice,Bob"
#     Sheet "{Prenom}"               tasks for that person
#     Sheet "{Prenom}_absence"       absences for that person
# - App is READ-ONLY vs SharePoint
# - No simulation UI: macro params come from Config only
# ============================================================

SHEET_CONFIG = "Config"
HOL_SUFFIX = "_absence"

WEEKDAY_LABELS = ["Lun", "Mar", "Mer", "Jeu", "Ven", "Sam", "Dim"]
SLOT_LABELS = ["AM", "PM"]

STATUSES = ["Todo", "Done"]
MODES = ["BLOCK", "SMOOTH", "FOCUS"]

HOLIDAY_SERIES_NAME = "Holiday"
HOLIDAY_COLOR = "#B0B0B0"  # gris

PA_ENDPOINT = (
    (st.secrets.get("PA_ENDPOINT", "") if hasattr(st, "secrets") else "")
    or os.environ.get("PA_ENDPOINT", "")
)


# ============================================================
# Fetch XLSX
# ============================================================

@st.cache_data(ttl=300, show_spinner=False)
def fetch_excel_from_power_automate(url: str) -> bytes:
    """
    Supports 2 responses:
    - raw XLSX bytes (Content-Type xlsx)
    - JSON with base64 field contentBytes
    """
    r = requests.get(url, timeout=60, verify=False)
    r.raise_for_status()

    ctype = (r.headers.get("Content-Type") or "").lower()

    if "application/json" in ctype or r.text.strip().startswith("{"):
        data = r.json()
        b64 = data.get("contentBytes") or data.get("file") or data.get("bytes")
        if not b64:
            raise ValueError("JSON received but missing contentBytes/file/bytes.")
        return base64.b64decode(b64)

    return r.content


def open_xls_bytes(xlsx_bytes: bytes) -> pd.ExcelFile:
    return pd.ExcelFile(BytesIO(xlsx_bytes))


def read_sheet(xls: pd.ExcelFile, name: str) -> pd.DataFrame:
    if name not in xls.sheet_names:
        return pd.DataFrame()
    return pd.read_excel(xls, sheet_name=name)


# ============================================================
# Date + parsing helpers
# ============================================================

def normalize_date(x) -> Optional[date]:
    if x is None or pd.isna(x):
        return None

    if isinstance(x, date) and not isinstance(x, pd.Timestamp):
        return x
    if isinstance(x, pd.Timestamp):
        return x.date()

    s = str(x).strip()
    if not s:
        return None

    d = pd.to_datetime(s, errors="coerce", format="%d/%m/%Y")
    if pd.isna(d):
        d = pd.to_datetime(s, errors="coerce")
    return d.date() if pd.notna(d) else None


def parse_workdays(s: object) -> Tuple[int, ...]:
    """
    UI / Excel: 1=lundi ... 7=dimanche
    Interne:    0=lundi ... 6=dimanche
    """
    if s is None:
        return tuple()

    txt = str(s).strip()
    if not txt:
        return tuple()

    out: List[int] = []
    for token in txt.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            v = int(token)
            if 1 <= v <= 7:
                out.append(v - 1)
        except ValueError:
            continue

    return tuple(out)


def parse_weekdays_list(s: str) -> Optional[Set[int]]:
    """
    UI / Excel: 1=lundi ... 7=dimanche
    Interne:    0=lundi ... 6=dimanche
    """
    s = str(s or "").strip()
    if not s:
        return None

    out = set()
    for x in s.split(","):
        x = x.strip()
        if not x:
            continue
        try:
            v = int(x)
            if 1 <= v <= 7:
                out.add(v - 1)  # conversion vers weekday()
        except Exception:
            pass

    return out if out else None


# ============================================================
# Normalization Config / Tasks / Holidays
# ============================================================

def normalize_config(cfg: pd.DataFrame) -> pd.DataFrame:
    defaults = {
        "workdays": "1,2,3,4,5",  # Lun..Ven (format UI 1..7)
        "start_from": "",
        "smooth_max_slots_per_day": 1,
        "focus_only_day": 0,
        "smooth_weekdays_default": "1,2,3,4,5",
        "project_colors": "",
        "people": "",
    }
    if cfg is None or cfg.empty:
        return pd.DataFrame([defaults])

    cfg = cfg.copy()
    cfg.columns = [str(c).strip() for c in cfg.columns]
    for k, v in defaults.items():
        if k not in cfg.columns:
            cfg[k] = v
    if cfg.empty:
        cfg = pd.DataFrame([defaults])
    return cfg


def get_people(cfg: pd.DataFrame, xls: pd.ExcelFile) -> List[str]:
    raw = ""
    if cfg is not None and not cfg.empty and "people" in cfg.columns:
        raw = str(cfg.loc[0, "people"] or "").strip()

    if raw:
        people = [p.strip() for p in raw.split(",") if p.strip()]
        if people:
            return people

    out = []
    for s in xls.sheet_names:
        if s == SHEET_CONFIG:
            continue
        if s.endswith(HOL_SUFFIX):
            continue
        if "_" in s:
            continue
        out.append(s)
    return sorted(out)


def normalize_tasks(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "id",
        "project",
        "task",
        "priority",
        "est_halfdays",
        "status",
        "deadline",
        "start_date",
        "mode",
        "focus_weekday",
        "smooth_weekdays",
        "notes",
    ]
    if df is None or df.empty:
        return pd.DataFrame(columns=required)

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    for c in required:
        if c not in df.columns:
            df[c] = ""

    df = df.reset_index(drop=True)
    df["id"] = range(1, len(df) + 1)

    df["priority"] = pd.to_numeric(df["priority"], errors="coerce").fillna(99).astype(int)
    df["est_halfdays"] = pd.to_numeric(df["est_halfdays"], errors="coerce").fillna(0.0).astype(float)

    df["status"] = df["status"].astype(str).str.strip()
    df.loc[df["status"] != "Done", "status"] = "Todo"

    df["deadline"] = df["deadline"].apply(normalize_date)
    df["start_date"] = df["start_date"].apply(normalize_date)

    df["mode"] = df["mode"].fillna("SMOOTH").astype(str).str.upper()
    df.loc[~df["mode"].isin(MODES), "mode"] = "SMOOTH"

    df["focus_weekday"] = pd.to_numeric(df["focus_weekday"], errors="coerce")
    df["smooth_weekdays"] = df["smooth_weekdays"].fillna("").astype(str)

    for c in ["project", "task", "notes"]:
        df[c] = df[c].fillna("").astype(str)

    return df[required].copy()


def normalize_holidays(hol: pd.DataFrame) -> pd.DataFrame:
    if hol is None or hol.empty:
        return pd.DataFrame(columns=["date", "slot", "label"])

    hol = hol.copy()
    hol.columns = [str(c).strip() for c in hol.columns]
    for c in ["date", "slot", "label"]:
        if c not in hol.columns:
            hol[c] = ""

    hol["date"] = hol["date"].apply(normalize_date)

    # ✅ IMPORTANT: on conserve "" pour "journée complète"
    hol["slot"] = hol["slot"].fillna("").astype(str).str.strip().str.upper()

    # ✅ on ne corrige en AM que si slot est non vide ET invalide
    mask_invalid_non_empty = (hol["slot"] != "") & (~hol["slot"].isin(SLOT_LABELS))
    hol.loc[mask_invalid_non_empty, "slot"] = "AM"

    hol["label"] = hol["label"].fillna("").astype(str).str.strip()
    hol.loc[hol["label"] == "", "label"] = "OFF"

    hol = hol.dropna(subset=["date"]).drop_duplicates(subset=["date", "slot"]).sort_values(["date", "slot"])
    return hol[["date", "slot", "label"]].copy()


# ============================================================
# Absences: color per label + annual calendar (label in cell)
# ============================================================
def build_deadlines_map(tasks_df: pd.DataFrame, only_todo: bool = True) -> Dict[date, List[str]]:
    """
    Retourne {date_deadline: ["[P1] Projet — Tâche", ...]}
    """
    if tasks_df is None or tasks_df.empty:
        return {}

    df = tasks_df.copy()
    if only_todo and "status" in df.columns:
        df = df[df["status"] != "Done"]

    df = df.dropna(subset=["deadline"]).copy()
    out: Dict[date, List[str]] = {}

    for _, r in df.iterrows():
        d = r.get("deadline")
        if not isinstance(d, date):
            continue
        label = f"[P{int(r.get('priority', 99))}] {r.get('project','')} — {r.get('task','')}".strip()
        out.setdefault(d, []).append(label)

    # option: trie par priorité (déjà dans le texte, mais on trie quand même)
    for k in out:
        out[k] = sorted(out[k])
    return out

ZONE_A_ICS_URL = "https://fr.ftp.opendatasoft.com/openscol/fr-en-calendrier-scolaire/Zone-A.ics"

@st.cache_data(ttl=24*3600, show_spinner=False)
def fetch_zone_a_ics() -> str:
    r = requests.get(ZONE_A_ICS_URL, timeout=30, verify=False)
    r.raise_for_status()
    return r.text


def _parse_ics_date(line: str) -> Optional[date]:
    # DTSTART;VALUE=DATE:20260801  or DTSTART:20260801T000000Z
    if ":" not in line:
        return None
    v = line.split(":", 1)[1].strip()
    if len(v) >= 8 and v[:8].isdigit():
        y, m, d = int(v[:4]), int(v[4:6]), int(v[6:8])
        return date(y, m, d)
    return None


def zone_a_school_holidays_dates(window_start: date, window_end: date) -> Set[date]:
    """
    Retourne les dates (jours) couvertes par les événements "Vacances ..." de Zone A,
    intersectées avec [window_start, window_end].
    """
    ics = fetch_zone_a_ics()

    in_event = False
    dtstart: Optional[date] = None
    dtend: Optional[date] = None
    summary: str = ""

    out: Set[date] = set()

    for raw in ics.splitlines():
        line = raw.strip()

        if line == "BEGIN:VEVENT":
            in_event = True
            dtstart = None
            dtend = None
            summary = ""
            continue

        if line == "END:VEVENT":
            if in_event and dtstart and dtend:
                # On garde uniquement les événements "Vacances ..."
                if summary.lower().startswith("vacances"):
                    # En iCal, DTEND est souvent exclusif pour les dates all-day
                    start = max(dtstart, window_start)
                    end_excl = min(dtend, window_end + timedelta(days=1))
                    d = start
                    while d < end_excl:
                        out.add(d)
                        d += timedelta(days=1)
            in_event = False
            continue

        if not in_event:
            continue

        if line.startswith("DTSTART"):
            dtstart = _parse_ics_date(line)
        elif line.startswith("DTEND"):
            dtend = _parse_ics_date(line)
        elif line.startswith("SUMMARY"):
            # SUMMARY:Vacances de la Toussaint
            summary = line.split(":", 1)[1].strip() if ":" in line else ""

    return out

def build_label_color_map(holidays_df: pd.DataFrame) -> Dict[str, str]:
    if holidays_df is None or holidays_df.empty or "label" not in holidays_df.columns:
        return {}

    labels = (
        holidays_df["label"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", "OFF")
        .unique()
        .tolist()
    )

    palette = px.colors.qualitative.Plotly

    def pick_color(label: str) -> str:
        h = hashlib.md5(label.encode("utf-8")).hexdigest()
        idx = int(h[:8], 16) % len(palette)
        return palette[idx]

    return {lab: pick_color(lab) for lab in sorted(labels)}


def render_rolling_absence_calendar_html(
    holidays_df: pd.DataFrame,
    start_month: date,
    label_colors: Dict[str, str],
    months: int = 12,
    school_days: Optional[Set[date]] = None,
    deadlines_map: Optional[Dict[date, List[str]]] = None,
) -> str:
    # map: date -> { "AM": label, "PM": label }
    m: Dict[date, Dict[str, str]] = {}
    if holidays_df is not None and not holidays_df.empty:
        for _, r in holidays_df.iterrows():
            d = r.get("date")
            slot_raw = r.get("slot", "")
            slot = str(slot_raw).strip().upper()
            label = str(r.get("label", "")).strip() or "OFF"

            if isinstance(d, date):
                day_map = m.setdefault(d, {})

                # ✅ slot vide = journée complète
                if slot == "" or pd.isna(slot_raw):
                    day_map["AM"] = label
                    day_map["PM"] = label

                # demi-journée explicite
                elif slot in ("AM", "PM"):
                    day_map[slot] = label

    cal = calendar.Calendar(firstweekday=0)  # lundi

    def badge(kind: str, label: str) -> str:
        safe = (label or "OFF").replace("<", "&lt;").replace(">", "&gt;")
        color = label_colors.get(label or "OFF", "#444")
        return f"""
        <div class="mpw-badge" style="
            background:{color};
            border:1px solid {color};
            color:#fff;
        ">
            <span class="mpw-slot">{kind}</span>
            <span class="mpw-sep">—</span>
            <span class="mpw-label">{safe}</span>
        </div>
        """

    header = "".join(f"<th>{w}</th>" for w in ["Lun", "Mar", "Mer", "Jeu", "Ven"])

    months_html: List[str] = []

    # 🔁 boucle sur 12 mois glissants
    for i in range(months):
        y = (start_month.year + (start_month.month - 1 + i) // 12)
        mth = ((start_month.month - 1 + i) % 12) + 1

        month_name = calendar.month_name[mth]
        weeks = cal.monthdayscalendar(y, mth)

        rows: List[str] = []
        for week in weeks:
            week_5 = week[:5]  # Lun..Ven
            if all(dn == 0 for dn in week_5):
                continue

            tds: List[str] = []
            for daynum in week_5:
                if daynum == 0:
                    tds.append("<td class='mpw-empty'></td>")
                    continue

                d = date(y, mth, daynum)
                day_slots = m.get(d, {})
                am = day_slots.get("AM")
                pm = day_slots.get("PM")

                # ✅ Deadline marker
                dl = (deadlines_map or {}).get(d, [])
                if dl:
                    tooltip = "&#10;".join([x.replace("<","&lt;").replace(">","&gt;") for x in dl])
                    marker = f"<span class='mpw-deadline' title='{tooltip}'>🎯</span>"
                else:
                    marker = ""
                cell_parts = [f"<div class='mpw-daynum'>{daynum}{marker}</div>"]

                if am and pm and am == pm:
                    cell_parts.append(badge("Journée", am))
                    td_cls = "mpw-day mpw-full"
                else:
                    if am:
                        cell_parts.append(badge("AM", am))
                    if pm:
                        cell_parts.append(badge("PM", pm))
                    td_cls = "mpw-day"
                
                is_school_holiday = (school_days is not None and d in school_days)
                # classe de fond bleu clair
                extra_cls = " mpw-school" if is_school_holiday else ""
                td_cls = td_cls + extra_cls


                tds.append(f"<td class='{td_cls}'>" + "".join(cell_parts) + "</td>")

            rows.append("<tr>" + "".join(tds) + "</tr>")

        months_html.append(
            f"""
            <div class="mpw-month">
              <div class="mpw-month-title">{month_name} {y}</div>
              <table class="mpw-cal mpw-cal-5">
                <thead><tr>{header}</tr></thead>
                <tbody>
                  {''.join(rows)}
                </tbody>
              </table>
            </div>
            """
        )

    # CSS identique à avant
    style = """
    <style>
      .mpw-school{
        background: #e8f2ff !important;   /* bleu clair */
        border: 1px solid #cfe4ff !important;
      }
      .mpw-grid{
        display:grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 14px;
      }
      @media (max-width: 1100px){
        .mpw-grid{ grid-template-columns: repeat(2, 1fr); }
      }
      @media (max-width: 700px){
        .mpw-grid{ grid-template-columns: 1fr; }
      }
      .mpw-month{
        border: 1px solid #e6e6e6;
        border-radius: 12px;
        padding: 10px;
        background: white;
      }
      .mpw-month-title{
        font-weight: 800;
        margin: 2px 4px 8px;
        font-size: 14px;
      }
      table.mpw-cal{
        width: 100%;
        table-layout: fixed;
        border-collapse: separate;
        border-spacing: 6px;
      }
      .mpw-cal th{
        font-size: 11px;
        color: #666;
        font-weight: 700;
        text-align: center;
      }
      .mpw-cal td{
        vertical-align: top;
        height: 40px;       /* ⬅️ plus compact */
        border-radius: 8px;
        background: #fafafa;
        padding: 3px;       /* ⬅️ moins de padding */
      }
      .mpw-daynum{
        display:flex;
        align-items:center;
        gap:6px;
        font-weight: 900;
        font-size: 11px;
        margin-bottom: 1px;
        line-height: 1;
      }
      .mpw-deadline{
        font-size: 12px;
        cursor: help;
        opacity: 0.95;
        filter: saturate(1.2); 
      }
      .mpw-badge{
        font-size: 9.5px;   /* ⬅️ un poil plus petit */
        padding: 2px 6px;
        border-radius: 999px;
        margin-top: 1px;    /* ⬅️ colle le badge */
        font-weight: 900;
        line-height: 1.0;
      }
      .mpw-empty{ background: transparent !important; }
      .mpw-full{
        background: #f0f0f0;
        border: 1px solid #e2e2e2;
      }
    </style>
    """

    return style + f"<div class='mpw-grid'>{''.join(months_html)}</div>"


# ============================================================
# Colors + HTML helpers (planning tasks)
# ============================================================

def load_project_colors_from_cfg(cfg: pd.DataFrame) -> Dict[str, str]:
    s = str(cfg.loc[0, "project_colors"]) if "project_colors" in cfg.columns else ""
    if not s.strip():
        return {}
    try:
        d = json.loads(s)
        if isinstance(d, dict):
            return {str(k): str(v) for k, v in d.items()}
        return {}
    except Exception:
        return {}


def ensure_project_color_map(tasks_df: pd.DataFrame, existing: Dict[str, str]) -> Dict[str, str]:
    projects = sorted([p for p in tasks_df["project"].dropna().astype(str).unique().tolist() if p.strip()])
    palette = px.colors.qualitative.Plotly
    used = set(existing.values())

    def next_free_color(seed: int) -> str:
        for k in range(seed, seed + len(palette) * 4):
            c = palette[k % len(palette)]
            if c not in used:
                return c
        return palette[seed % len(palette)]

    m = dict(existing)
    seed = 0
    for p in projects:
        if p not in m:
            c = next_free_color(seed)
            m[p] = c
            used.add(c)
            seed += 1
    return m


def colorize_label_html(label: str, color: str) -> str:
    safe = (label or "").replace("<", "&lt;").replace(">", "&gt;")
    return (
        "<div style='line-height:1.15; white-space:normal; word-break:break-word;'>"
        f"<span style='color:{color}; font-weight:700;'>●</span> {safe}"
        "</div>"
    )


def holiday_cell_html(label: str) -> str:
    safe = (label or "OFF").replace("<", "&lt;").replace(">", "&gt;")
    return (
        "<div style='padding:4px 6px; border-radius:6px; "
        "background:#f2f2f2; color:#333; font-weight:600; text-align:center;'>"
        f"🏖️ {safe}"
        "</div>"
    )


def build_holidays_map(holidays_df: pd.DataFrame) -> Dict[str, str]:
    m: Dict[str, str] = {}
    if holidays_df is None or holidays_df.empty:
        return m
    for _, r in holidays_df.iterrows():
        d = r.get("date")
        slot = str(r.get("slot", "")).upper()
        label = str(r.get("label", "")).strip() or "OFF"
        if isinstance(d, date) and slot in ("AM", "PM"):
            m[f"{d.isoformat()} {slot}"] = label
    return m


# ============================================================
# Scheduling engine (macro)
# ============================================================

@dataclass(frozen=True)
class Slot:
    day: date
    ampm: str

    def key(self) -> str:
        return f"{self.day.isoformat()} {self.ampm}"


def is_workday(d: date, workdays: Tuple[int, ...]) -> bool:
    return d.weekday() in workdays


def next_workday(d: date, workdays: Tuple[int, ...]) -> date:
    while not is_workday(d, workdays):
        d += timedelta(days=1)
    return d


def build_blocked_slots(holidays_df: pd.DataFrame) -> Set[str]:
    blocked = set()
    if holidays_df is None or holidays_df.empty:
        return blocked

    for _, r in holidays_df.iterrows():
        d = r.get("date")
        slot_raw = r.get("slot", "")
        slot = str(slot_raw).strip().upper()

        if not isinstance(d, date):
            continue

        # ✅ slot vide = journée complète
        if slot == "" or pd.isna(slot_raw):
            blocked.add(f"{d.isoformat()} AM")
            blocked.add(f"{d.isoformat()} PM")

        # demi-journée explicite
        elif slot in ("AM", "PM"):
            blocked.add(f"{d.isoformat()} {slot}")

    return blocked


def generate_slots(start_from: date, workdays: Tuple[int, ...], blocked: Set[str], max_days: int = 365) -> List[Slot]:
    slots: List[Slot] = []
    d = next_workday(start_from, workdays)
    days_count = 0

    while days_count < max_days:
        for ampm in ("AM", "PM"):
            s = Slot(d, ampm)
            if s.key() not in blocked:
                slots.append(s)
        d = next_workday(d + timedelta(days=1), workdays)
        days_count += 1

    return slots


def sort_backlog(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(["priority", "deadline", "id"], ascending=[True, True, True], na_position="last")


def schedule_macro_halfday(
    tasks_df: pd.DataFrame,
    *,
    workdays: Tuple[int, ...],
    start_from: date,
    holidays_df: pd.DataFrame,
    smooth_max_slots_per_day: int,
    smooth_weekdays_default: Tuple[int, ...],
    focus_only_day: bool,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = tasks_df[(tasks_df["status"] != "Done") & (tasks_df["est_halfdays"] > 0)].copy()
    if df.empty:
        return pd.DataFrame(columns=["date", "slot", "task_id", "project", "task", "mode"]), df

    df = sort_backlog(df).copy()
    df["remaining_slots"] = df["est_halfdays"].astype(float)
    df["start_planned"] = pd.NaT
    df["end_planned"] = pd.NaT

    blocked = build_blocked_slots(holidays_df)
    slots = generate_slots(start_from, workdays, blocked, max_days=730)

    used_per_day: Dict[Tuple[int, date], int] = {}
    plan_rows: List[Dict] = []

    safety = 0
    slot_i = 0
    fallback_days_set = set(smooth_weekdays_default)

    while (df["remaining_slots"] > 1e-9).any():
        safety += 1
        if safety > 200000:
            break

        if slot_i >= len(slots):
            last_day = slots[-1].day if slots else start_from
            more = generate_slots(last_day + timedelta(days=1), workdays, blocked, max_days=365)
            slots.extend(more)

        slot = slots[slot_i]
        slot_i += 1

        focus_candidates = []
        for idx, r in df.iterrows():
            if float(df.loc[idx, "remaining_slots"]) <= 1e-9:
                continue
            if str(r["mode"]).upper() == "FOCUS":
                fw = r["focus_weekday"]
                if pd.notna(fw) and int(fw) == slot.day.weekday():
                    sd = normalize_date(r.get("start_date", None))
                    if sd is not None and slot.day < sd:
                        continue
                    focus_candidates.append(idx)

        has_focus_today = len(focus_candidates) > 0

        if has_focus_today and focus_only_day:
            candidate_indices = focus_candidates
        else:
            candidate_indices = focus_candidates + [idx for idx in df.index.tolist() if idx not in focus_candidates]

        chosen_idx: Optional[int] = None

        for idx in candidate_indices:
            r = df.loc[idx]
            rem = float(df.loc[idx, "remaining_slots"])
            if rem <= 1e-9:
                continue

            sd = normalize_date(r.get("start_date", None))
            if sd is not None and slot.day < sd:
                continue

            mode = str(r["mode"]).upper()

            if mode == "FOCUS":
                fw = r["focus_weekday"]
                if pd.isna(fw) or int(fw) != slot.day.weekday():
                    continue

            if mode in ("SMOOTH", "FOCUS"):
                task_days = parse_weekdays_list(r.get("smooth_weekdays", ""))
                effective_days = task_days if task_days is not None else fallback_days_set
                if slot.day.weekday() in effective_days:
                    cap = max(1, int(smooth_max_slots_per_day))
                    used = used_per_day.get((int(r["id"]), slot.day), 0)
                    if used >= cap:
                        continue

            chosen_idx = idx
            break

        if chosen_idx is None:
            continue

        r = df.loc[chosen_idx]
        tid = int(r["id"])
        mode = str(r["mode"]).upper()

        plan_rows.append(
            {
                "date": slot.day,
                "slot": slot.ampm,
                "task_id": tid,
                "project": r["project"],
                "task": r["task"],
                "mode": mode,
            }
        )

        if pd.isna(df.loc[chosen_idx, "start_planned"]):
            df.loc[chosen_idx, "start_planned"] = pd.Timestamp(slot.day)

        df.loc[chosen_idx, "remaining_slots"] = float(df.loc[chosen_idx, "remaining_slots"]) - 1.0
        if float(df.loc[chosen_idx, "remaining_slots"]) <= 1e-9:
            df.loc[chosen_idx, "end_planned"] = pd.Timestamp(slot.day)

        if mode in ("SMOOTH", "FOCUS"):
            used_per_day[(tid, slot.day)] = used_per_day.get((tid, slot.day), 0) + 1

    plan_df = pd.DataFrame(plan_rows)
    return plan_df, df


def iso_week_start(d: date) -> date:
    return d - timedelta(days=d.weekday())


def make_week_grid_html(
    plan_df: pd.DataFrame,
    anchor_day: date,
    weeks: int,
    project_color_map: Dict[str, str],
    holidays_map: Dict[str, str],
) -> pd.DataFrame:
    p = plan_df.copy()
    if not p.empty:
        p["label"] = p.apply(lambda r: f"{r['project']} • {r['task']}", axis=1)

    # NOTE: start_from est une variable globale (définie plus bas dans l'UI)
    min_day = start_from
    if not p.empty:
        min_day = min(p["date"].min(), start_from)

    week0 = iso_week_start(anchor_day)
    week_starts = [week0 + timedelta(days=7 * i) for i in range(weeks)]

    cols = []
    for wd in range(7):
        for ampm in ("AM", "PM"):
            cols.append(f"{WEEKDAY_LABELS[wd]} {ampm}")

    rows = []
    for ws in week_starts:
        row = {"Semaine": f"{ws.isocalendar().year}-W{ws.isocalendar().week:02d}"}
        for c in cols:
            row[c] = ""

        for wd in range(7):
            d = ws + timedelta(days=wd)
            for ampm in ("AM", "PM"):
                col = f"{WEEKDAY_LABELS[wd]} {ampm}"
                key = f"{d.isoformat()} {ampm}"
                pieces: List[str] = []

                if key in holidays_map:
                    pieces.append(holiday_cell_html(holidays_map[key]))

                if not p.empty:
                    sub = p[(p["date"] == d) & (p["slot"] == ampm)]
                    if not sub.empty:
                        for _, rr in sub.iterrows():
                            proj = str(rr["project"])
                            color = project_color_map.get(proj, "#666")
                            pieces.append(colorize_label_html(f"{rr['project']} • {rr['task']}", color))

                row[col] = "<br/>".join(pieces)

        rows.append(row)

    return pd.DataFrame(rows)


def auto_height(df, row_height=35, max_height=600):
    return min(max_height, (len(df) + 1) * row_height)


# ============================================================
# Streamlit UI
# ============================================================

st.set_page_config(page_title="Mini-Planyway", layout="wide", initial_sidebar_state="collapsed")
st.markdown(
    """
    <style>
    .block-container { padding-top: 1.2rem !important; }
    h1 { margin-bottom: 0.3rem !important; }
    header { visibility: hidden; height: 0px; }
    [data-testid="stToolbar"] { display: none; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("🗓️ Mini planification équipe")

if not PA_ENDPOINT:
    st.error("PA_ENDPOINT manquant.")
    st.stop()

try:
    xlsx_bytes = fetch_excel_from_power_automate(PA_ENDPOINT)
    xls = open_xls_bytes(xlsx_bytes)
except Exception as e:
    st.error(
        "Impossible de récupérer le fichier via Power Automate.\n\n"
        f"Détail: {e}\n\n"
        "👉 Vérifie que le flow renvoie bien le binaire XLSX (Content-Type xlsx) "
        "ou un JSON {contentBytes}."
    )
    st.stop()

cfg = normalize_config(read_sheet(xls, SHEET_CONFIG))
people = get_people(cfg, xls)
if not people:
    st.error("Aucune personne détectée. Renseigne Config.people='Alice,Bob' ou crée des onglets prénom.")
    st.stop()

if "person" not in st.session_state or st.session_state["person"] not in people:
    st.session_state["person"] = people[0]

with st.expander("⚙️ Contrôles", expanded=True):
    c_left, c_right = st.columns([1, 1], vertical_alignment="center")

    with c_left:
        if st.button("🔄 Rafraîchir la source", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    with c_right:
        person = st.selectbox(
            "Personne",
            options=people,
            index=people.index(st.session_state["person"]),
            key="person_select",
            label_visibility="collapsed",
        )
        st.session_state["person"] = person

person = st.session_state["person"]

tasks = normalize_tasks(read_sheet(xls, person))
holidays = normalize_holidays(read_sheet(xls, f"{person}{HOL_SUFFIX}"))

existing_colors = load_project_colors_from_cfg(cfg)
project_color_map = ensure_project_color_map(tasks, existing_colors)

workdays = parse_workdays(cfg.loc[0, "workdays"]) or (0, 1, 2, 3, 4)
smooth_max_slots_per_day = int(cfg.loc[0, "smooth_max_slots_per_day"]) if "smooth_max_slots_per_day" in cfg.columns else 1
smooth_weekdays_default = parse_workdays(cfg.loc[0, "smooth_weekdays_default"]) or workdays
focus_only_day = bool(int(cfg.loc[0, "focus_only_day"])) if "focus_only_day" in cfg.columns else False

start_from_str = str(cfg.loc[0, "start_from"]) if "start_from" in cfg.columns else ""
start_from = normalize_date(start_from_str) if start_from_str.strip() else None
start_from = start_from or date.today()

plan_df, planned_tasks = schedule_macro_halfday(
    tasks,
    workdays=tuple(workdays),
    start_from=start_from,
    holidays_df=holidays,
    smooth_max_slots_per_day=int(smooth_max_slots_per_day),
    smooth_weekdays_default=tuple(smooth_weekdays_default),
    focus_only_day=bool(focus_only_day),
)

tab_tasks, tab_holidays, tab_week, tab_gantt = st.tabs(
    ["🧾 Tâches", "🏖️ Absence", "🗂️ Vue hebdo", "📈 Gantt macro"]
)

# --------------------------
# Tasks tab
# --------------------------
with tab_tasks:
    st.subheader(f"Tâches — {person}")

    with st.expander("🔎 Filtres tâches", expanded=False):
        c0, c1, c2, c3 = st.columns([2, 2, 2, 2])

        with c0:
            q = st.text_input("Recherche", value="", placeholder="project / task / notes…", key=f"tasks_q__{person}")

        projects = sorted([p for p in tasks["project"].dropna().unique().tolist() if str(p).strip() != ""])
        with c1:
            f_project = st.multiselect(
                "Projets",
                options=projects,
                default=projects,
                key=f"tasks_fproj__{person}",
                label_visibility="collapsed",
                placeholder="Projets",
            )
        with c2:
            f_status = st.multiselect(
                "Statuts",
                options=STATUSES,
                default=[s for s in STATUSES if s != "Done"],
                key=f"tasks_fstatus__{person}",
                label_visibility="collapsed",
                placeholder="Statuts",
            )
        with c3:
            f_mode = st.multiselect(
                "Modes",
                options=MODES,
                default=MODES,
                key=f"tasks_fmode__{person}",
                label_visibility="collapsed",
                placeholder="Modes",
            )

    tasks_view = tasks.copy()
    if f_project:
        tasks_view = tasks_view[tasks_view["project"].isin(f_project)]
    tasks_view = tasks_view[tasks_view["status"].isin(f_status)]
    tasks_view = tasks_view[tasks_view["mode"].isin(f_mode)]

    if q.strip():
        qq = q.strip().lower()
        mask = (
            tasks_view["project"].astype(str).str.lower().str.contains(qq, na=False)
            | tasks_view["task"].astype(str).str.lower().str.contains(qq, na=False)
            | tasks_view["notes"].astype(str).str.lower().str.contains(qq, na=False)
        )
        tasks_view = tasks_view[mask]

    tasks_sorted = tasks_view.sort_values(
        ["priority", "deadline", "id"],
        ascending=[True, True, True],
        na_position="last",
    )

    st.dataframe(
        tasks_sorted,
        use_container_width=True,
        hide_index=True,
        height=auto_height(tasks_sorted),
    )

# --------------------------
# Holidays tab (annual calendar, label written inside cells)
# --------------------------
with tab_holidays:
    st.subheader(f"Congés / Formations — {person}")

    if holidays.empty:
        st.info("Aucune absence.")
    else:
        label_colors = build_label_color_map(holidays)

        start_month = date.today().replace(day=1)
        months = 12
        window_start = start_month
        # fin de fenêtre = dernier jour du mois (start_month + 12 mois - 1 jour)
        end_y = (start_month.year + (start_month.month - 1 + months) // 12)
        end_m = ((start_month.month - 1 + months) % 12) + 1
        window_end = date(end_y, end_m, 1) - timedelta(days=1)
        school_days = zone_a_school_holidays_dates(window_start, window_end)
        deadlines_map = build_deadlines_map(tasks, only_todo=True)

        html = render_rolling_absence_calendar_html(
            holidays_df=holidays,
            start_month=start_month,
            label_colors=label_colors,
            months=months,
            school_days=school_days,
            deadlines_map=deadlines_map,
        )

        components.html(html, height=1450, scrolling=True)

# --------------------------
# Week view
# --------------------------
with tab_week:
    st.subheader(f"Vue macro hebdo — {person}")

    if tasks.empty:
        st.info("Aucune tâche.")
    else:
        total_slots = float(tasks[tasks["status"] != "Done"]["est_halfdays"].sum())
        finish = pd.to_datetime(planned_tasks["end_planned"], errors="coerce").max()
        finish_str = finish.date().isoformat() if pd.notna(finish) else "—"

        c1, c2, c3 = st.columns(3)
        c1.metric("Charge restante (demi-journées)", round(total_slots, 1))
        c2.metric("Fin prévisionnelle (macro)", finish_str)

        st.markdown("### Grille hebdo")
        weeks_to_show = st.slider("Nb de semaines à afficher (grille)", 4, 16, 8, 1)

        show_cols = ["Semaine"]
        for wd in range(7):
            if wd in workdays:
                for ampm in ("AM", "PM"):
                    show_cols.append(f"{WEEKDAY_LABELS[wd]} {ampm}")

        holidays_map = build_holidays_map(holidays)

        week_grid_html = make_week_grid_html(
            plan_df,
            anchor_day=date.today(),
            weeks=int(weeks_to_show),
            project_color_map=project_color_map,
            holidays_map=holidays_map,
        )[show_cols]

        st.markdown(
            """
            <style>
            table { table-layout: fixed; width: 100%; }
            th, td { padding: 4px 6px !important; font-size: 12px !important; vertical-align: top; }
            td { word-wrap: break-word; white-space: normal; }
            </style>
            """,
            unsafe_allow_html=True,
        )
        st.markdown(week_grid_html.to_html(escape=False, index=False), unsafe_allow_html=True)

        st.markdown("### Vue charge par semaine")

        tmp = plan_df.copy()
        if not tmp.empty:
            tmp["week_start"] = tmp["date"].apply(iso_week_start)
            tmp["week"] = tmp["week_start"].apply(lambda d: f"{d.isocalendar().year}-W{d.isocalendar().week:02d}")
            w_tasks = (
                tmp.groupby(["week_start", "week", "project"], as_index=False)
                .size()
                .rename(columns={"size": "slots", "project": "series"})
            )
        else:
            w_tasks = pd.DataFrame(columns=["week_start", "week", "series", "slots"])

        hol_df = holidays.copy()
        if hol_df.empty:
            w_hol = pd.DataFrame(columns=["week_start", "week", "series", "slots"])
        else:
            hol_df = hol_df.dropna(subset=["date"]).drop_duplicates(subset=["date", "slot"]).copy()
            hol_df = hol_df[hol_df["date"].apply(lambda d: isinstance(d, date) and d.weekday() in workdays)]

            hol_df["week_start"] = hol_df["date"].apply(iso_week_start)
            hol_df["week"] = hol_df["week_start"].apply(lambda d: f"{d.isocalendar().year}-W{d.isocalendar().week:02d}")

            # ✅ slot vide = journée complète = 2 demi-journées
            hol_df["slot_norm"] = hol_df["slot"].fillna("").astype(str).str.strip().str.upper()
            hol_df["slots_weight"] = hol_df["slot_norm"].apply(lambda s: 2 if s == "" else 1)

            w_hol = (
                hol_df.groupby(["week_start", "week"], as_index=False)["slots_weight"]
                .sum()
                .rename(columns={"slots_weight": "slots"})
            )
            w_hol["series"] = HOLIDAY_SERIES_NAME

        w = pd.concat(
            [
                w_tasks[["week_start", "week", "series", "slots"]],
                w_hol[["week_start", "week", "series", "slots"]],
            ],
            ignore_index=True,
        ).sort_values(["week_start", "series"])

        week_order = w["week"].drop_duplicates().tolist()
        color_map_all = dict(project_color_map)
        color_map_all[HOLIDAY_SERIES_NAME] = HOLIDAY_COLOR

        fig = px.bar(
            w,
            x="week",
            y="slots",
            color="series",
            barmode="stack",
            color_discrete_map=color_map_all,
            category_orders={"week": week_order},
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("### Dates prévues par tâche")
        out = planned_tasks.copy()
        out["start_planned"] = pd.to_datetime(out["start_planned"], errors="coerce").dt.date
        out["end_planned"] = pd.to_datetime(out["end_planned"], errors="coerce").dt.date
        deadline_map = tasks.set_index("id")["deadline"].to_dict()
        out["deadline"] = out["id"].map(deadline_map)

        def late(row):
            d = row["deadline"]
            e = row["end_planned"]
            if d is None or pd.isna(d) or e is None or pd.isna(e):
                return False
            return e > d

        out["late"] = out.apply(late, axis=1)

        show = [
            "id",
            "project",
            "task",
            "priority",
            "est_halfdays",
            "mode",
            "focus_weekday",
            "start_planned",
            "end_planned",
            "deadline",
            "late",
        ]

        out_sorted = out[show].sort_values(
            ["priority", "deadline", "id"],
            ascending=[True, True, True],
            na_position="last",
        )

        st.dataframe(
            out_sorted,
            use_container_width=True,
            hide_index=True,
            height=auto_height(out_sorted),
        )

# --------------------------
# Gantt
# --------------------------
with tab_gantt:
    st.subheader(f"Gantt macro — {person}")
    if plan_df.empty:
        st.info("Aucune tâche planifiée.")
    else:
        g = planned_tasks.copy()
        g["Start"] = pd.to_datetime(g["start_planned"], errors="coerce")
        g["Finish"] = pd.to_datetime(g["end_planned"], errors="coerce") + pd.Timedelta(hours=23, minutes=59)
        g = g.dropna(subset=["Start", "Finish"]).copy()
        g["Label"] = g.apply(lambda r: f"[P{int(r['priority'])}] {r['project']} — {r['task']}", axis=1)

        fig = px.timeline(
            g.sort_values(["priority", "Start"]),
            x_start="Start",
            x_end="Finish",
            y="Label",
            color="project",
            color_discrete_map=project_color_map,
            hover_data=["id", "project", "priority", "est_halfdays", "mode", "focus_weekday"],
        )
        fig.update_yaxes(autorange="reversed")
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Macro: les barres viennent des slots AM/PM planifiés (pas d’heures).")