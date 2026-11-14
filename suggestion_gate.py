# -*- coding: utf-8 -*-
"""
Suggestion gate — gestiona soft introducing de capacidades de Knot.

Triggers explícitos detectan oportunidades en handlers individuales.
El gate decide si emitir, aplica cooldown, dismiss count y disable per-trigger.

Estado persistente en Config DB columna 'Feature Hints' (JSON dict):
{
  "trigger_id": {
    "first_suggested_at": ISO datetime,
    "last_suggested_at": ISO datetime,
    "accepted": bool,
    "dismissed_count": int,
    "disabled": bool,
  }
}
"""

from datetime import datetime, timedelta
from typing import Optional

from state import now_argentina


def _ahora() -> datetime:
    """Hora argentina sin zona. El servidor corre en UTC: con la hora del sistema "hoy"
    cambiaba a las 21:00 y el tope diario de sugerencias se contaba mal."""
    return now_argentina().replace(tzinfo=None)

# Configuración global
GLOBAL_DAILY_CAP = 1            # max sugerencias por día (excepto must_fire)
PER_TRIGGER_COOLDOWN_DAYS = 7   # no repetir misma sugerencia antes de N días
DISMISS_THRESHOLD = 2           # tras N rechazos, no volver a sugerir


class Hint:
    """Una sugerencia detectada por un handler. Sin lógica de filtrado."""
    def __init__(
        self,
        trigger_id: str,
        message: str,
        action_intent: str,
        payload: dict = None,
        must_fire: bool = False,
    ):
        self.trigger_id = trigger_id        # ej: "gym_recurring", "first_combustible"
        self.message = message              # texto a enviar al usuario
        self.action_intent = action_intent  # qué hacer si dice que sí (ej: "create_recurring_event")
        self.payload = payload or {}        # data necesaria para ejecutar la acción
        self.must_fire = must_fire          # bypass del cap global (para Capa 1: sin data)


def should_fire(hint: Hint, hints_state: dict, today_count: int) -> bool:
    """
    Decide si emitir un hint.
    - hints_state: dict completo de feature_hints del usuario
    - today_count: cuántas sugerencias ya se emitieron hoy
    """
    if hint.must_fire:
        return True

    if today_count >= GLOBAL_DAILY_CAP:
        return False

    state = hints_state.get(hint.trigger_id, {})
    if state.get("disabled"):
        return False
    if state.get("dismissed_count", 0) >= DISMISS_THRESHOLD:
        return False
    if state.get("accepted"):
        # Si ya lo aceptó, no volver a ofrecer (la feature ya está en uso)
        return False
    last = state.get("last_suggested_at")
    if last:
        try:
            last_dt = datetime.fromisoformat(last)
            if _ahora() - last_dt < timedelta(days=PER_TRIGGER_COOLDOWN_DAYS):
                return False
        except Exception:
            pass
    return True


def record_suggested(hints_state: dict, trigger_id: str) -> None:
    """Marca que un hint se acaba de emitir."""
    now_iso = _ahora().isoformat()
    s = hints_state.get(trigger_id, {})
    if not s.get("first_suggested_at"):
        s["first_suggested_at"] = now_iso
    s["last_suggested_at"] = now_iso
    hints_state[trigger_id] = s


def record_accepted(hints_state: dict, trigger_id: str) -> None:
    s = hints_state.get(trigger_id, {})
    s["accepted"] = True
    hints_state[trigger_id] = s


def record_dismissed(hints_state: dict, trigger_id: str) -> None:
    s = hints_state.get(trigger_id, {})
    s["dismissed_count"] = s.get("dismissed_count", 0) + 1
    hints_state[trigger_id] = s


def disable_trigger(hints_state: dict, trigger_id: str) -> None:
    s = hints_state.get(trigger_id, {})
    s["disabled"] = True
    hints_state[trigger_id] = s


def count_today(hints_state: dict) -> int:
    """Cuántos hints se emitieron hoy (excluyendo must_fire que no consumen cap)."""
    today = _ahora().date()
    count = 0
    for s in hints_state.values():
        last = s.get("last_suggested_at")
        if last:
            try:
                if datetime.fromisoformat(last).date() == today:
                    count += 1
            except Exception:
                pass
    return count
