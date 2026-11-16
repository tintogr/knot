"""
El "mundo" falso del banco de conversaciones.

El banco corre a Knot ENTERO (agente de entrada, módulos, herramientas) contra el modelo
real, pero sin tocar nada de verdad: Notion, WhatsApp y Google Calendar se reemplazan por
esta copia en memoria, que además anota todo lo que Knot hizo para poder comprobarlo.

Así se puede probar "vianda 11500" o "no, eran 12500" y mirar qué quedó guardado, sin
ensuciar los datos reales ni mandar un solo mensaje.
"""
import types
from calendar import monthrange
from datetime import date, timedelta

N = types.SimpleNamespace

INGRESO = "→INGRESO←"
EGRESO = "← EGRESO →"


def _es_ingreso(valor: str) -> bool:
    return "INGRESO" in (valor or "").upper()


class DatosFalsos:
    """Reemplaza a NotionDataStore. Solo lo que Knot usa al conversar."""

    def __init__(self, mundo: "Mundo"):
        self.m = mundo
        self._services = mundo.servicios
        self._n = 0

    def _id(self, prefijo: str) -> str:
        self._n += 1
        return f"{prefijo}{self._n:03d}"

    # Lo que no esté definido acá: no rompe, queda anotado y devuelve una lista vacía.
    def __getattr__(self, nombre):
        if nombre.startswith("__"):
            raise AttributeError(nombre)

        async def _sin_hacer(*a, **k):
            self.m.no_previstas.append(nombre)
            return []
        return _sin_hacer

    # ── Servicios ────────────────────────────────────────────────────────────
    def service_of(self, texto: str):
        t = f" {(texto or '').lower()} "
        for svc in self._services:
            terminos = [svc.get("servicio"), svc.get("empresa")] + list(svc.get("aliases") or [])
            if any(x and f" {x.lower()} " in t.replace("-", " ").replace("—", " ") for x in terminos):
                return svc
        return None

    async def load_services(self):
        return self._services

    async def registrar_pago_servicio(self, texto, fecha_pago):
        return None

    @staticmethod
    def vencimiento_de(notas):
        import re
        m = re.search(r"vence\s+(\d{1,2})/(\d{1,2})/(\d{4})", notas or "", re.I)
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1))) if m else None
        except ValueError:
            return None

    # ── Finanzas ─────────────────────────────────────────────────────────────
    def _vivos(self):
        return [x for x in self.m.movimientos if x.id not in self.m.archivados]

    async def create_expense(self, data: dict):
        e = N(id=self._id("mov"), name=data["name"], value_ars=float(data["value_ars"]),
              in_out="INGRESO" if _es_ingreso(data.get("in_out")) else "EGRESO",
              categories=list(data.get("categories") or []), method="", date=data.get("date"),
              time=data.get("time"), client=list(data.get("client") or []), emoji=data.get("emoji") or "",
              notes=data.get("notes"), liters=data.get("liters"), estado=data.get("estado"))
        if data.get("payment_method_id"):
            pm = next((p for p in self.m.medios if p.id == data["payment_method_id"]), None)
            e.method = pm.name if pm else ""
        self.m.movimientos.append(e)
        self.m.creados.append(e)
        return e

    async def get_expense(self, entry_id):
        limpio = (entry_id or "").replace("-", "")
        return next((x for x in self._vivos() if x.id.replace("-", "") == limpio), None)

    async def update_expense(self, entry_id, updates: dict):
        e = await self.get_expense(entry_id)
        if not e:
            raise RuntimeError(f"no existe {entry_id}")
        self.m.cambios.append((e.id, dict(updates)))
        for clave, campo in (("value_ars", "value_ars"), ("name", "name"), ("categories", "categories"),
                             ("notes", "notes"), ("estado", "estado"), ("date", "date"),
                             ("liters", "liters"), ("emoji", "emoji")):
            if clave in updates and updates[clave] is not None:
                setattr(e, campo, updates[clave])
        if updates.get("payment_method_id"):
            pm = next((p for p in self.m.medios if p.id == updates["payment_method_id"]), None)
            e.method = pm.name if pm else e.method
        return e

    async def archive_expense(self, entry_id):
        self.m.archivados.add(entry_id)
        return True

    async def find_recent_duplicate(self, name, value_ars, date_str, within_minutes=10):
        return False

    async def find_category_from_history(self, name, predicted_cats):
        return predicted_cats, False

    async def increment_payment_method_uses(self, page_id, current_uses):
        return None

    async def load_payment_methods(self):
        return self.m.medios

    def _filtrar(self, filtro):
        out = []
        for e in self._vivos():
            if filtro is not None and filtro.name_contains and filtro.name_contains.lower() not in (e.name or "").lower():
                continue
            if filtro is not None and filtro.category and filtro.category not in (e.categories or []):
                continue
            if filtro is not None and filtro.date_range and e.date:
                d = date.fromisoformat(str(e.date)[:10])
                if not (filtro.date_range.start <= d <= filtro.date_range.end):
                    continue
            out.append(e)
        out.sort(key=lambda x: str(x.date or ""), reverse=True)
        return out[: (filtro.limit if filtro is not None else 50)]

    async def query_expenses(self, filtro=None):
        return self._filtrar(filtro)

    async def search_expenses(self, query, month=None):
        from state import QueryFilter, DateRange, now_argentina
        month = month or now_argentina().strftime("%Y-%m")
        a, mm = (int(x) for x in month.split("-")[:2])
        return self._filtrar(QueryFilter(name_contains=query, limit=10, date_range=DateRange(
            start=date(a, mm, 1), end=date(a, mm, monthrange(a, mm)[1]))))

    async def get_services_summary(self, month=None):
        return [e for e in await self.search_expenses("", month) if "Recurrente" in (e.categories or [])]

    async def get_financial_summary(self, month=None):
        entradas = await self.search_expenses("", month)
        ingresos = egresos = pendiente = 0
        por_cat, impagas = {}, []
        for e in entradas:
            if e.in_out == "INGRESO":
                ingresos += e.value_ars
            elif e.estado == "Impaga":
                pendiente += e.value_ars
                impagas.append((e.name, e.value_ars))
            else:
                egresos += e.value_ars
                for c in e.categories or []:
                    por_cat[c] = por_cat.get(c, 0) + e.value_ars
        return {"ingresos": ingresos, "egresos": egresos, "balance": ingresos - egresos, "by_category": por_cat,
                "entries": len(entradas), "pendiente": pendiente, "impagas": impagas}

    async def get_impaga_facturas(self, provider=None, estricto=False):
        out = [e for e in self._vivos() if e.estado == "Impaga"]
        if provider:
            out = [e for e in out if provider.lower() in (e.name or "").lower()]
        return out

    async def get_finance_history_by_provider(self, provider, limit=5):
        return [e for e in self._vivos() if e.estado != "Impaga"
                and (provider or "").lower() in (e.name or "").lower()][:limit]

    async def mark_finance_paid(self, page_id, paid_amount=None, payment_method=None, notes=None):
        e = await self.get_expense(page_id)
        if not e:
            return False
        self.m.cambios.append((e.id, {"estado": "Pagada", "value_ars": paid_amount}))
        e.estado = "Pagada"
        if paid_amount is not None:
            e.value_ars = float(paid_amount)
        if payment_method:
            e.method = str(payment_method)
        if notes:
            e.notes = ((e.notes or "") + "\n" + notes).strip()
        return True

    async def create_finance_invoice(self, provider, amount, period, due_date="", category="Recurrente", **k):
        e = await self.create_expense({"name": f"Factura {provider} — {period}", "in_out": EGRESO,
                                       "value_ars": amount or 0, "categories": [category],
                                       "date": str(self.m.hoy), "estado": "Impaga"})
        return True, e.id

    async def _update_page(self, page_id, props):
        return {}

    async def get_pending_factura_tasks(self):
        return []

    async def mark_factura_task_paid(self, page_id):
        return True

    async def create_factura_task(self, *a, **k):
        return True, self._id("tarea")

    # ── Tareas ───────────────────────────────────────────────────────────────
    async def get_pending_tasks(self, category=None):
        return [t for t in self.m.tareas if t.status != "Listo"]

    async def create_task(self, data: dict):
        t = N(id=self._id("tarea"), name=data.get("name", ""), status=data.get("status", "Sin empezar"),
              priority=data.get("priority"), due_date=data.get("due_date"), notes=data.get("notes"),
              category=data.get("category"), source=data.get("source"))
        self.m.tareas.append(t)
        self.m.tareas_creadas.append(t)
        return t

    async def update_task(self, task_id, updates: dict):
        t = next(x for x in self.m.tareas if x.id == task_id)
        for k, v in updates.items():
            setattr(t, k, v)
        return t

    # ── Compras ──────────────────────────────────────────────────────────────
    async def get_shopping_list(self, only_missing=True):
        return [c for c in self.m.compras if c.id not in self.m.archivados and not (only_missing and c.in_stock)]

    async def search_shopping_item(self, name):
        n = (name or "").strip().lower()
        candidatos = [n] + ([n[:-1]] if n.endswith("s") and len(n) > 3 else [])
        for c in candidatos:
            hallados = [x for x in self.m.compras if x.id not in self.m.archivados and c in x.name.lower()]
            if hallados:
                return hallados
        return []

    async def add_shopping_item(self, data: dict):
        c = N(id=self._id("compra"), name=(data.get("name") or "").strip(), in_stock=False,
              category=data.get("category"), stores=[data["store"]] if data.get("store") else [],
              frequency=data.get("frequency"), emoji=data.get("emoji", ""), notes=data.get("notes"))
        self.m.compras.append(c)
        self.m.compras_agregadas.append(c)
        return c

    async def update_shopping_item(self, item_id, updates: dict):
        c = next(x for x in self.m.compras if x.id == item_id)
        for k, v in updates.items():
            setattr(c, k, v)
        self.m.cambios.append((item_id, dict(updates)))
        return c

    async def archive_shopping_item(self, item_id):
        self.m.archivados.add(item_id)
        return True

    # ── Plantas, reuniones, proyectos ────────────────────────────────────────
    async def query_plants(self):
        return self.m.plantas

    async def search_plants(self, query):
        return [p for p in self.m.plantas if (query or "").lower() in p.name.lower()]

    async def update_plant(self, plant_id, updates: dict):
        p = next(x for x in self.m.plantas if x.id == plant_id)
        for k, v in updates.items():
            setattr(p, k, v)
        self.m.cambios.append((plant_id, dict(updates)))
        return p

    async def query_meetings(self, limit=20):
        return []

    async def search_meetings(self, query, limit=5):
        return []

    async def query_projects(self, area=None):
        return []


class Mundo:
    """Los datos de partida de un caso y el registro de todo lo que Knot hizo."""

    def __init__(self, hoy: date, datos: dict = None):
        datos = datos or {}
        self.hoy = hoy
        mes = hoy.strftime("%Y-%m")
        self.movimientos = [N(id=d["id"], name=d["name"], value_ars=float(d["value_ars"]),
                              in_out=d.get("in_out", "EGRESO"), categories=list(d.get("categories") or []),
                              method=d.get("method", ""), date=d.get("date") or f"{mes}-01",
                              time=None, client=[], emoji="", notes=d.get("notes"), liters=None,
                              estado=d.get("estado", "Pagada"))
                            for d in datos.get("movimientos", [])]
        self.tareas = [N(id=d["id"], name=d["name"], status=d.get("status", "Sin empezar"), priority=None,
                         due_date=d.get("due_date"), notes=None, category="Agenda", source="Knot")
                       for d in datos.get("tareas", [])]
        self.compras = [N(id=d["id"], name=d["name"], in_stock=False, category=d.get("category"), stores=[],
                          frequency=None, emoji="", notes=None) for d in datos.get("compras", [])]
        self.plantas = [N(id=d["id"], name=d["name"], species=None, light=None, watering=d.get("watering"),
                          location=d.get("location"), status="Bien", purchase_date=None, price=None, notes=None,
                          emoji="", last_watering=d.get("last_watering")) for d in datos.get("plantas", [])]
        self.medios = [N(id=d["id"], name=d["name"], modality=d.get("modality", ""), bank=d.get("bank"),
                         last4=d.get("last4"), owner=None, is_default=d.get("is_default", False), uses=0)
                       for d in datos.get("medios", [])]
        self.servicios = list(datos.get("servicios", []))
        self.agenda = datos.get("agenda") or "No hay eventos en ese periodo."
        self.personas = dict(datos.get("personas") or {})
        # Registro de lo que pasó
        self.enviados: list[str] = []          # todo lo que Knot le dijo al usuario
        self.botones: list[dict] = []          # preguntas con botones
        self.rutas: list = []                  # módulo elegido para cada mensaje
        self.creados: list = []                # movimientos nuevos
        self.cambios: list = []                # (id, campos) de lo modificado
        self.archivados: set = set()           # ids borrados
        self.tareas_creadas: list = []
        self.compras_agregadas: list = []
        self.recordatorios: list[dict] = []
        self.eventos: list[dict] = []
        self.no_previstas: list[str] = []      # llamadas a la base que el mundo falso no implementa
        self.ds = DatosFalsos(self)

    def dicho(self) -> str:
        return "\n".join(self.enviados + [b["texto"] for b in self.botones])


def base(hoy: date) -> dict:
    """Datos de partida comunes: lo mínimo para que Knot tenga con qué trabajar."""
    mes = hoy.strftime("%Y-%m")
    mes_ant = (hoy.replace(day=1) - timedelta(days=1))
    meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre",
             "Octubre", "Noviembre", "Diciembre"]
    vence = hoy + timedelta(days=5)
    return {
        "movimientos": [
            {"id": "sup1", "name": "Supermercado La Anónima", "value_ars": 45000, "categories": ["Supermercado"],
             "date": f"{mes}-02"},
            {"id": "sueldo", "name": "Sueldo", "value_ars": 900000, "in_out": "INGRESO", "categories": ["Sueldo"],
             "date": f"{mes}-01"},
            {"id": "camuzzi", "name": f"Factura Camuzzi — {meses[hoy.month - 1]} {hoy.year}", "value_ars": 30000,
             "categories": ["Recurrente"], "estado": "Impaga", "date": f"{mes}-01",
             "notes": f"vence {vence.strftime('%d/%m/%Y')}"},
            {"id": "movistar", "name": f"Factura Movistar — {meses[mes_ant.month - 1]} {mes_ant.year}",
             "value_ars": 29791.8, "categories": ["Recurrente"], "estado": "Impaga",
             "date": mes_ant.strftime("%Y-%m-20")},
        ],
        "tareas": [{"id": "t_plomero", "name": "Llamar al plomero"},
                   {"id": "t_carnet", "name": "Renovar el carnet", "due_date": str(hoy)}],
        "compras": [{"id": "c_leche", "name": "Leche", "category": "Lacteo"},
                    {"id": "c_yerba", "name": "Yerba", "category": "Infusion"}],
        "plantas": [{"id": "p_potus", "name": "Potus", "location": "Interior", "watering": "Semanal",
                     "last_watering": str(hoy - timedelta(days=4))}],
        "medios": [{"id": "pm_efectivo", "name": "Efectivo", "modality": "Cash"},
                   {"id": "pm_bbva", "name": "BBVA Débito", "modality": "Debit", "bank": "BBVA", "is_default": True},
                   {"id": "pm_mp", "name": "Mercado Pago", "modality": "QR"}],
        "servicios": [
            {"servicio": "Luz", "empresa": "CALF", "aliases": ["calf", "electricidad"], "categoria": "Recurrente"},
            {"servicio": "Gas", "empresa": "Camuzzi", "aliases": ["camuzzi"], "categoria": "Recurrente"},
            {"servicio": "Monotributo", "empresa": "ARCA", "aliases": ["arca", "afip"], "categoria": "Recurrente"},
            {"servicio": "Expensas", "empresa": "Interfast", "aliases": ["interfast"], "categoria": "Recurrente"},
            {"servicio": "Teléfono", "empresa": "Movistar", "aliases": ["movistar"], "categoria": "Recurrente"},
        ],
        "personas": {"Fabián": {"relacion": "papá", "le_dice": ["mi viejo", "papá"]}},
        "agenda": "\n".join([
            f"- {(hoy + timedelta(days=1)).strftime('%d/%m')} 18:00 -- Funcional",
            f"- {(hoy + timedelta(days=3)).strftime('%d/%m')} 10:00 -- Turno dentista",
        ]),
    }
