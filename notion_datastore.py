"""
notion_datastore.py

Concrete DataStore implementation for Notion.
Translates the ~35 direct httpx calls currently scattered across main.py
into an organized, typed interface.

Usage:
    ds = NotionDataStore(token="secret_xxx", db_ids={
        "finances": "4ed34d2a...",
        "shopping": "cb85fdf7...",
        ...
    })
    entry = await ds.create_expense({...})

Gradual migration: import this file in main.py and replace functions
one at a time without breaking anything.
"""

import json
import httpx
from datetime import date, datetime, timedelta
from calendar import monthrange
from dataclasses import dataclass

# Import types from base.py if the matrics package is available;
# otherwise fall back to inline definitions so this file works standalone.
try:
    from matrics.datastore.base import (
        DataStore, DataStoreError, QueryFilter, DateRange,
        EntryResult, ShoppingItem, PlantEntry, MeetingEntry,
        TaskEntry, ProjectEntry, RecipeEntry, GeoReminder, UserConfig,
    )
except ImportError:
    # Standalone fallback: no matrics package required.
    DataStore = object
    DataStoreError = Exception

    @dataclass
    class QueryFilter:
        date_range: object = None
        name_contains: str = None
        category: str = None
        tags: list = None
        limit: int = 50

    @dataclass
    class DateRange:
        start: date = None
        end: date = None

    @dataclass
    class EntryResult:
        id: str = ""
        name: str = ""
        value_ars: float = 0
        in_out: str = ""
        categories: list = None
        method: str = ""
        date: date = None
        time: str = None
        client: list = None
        emoji: str = ""
        notes: str = None
        liters: float = None
        estado: str = None

    @dataclass
    class ShoppingItem:
        id: str = ""
        name: str = ""
        in_stock: bool = False
        category: str = None
        stores: list = None
        frequency: str = None
        emoji: str = ""
        notes: str = None

    @dataclass
    class PlantEntry:
        id: str = ""
        name: str = ""
        species: str = None
        light: str = None
        watering: str = None
        location: str = None
        status: str = None
        purchase_date: date = None
        price: float = None
        notes: str = None
        emoji: str = ""
        last_watering: date = None

    @dataclass
    class MeetingEntry:
        id: str = ""
        name: str = ""
        with_whom: str = None
        date: date = None
        notes: str = None
        calendar_link: str = None

    @dataclass
    class TaskEntry:
        id: str = ""
        name: str = ""
        category: str = None
        status: str = ""
        priority: str = None
        due_date: date = None
        source: str = None
        notes: str = None

    @dataclass
    class ProjectEntry:
        id: str = ""
        name: str = ""
        entry_type: str = ""
        area: str = ""
        status: str = ""
        priority: str = None
        description: str = None
        emoji: str = ""

    @dataclass
    class RecipeEntry:
        id: str = ""
        name: str = ""
        source: str = None
        difficulty: str = None
        recipe_type: list = None
        cooking_method: str = None
        healthy: str = None
        ingredient_ids: list = None

    @dataclass
    class GeoReminder:
        id: str = ""
        name: str = ""
        reminder_type: str = ""
        shop_name: str = None
        lat: float = None
        lon: float = None
        radius: int = 300
        recurrent: bool = False
        active: bool = True

    @dataclass
    class UserConfig:
        phone: str = ""
        greeting_name: str = None
        daily_summary_hour: int = None
        daily_summary_minute: int = None
        resumen_nocturno_enabled: bool = True
        resumen_nocturno_hour: int = 22
        resumen_semanal_enabled: bool = True
        resumen_semanal_hour: int = 21
        resumen_extras: list = None
        news_topics: list = None
        service_providers: dict = None
        known_places: list = None
        activities: dict = None
        domain_profiles: dict = None
        purchase_counts: dict = None
        saved_lat: float = None
        saved_lon: float = None
        saved_city: str = None
        last_summary_date: str = None
        known_shops: dict = None    # {"la anonima": "Supermercado", "farmacity": "Farmacia"}
        feature_hints: dict = None  # {trigger_id: {first_suggested_at, accepted, dismissed_count, disabled}}
        generative_lists: dict = None  # {name: db_id}
        pending_invoice_confirmations: list = None  # [{id, situation, provider, ...}]

    @dataclass
    class PaymentMethod:
        id: str = ""
        name: str = ""
        modality: str = ""   # Cash, Transfer, Debit, Credit, QR
        bank: str = None
        last4: str = None
        owner: str = None
        is_default: bool = False
        uses: int = 0


# ── Domain constants ───────────────────────────────────────────────────────────

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

# Exact Notion select values — must match the DB exactly, never change.
INGRESO_EXACT = "\u2192INGRESO\u2190"
EGRESO_EXACT = "\u2190 EGRESO \u2192"

# Exact Notion multi_select option names for Shopping.
SHOPPING_CATEGORIES = [
    "Frutas y verduras", "Enlatado", "Infusion", "Lacteo", "Especias",
    "Limpieza", "Panificado", "Herramienta", "Construccion", "Higiene",
    "Electronica", "Carne", "Galletitas", "Alcohol", "Bebida", "Fiambre",
    "Grano", "Comida", "Cosmetica",
]
SHOPPING_FREQUENCY = ["Often", "Monthly", "Annual", "One time"]

# Words too generic to use as fuzzy search keys.
GENERIC_WORDS = {
    "salsa", "crema", "pasta", "sopa", "caldo", "jugo",
    "queso", "pan", "leche", "aceite", "harina", "arroz",
}


# ── Notion property extraction helpers ────────────────────────────────────────

def _get_title(props: dict, field: str = "Name") -> str:
    """Extract text from a title field."""
    titles = props.get(field, {}).get("title", [])
    return titles[0]["plain_text"] if titles else ""


def _get_text(props: dict, field: str) -> str:
    """Extract text from a rich_text field."""
    rt = props.get(field, {}).get("rich_text", [])
    return rt[0]["plain_text"] if rt else ""


def _get_number(props: dict, field: str) -> float | None:
    """Extract a number. Returns None if the field is empty."""
    return props.get(field, {}).get("number")


def _get_checkbox(props: dict, field: str) -> bool:
    return props.get(field, {}).get("checkbox", False)


def _get_select(props: dict, field: str) -> str:
    """Extract the name from a select field. Returns '' if empty."""
    sel = props.get(field, {}).get("select")
    return sel["name"] if sel else ""


def _get_multi_select(props: dict, field: str) -> list[str]:
    """Extract list of names from a multi_select field."""
    return [c["name"] for c in props.get(field, {}).get("multi_select", [])]


def _load_json_list(props: dict, field: str) -> list | None:
    """Extract a JSON list from a rich_text field. Returns None if empty or invalid."""
    raw = _get_text(props, field)
    if not raw:
        return None
    try:
        result = json.loads(raw)
        return result if isinstance(result, list) else None
    except Exception:
        return None


def _get_status(props: dict, field: str) -> str:
    """Extract the name from a status field."""
    st = props.get(field, {}).get("status")
    return st["name"] if st else ""


def _get_date(props: dict, field: str) -> str:
    """Extract the start date as a YYYY-MM-DD string."""
    d = props.get(field, {}).get("date")
    return d["start"][:10] if d and d.get("start") else ""


def _get_url(props: dict, field: str) -> str:
    return props.get(field, {}).get("url") or ""


def _get_relation_ids(props: dict, field: str) -> list[str]:
    return [r["id"] for r in props.get(field, {}).get("relation", [])]


def _clean_db_id(db_id: str) -> str:
    """Notion accepts IDs with or without dashes. We normalize for consistency."""
    return db_id.replace("-", "")


def _normalize_in_out(raw: str) -> str:
    """Enforce the exact In-Out value so Notion formulas don't break."""
    if not raw:
        return EGRESO_EXACT
    if "INGRESO" in raw.upper():
        return INGRESO_EXACT
    return EGRESO_EXACT


# ══════════════════════════════════════════════════════════════════════════════
# Collections — templates y helpers
# ══════════════════════════════════════════════════════════════════════════════

COLLECTION_TEMPLATES: dict[str, dict] = {
    "pelis": {
        "aliases":       {"pelis", "películas", "peliculas", "peli", "movies", "cine", "film", "films"},
        "icon":          "🎬",
        "display":       "Películas",
        "default_estado":"Pendiente",
        "schema": {
            "Name":       {"title": {}},
            "Director":   {"rich_text": {}},
            "Año":        {"number": {"format": "number"}},
            "Género":     {"multi_select": {"options": []}},
            "Estado":     {"select": {"options": [
                {"name": "Pendiente", "color": "gray"},
                {"name": "Viendo",    "color": "yellow"},
                {"name": "Visto",     "color": "green"},
            ]}},
            "Puntuación": {"number": {"format": "number"}},
            "Plataforma": {"select": {"options": [
                {"name": "Netflix",  "color": "red"},
                {"name": "Disney+",  "color": "blue"},
                {"name": "Prime",    "color": "orange"},
                {"name": "HBO",      "color": "purple"},
                {"name": "Cine",     "color": "gray"},
                {"name": "Otro",     "color": "default"},
            ]}},
            "Notas":      {"rich_text": {}},
            "Agregada":   {"date": {}},
        },
        "item_prompt": '[{"name": "título", "director": "director o null", "year": año_entero_o_null, "genre": "género o null", "platform": "plataforma o null", "notes": "sinopsis breve o null"}]',
    },
    "libros": {
        "aliases":       {"libros", "libro", "books", "book", "lectura", "leer"},
        "icon":          "📚",
        "display":       "Libros",
        "default_estado":"Pendiente",
        "schema": {
            "Name":       {"title": {}},
            "Autor":      {"rich_text": {}},
            "Año":        {"number": {"format": "number"}},
            "Género":     {"multi_select": {"options": []}},
            "Estado":     {"select": {"options": [
                {"name": "Pendiente", "color": "gray"},
                {"name": "Leyendo",   "color": "yellow"},
                {"name": "Leído",     "color": "green"},
            ]}},
            "Puntuación": {"number": {"format": "number"}},
            "Notas":      {"rich_text": {}},
            "Agregada":   {"date": {}},
        },
        "item_prompt": '[{"name": "título", "author": "autor o null", "year": año_entero_o_null, "genre": "género o null", "notes": "descripción breve o null"}]',
    },
    "lugares": {
        "aliases":       {"lugares", "lugar", "viajes", "viaje", "destinos", "places", "travel"},
        "icon":          "🗺️",
        "display":       "Lugares",
        "default_estado":"Soñado",
        "schema": {
            "Name":    {"title": {}},
            "País":    {"rich_text": {}},
            "Estado":  {"select": {"options": [
                {"name": "Soñado",   "color": "gray"},
                {"name": "Planeado", "color": "yellow"},
                {"name": "Visitado", "color": "green"},
            ]}},
            "Notas":   {"rich_text": {}},
            "Agregada":{"date": {}},
        },
        "item_prompt": '[{"name": "nombre del lugar", "country": "país o null", "notes": "descripción breve o null"}]',
    },
}

# Reverse alias map: alias → template_key
_ALIAS_TO_TEMPLATE: dict[str, str] = {
    alias: key
    for key, tmpl in COLLECTION_TEMPLATES.items()
    for alias in tmpl["aliases"]
}


def detect_collection_template(list_name: str) -> str | None:
    """Returns the template key for list_name, or None if generic."""
    return _ALIAS_TO_TEMPLATE.get(list_name.lower().strip())


def _build_collection_props(item: dict, template_key: str | None, today: str) -> dict:
    """Map a generated item dict to Notion page properties per template."""
    name = (item.get("name") or "").strip()
    if template_key == "pelis":
        props: dict = {
            "Name":     {"title": [{"text": {"content": name[:200]}}]},
            "Agregada": {"date": {"start": today}},
            "Estado":   {"select": {"name": "Pendiente"}},
        }
        if item.get("director"):
            props["Director"] = {"rich_text": [{"text": {"content": str(item["director"])[:200]}}]}
        if item.get("year") and str(item["year"]).lstrip("-").isdigit():
            props["Año"] = {"number": int(item["year"])}
        if item.get("genre"):
            props["Género"] = {"multi_select": [{"name": str(item["genre"])[:100]}]}
        if item.get("platform"):
            props["Plataforma"] = {"select": {"name": str(item["platform"])[:100]}}
        if item.get("notes"):
            props["Notas"] = {"rich_text": [{"text": {"content": str(item["notes"])[:500]}}]}
        return props
    if template_key == "libros":
        props = {
            "Name":     {"title": [{"text": {"content": name[:200]}}]},
            "Agregada": {"date": {"start": today}},
            "Estado":   {"select": {"name": "Pendiente"}},
        }
        if item.get("author"):
            props["Autor"] = {"rich_text": [{"text": {"content": str(item["author"])[:200]}}]}
        if item.get("year") and str(item["year"]).lstrip("-").isdigit():
            props["Año"] = {"number": int(item["year"])}
        if item.get("genre"):
            props["Género"] = {"multi_select": [{"name": str(item["genre"])[:100]}]}
        if item.get("notes"):
            props["Notas"] = {"rich_text": [{"text": {"content": str(item["notes"])[:500]}}]}
        return props
    if template_key == "lugares":
        props = {
            "Name":     {"title": [{"text": {"content": name[:200]}}]},
            "Agregada": {"date": {"start": today}},
            "Estado":   {"select": {"name": "Soñado"}},
        }
        if item.get("country"):
            props["País"] = {"rich_text": [{"text": {"content": str(item["country"])[:200]}}]}
        if item.get("notes"):
            props["Notas"] = {"rich_text": [{"text": {"content": str(item["notes"])[:500]}}]}
        return props
    # Generic
    props = {
        "Name":  {"title": [{"text": {"content": name[:200]}}]},
        "Added": {"date": {"start": today}},
    }
    notes = (item.get("notes") or "").strip()
    if notes:
        props["Notes"] = {"rich_text": [{"text": {"content": notes[:500]}}]}
    tags = item.get("tags") or []
    if tags:
        props["Tags"] = {"multi_select": [{"name": str(t)[:100]} for t in tags[:5]]}
    return props


# ══════════════════════════════════════════════════════════════════════════════
# NotionDataStore
# ══════════════════════════════════════════════════════════════════════════════

class NotionDataStore:
    """
    DataStore implementation for Notion.

    Accepts a token and a dict of database IDs:
    {
        "finances":      "4ed34d2a...",
        "shopping":      "cb85fdf7...",
        "recipes":       "8fa008a7...",
        "plants":        "39d22615...",
        "meetings":      "ed5b5023...",
        "tasks":         "90b44158...",
        "config":        "2f81017d...",
        "geo_reminders": "5fe7a531...",
        "projects":      "0924aff7...",
    }
    """

    def __init__(self, token: str, db_ids: dict):
        self._db_ids = {k: v.replace("-", "") for k, v in db_ids.items()}
        self._headers_cache = {
            "Authorization": f"Bearer {token}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json",
        }
        self._http = httpx.AsyncClient(timeout=15)

    async def aclose(self):
        await self._http.aclose()

    # ── Internal HTTP helpers ──────────────────────────────────────────────

    def _headers(self) -> dict:
        return self._headers_cache

    def _db(self, name: str) -> str:
        """Return the clean database ID. Raises an error if not configured."""
        db_id = self._db_ids.get(name)
        if not db_id:
            raise DataStoreError(f"Database '{name}' not configured in db_ids")
        return db_id

    async def _query_db(
        self,
        db_name: str,
        filter_obj: dict = None,
        sorts: list = None,
        page_size: int = 100,
    ) -> list[dict]:
        """Generic database query. Returns a list of pages."""
        body = {"page_size": page_size}
        if filter_obj:
            body["filter"] = filter_obj
        if sorts:
            body["sorts"] = sorts
        r = await self._http.post(
            f"{NOTION_API}/databases/{self._db(db_name)}/query",
            headers=self._headers_cache,
            json=body,
        )
        if r.status_code != 200:
            raise DataStoreError(f"Query {db_name} failed ({r.status_code}): {r.text[:200]}")
        return r.json().get("results", [])

    async def _create_page(
        self,
        db_name: str,
        props: dict,
        emoji: str = None,
    ) -> dict:
        """Create a page in a database. Returns the created page."""
        body = {
            "parent": {"database_id": self._db(db_name)},
            "properties": props,
        }
        if emoji:
            body["icon"] = {"type": "emoji", "emoji": emoji}
        r = await self._http.post(
            f"{NOTION_API}/pages",
            headers=self._headers_cache,
            json=body,
        )
        if r.status_code not in (200, 201):
            raise DataStoreError(f"Create in {db_name} failed ({r.status_code}): {r.text[:200]}")
        return r.json()

    async def _update_page(self, page_id: str, props: dict) -> dict:
        """Update page properties. Returns the updated page."""
        r = await self._http.patch(
            f"{NOTION_API}/pages/{page_id}",
            headers=self._headers_cache,
            json={"properties": props},
        )
        if r.status_code != 200:
            raise DataStoreError(f"Update {page_id} failed ({r.status_code}): {r.text[:200]}")
        return r.json()

    async def _archive_page(self, page_id: str) -> bool:
        """Archive (soft-delete) a page."""
        r = await self._http.patch(
            f"{NOTION_API}/pages/{page_id}",
            headers=self._headers_cache,
            json={"archived": True},
        )
        return r.status_code == 200

    async def _get_workspace_parent(self) -> str | None:
        """Find a page_id we can use as parent for new databases.
        Walks up from any configured DB until it hits a workspace-rooted page."""
        for db_name in ("config", "tasks", "shopping", "finances"):
            db_id = self._db_ids.get(db_name)
            if not db_id:
                continue
            r = await self._http.get(f"{NOTION_API}/databases/{db_id}", headers=self._headers_cache)
            if r.status_code != 200:
                continue
            parent = r.json().get("parent", {})
            if parent.get("type") == "page_id":
                return parent["page_id"]
        return None

    async def create_generative_list_db(self, list_name: str, template_key: str | None = None) -> str | None:
        """Create a new Notion database to back a collection. Uses rich schema for known templates."""
        parent_page_id = await self._get_workspace_parent()
        if not parent_page_id:
            return None
        tmpl = COLLECTION_TEMPLATES.get(template_key) if template_key else None
        icon  = tmpl["icon"]    if tmpl else "📋"
        title = tmpl["display"] if tmpl else list_name
        props = tmpl["schema"]  if tmpl else {
            "Name":  {"title": {}},
            "Notes": {"rich_text": {}},
            "Added": {"date": {}},
            "Tags":  {"multi_select": {"options": []}},
            "Done":  {"checkbox": {}},
        }
        body = {
            "parent": {"type": "page_id", "page_id": parent_page_id},
            "title":  [{"type": "text", "text": {"content": title}}],
            "icon":   {"type": "emoji", "emoji": icon},
            "properties": props,
        }
        r = await self._http.post(f"{NOTION_API}/databases", headers=self._headers_cache, json=body)
        if r.status_code in (200, 201):
            return r.json().get("id")
        return None

    async def add_items_to_list_db(self, db_id: str, items: list[dict], template_key: str | None = None) -> int:
        """items: dicts with fields per template. Returns count inserted."""
        from datetime import datetime as _dt, timezone as _tz, timedelta as _td
        today = (_dt.now(_tz.utc) - _td(hours=3)).date().isoformat()
        count = 0
        for item in items:
            name = (item.get("name") or "").strip()
            if not name:
                continue
            props = _build_collection_props(item, template_key, today)
            r = await self._http.post(
                f"{NOTION_API}/pages",
                headers=self._headers_cache,
                json={"parent": {"database_id": db_id}, "properties": props},
            )
            if r.status_code in (200, 201):
                count += 1
        return count

    async def _append_blocks(self, page_id: str, blocks: list[dict]) -> bool:
        """Append content blocks to a page (used for recipes, etc.)."""
        r = await self._http.patch(
            f"{NOTION_API}/blocks/{page_id}/children",
            headers=self._headers_cache,
            json={"children": blocks[:100]},
        )
        return r.status_code == 200

    # ══════════════════════════════════════════════════════════════════════
    # FINANCES
    # ══════════════════════════════════════════════════════════════════════

    def _parse_expense(self, page: dict) -> EntryResult:
        """Convert a Notion page to an EntryResult."""
        props = page.get("properties", {})
        in_out_raw = _get_select(props, "In - Out")
        return EntryResult(
            id=page["id"],
            name=_get_title(props),
            value_ars=_get_number(props, "Value (ars)") or 0,
            in_out="INGRESO" if "INGRESO" in in_out_raw else "EGRESO",
            categories=_get_multi_select(props, "Category"),
            method=_get_select(props, "Method"),
            date=_get_date(props, "Date"),
            time=None,
            client=_get_multi_select(props, "Client"),
            emoji=page.get("icon", {}).get("emoji", ""),
            notes=_get_text(props, "Notes") or None,
            liters=_get_number(props, "Liters"),
            estado=_get_select(props, "Estado") or None,
        )

    async def create_expense(self, data: dict) -> EntryResult:
        """
        Create an expense or income entry.
        data keys: name, in_out, value_ars, exchange_rate, categories,
                    method, date, time, client, liters, consumo_kwh,
                    notes, emoji
        """
        normalized_in_out = _normalize_in_out(data.get("in_out", ""))
        props = {
            "Name": {"title": [{"text": {"content": data["name"]}}]},
            "In - Out": {"select": {"name": normalized_in_out}},
            "Value (ars)": {"number": float(data["value_ars"])},
            "Exchange Rate": {"number": data.get("exchange_rate", 0)},
        }
        if data.get("categories"):
            props["Category"] = {"multi_select": [{"name": c} for c in data["categories"]]}
        if data.get("date"):
            if data.get("time"):
                props["Date"] = {"date": {
                    "start": f"{data['date']}T{data['time']}:00",
                    "time_zone": "America/Argentina/Buenos_Aires",
                }}
            else:
                props["Date"] = {"date": {"start": data["date"]}}
        if data.get("client"):
            props["Client"] = {"multi_select": [{"name": c} for c in data["client"]]}
        if data.get("liters") is not None:
            props["Liters"] = {"number": float(data["liters"])}
        if data.get("consumo_kwh") is not None:
            props["Consumption (kWh)"] = {"number": float(data["consumo_kwh"])}
        if data.get("notes"):
            props["Notes"] = {"rich_text": [{"text": {"content": data["notes"]}}]}
        if data.get("estado"):
            props["Estado"] = {"select": {"name": data["estado"]}}
        if data.get("payment_method_id"):
            props["Method"] = {"relation": [{"id": data["payment_method_id"]}]}

        emoji = data.get("emoji") or "\U0001f4b8"
        page = await self._create_page("finances", props, emoji=emoji)
        return self._parse_expense(page)

    async def query_expenses(self, filters: QueryFilter = None) -> list[EntryResult]:
        """Query expenses/income by filters."""
        notion_filter = {"and": []}

        if filters and filters.date_range:
            notion_filter["and"].append(
                {"property": "Date", "date": {"on_or_after": str(filters.date_range.start)}}
            )
            notion_filter["and"].append(
                {"property": "Date", "date": {"on_or_before": str(filters.date_range.end)}}
            )
        if filters and filters.name_contains:
            notion_filter["and"].append(
                {"property": "Name", "title": {"contains": filters.name_contains[:30]}}
            )
        if filters and filters.category:
            notion_filter["and"].append(
                {"property": "Category", "multi_select": {"contains": filters.category}}
            )

        # No filters: query without filter object
        filter_obj = notion_filter if notion_filter["and"] else None
        limit = filters.limit if filters else 50

        pages = await self._query_db(
            "finances",
            filter_obj=filter_obj,
            sorts=[{"property": "Date", "direction": "descending"}],
            page_size=limit,
        )
        return [self._parse_expense(p) for p in pages]

    async def update_expense(self, entry_id: str, updates: dict) -> EntryResult:
        """
        Update fields of an expense entry.
        Supported update keys: value_ars, categories, name, method, notes, liters
        """
        props = {}
        if "value_ars" in updates:
            props["Value (ars)"] = {"number": float(updates["value_ars"])}
        if "categories" in updates:
            props["Category"] = {"multi_select": [{"name": c} for c in updates["categories"]]}
        if "name" in updates:
            props["Name"] = {"title": [{"text": {"content": updates["name"]}}]}
        if "payment_method_id" in updates:
            props["Method"] = {"relation": [{"id": updates["payment_method_id"]}]}
        emoji_update = updates.get("emoji")
        if "notes" in updates:
            props["Notes"] = {"rich_text": [{"text": {"content": updates["notes"]}}]}
        if "liters" in updates:
            props["Liters"] = {"number": float(updates["liters"])}

        page = await self._update_page(entry_id, props)
        if emoji_update:
            try:
                await self._http.patch(
                    f"{NOTION_API}/pages/{entry_id}",
                    headers=self._headers_cache,
                    json={"icon": {"type": "emoji", "emoji": emoji_update}},
                )
            except Exception:
                pass
        return self._parse_expense(page)

    async def archive_expense(self, entry_id: str) -> bool:
        return await self._archive_page(entry_id)

    async def increment_payment_method_uses(self, page_id: str, current_uses: int) -> None:
        """Increment the Uses counter for a payment method."""
        try:
            await self._update_page(page_id, {"Uses": {"number": current_uses + 1}})
        except Exception:
            pass

    async def migrate_empty_categories_to_recurrente(self) -> int:
        """One-time migration: set Category=Recurrente on all Finance records with empty Category."""
        updated = 0
        cursor = None
        while True:
            body = {
                "page_size": 100,
                "filter": {"property": "Category", "multi_select": {"is_empty": True}},
            }
            if cursor:
                body["start_cursor"] = cursor
            r = await self._http.post(
                f"{NOTION_API}/databases/{self._db('finances')}/query",
                headers=self._headers_cache,
                json=body,
            )
            if r.status_code != 200:
                break
            data = r.json()
            for page in data.get("results", []):
                page_id = page["id"]
                await self._update_page(page_id, {
                    "Category": {"multi_select": [{"name": "Recurrente"}]}
                })
                updated += 1
            if not data.get("has_more"):
                break
            cursor = data.get("next_cursor")
        return updated

    async def get_financial_summary(self, month: str = None) -> dict:
        """
        Financial summary for a month (format YYYY-MM).
        Returns: {"ingresos": float, "egresos": float, "balance": float,
                  "by_category": {cat: amount}, "entries": int}
        """
        if not month:
            from datetime import timezone
            now = datetime.now(timezone.utc) - timedelta(hours=3)
            month = now.strftime("%Y-%m")

        year, mon = map(int, month.split("-"))
        last_day = monthrange(year, mon)[1]

        entries = await self.query_expenses(QueryFilter(
            date_range=DateRange(
                start=date(year, mon, 1),
                end=date(year, mon, last_day),
            ),
            limit=100,
        ))

        ingresos = egresos = 0
        by_category = {}
        for e in entries:
            if e.in_out == "INGRESO":
                ingresos += e.value_ars
            else:
                egresos += e.value_ars
                for cat in (e.categories or []):
                    by_category[cat] = by_category.get(cat, 0) + e.value_ars

        return {
            "ingresos": ingresos,
            "egresos": egresos,
            "balance": ingresos - egresos,
            "by_category": by_category,
            "entries": len(entries),
        }

    async def search_expenses(self, query: str, month: str = None) -> list[EntryResult]:
        """Search expenses by name within a month. Replaces buscar_gastos()."""
        if not month:
            from datetime import timezone
            now = datetime.now(timezone.utc) - timedelta(hours=3)
            month = now.strftime("%Y-%m")

        year, mon = map(int, month.split("-"))
        last_day = monthrange(year, mon)[1]

        return await self.query_expenses(QueryFilter(
            date_range=DateRange(start=date(year, mon, 1), end=date(year, mon, last_day)),
            name_contains=query,
            limit=10,
        ))

    async def get_services_summary(self, month: str = None) -> list[EntryResult]:
        """Expense entries in the 'Recurrente' category for the month. Replaces query_servicios_mes()."""
        if not month:
            from datetime import timezone
            now = datetime.now(timezone.utc) - timedelta(hours=3)
            month = now.strftime("%Y-%m")

        year, mon = map(int, month.split("-"))
        last_day = monthrange(year, mon)[1]

        return await self.query_expenses(QueryFilter(
            date_range=DateRange(start=date(year, mon, 1), end=date(year, mon, last_day)),
            category="Recurrente",
            limit=30,
        ))

    async def find_category_from_history(
        self, name: str, predicted_cats: list[str]
    ) -> tuple[list[str], bool]:
        """
        Search previous entries to find which category was used for a similar expense.
        Returns (categories, changed) where changed=True if a different category was found.
        Replaces check_and_apply_category().
        """
        search_key = " ".join(name.split()[:3])
        try:
            results = await self.query_expenses(QueryFilter(
                name_contains=search_key, limit=3
            ))
            if results and results[0].categories and results[0].categories != predicted_cats:
                return results[0].categories, True
        except Exception:
            pass
        return predicted_cats, False

    # ══════════════════════════════════════════════════════════════════════
    # SHOPPING
    # ══════════════════════════════════════════════════════════════════════

    def _parse_shopping(self, page: dict) -> ShoppingItem:
        props = page.get("properties", {})
        return ShoppingItem(
            id=page["id"],
            name=_get_title(props),
            in_stock=_get_checkbox(props, "Stock"),
            category=_get_select(props, "Category") or None,
            stores=_get_multi_select(props, "Store"),
            frequency=_get_status(props, "Frequency") or None,
            emoji=page.get("icon", {}).get("emoji", ""),
            notes=_get_text(props, "Notes") or None,
        )

    async def get_shopping_list(self, only_missing: bool = True) -> list[ShoppingItem]:
        """Shopping list. If only_missing=True, returns only out-of-stock items."""
        filter_obj = {"property": "Stock", "checkbox": {"equals": False}} if only_missing else None
        pages = await self._query_db(
            "shopping",
            filter_obj=filter_obj,
            sorts=[{"property": "Category", "direction": "ascending"}],
            page_size=50,
        )
        return [self._parse_shopping(p) for p in pages]

    async def search_shopping_item(self, name: str) -> list[ShoppingItem]:
        """
        Search items by name (fuzzy: tries singular form and first word).
        Mirrors the current search_shopping_item() logic in main.py.
        """
        name = name.strip()
        candidates = [name]
        if name.endswith("s") and len(name) > 3:
            candidates.append(name[:-1])
        first_word = name.split()[0].lower()
        if len(name.split()) > 1 and len(first_word) > 5 and first_word not in GENERIC_WORDS:
            candidates.append(name.split()[0])

        for candidate in candidates:
            pages = await self._query_db(
                "shopping",
                filter_obj={"property": "Name", "title": {"contains": candidate[:25]}},
            )
            if pages:
                return [self._parse_shopping(p) for p in pages]
        return []

    async def search_shopping_item_raw(self, name: str) -> list[dict]:
        """
        Same as search_shopping_item but returns raw Notion pages.
        For compatibility with the current main.py during migration.
        """
        name = name.strip()
        candidates = [name]
        if name.endswith("s") and len(name) > 3:
            candidates.append(name[:-1])
        first_word = name.split()[0].lower()
        if len(name.split()) > 1 and len(first_word) > 5 and first_word not in GENERIC_WORDS:
            candidates.append(name.split()[0])

        for candidate in candidates:
            pages = await self._query_db(
                "shopping",
                filter_obj={"property": "Name", "title": {"contains": candidate[:25]}},
            )
            if pages:
                return pages
        return []

    async def add_shopping_item(self, data: dict) -> ShoppingItem:
        """
        Add an item to the shopping list.
        data keys: name, emoji, category, store, frequency, notes
        """
        props = {
            "Name": {"title": [{"text": {"content": data.get("name", "").strip()}}]},
            "Stock": {"checkbox": False},
        }
        if data.get("category") in SHOPPING_CATEGORIES:
            props["Category"] = {"select": {"name": data["category"]}}
        if data.get("store"):
            props["Store"] = {"multi_select": [{"name": data["store"]}]}
        if data.get("frequency") in SHOPPING_FREQUENCY:
            props["Frequency"] = {"status": {"name": data["frequency"]}}
        if data.get("notes"):
            props["Notes"] = {"rich_text": [{"text": {"content": data["notes"]}}]}

        emoji = data.get("emoji", "\U0001f6d2")
        page = await self._create_page("shopping", props, emoji=emoji)
        return self._parse_shopping(page)

    async def update_shopping_item(self, item_id: str, updates: dict) -> ShoppingItem:
        """Update a shopping item. Supported update keys: in_stock, name, category, notes"""
        props = {}
        if "in_stock" in updates:
            props["Stock"] = {"checkbox": updates["in_stock"]}
        if "name" in updates:
            props["Name"] = {"title": [{"text": {"content": updates["name"]}}]}
        if "category" in updates:
            props["Category"] = {"select": {"name": updates["category"]}}
        if "notes" in updates:
            props["Notes"] = {"rich_text": [{"text": {"content": updates["notes"]}}]}
        if "frequency" in updates:
            props["Frequency"] = {"status": {"name": updates["frequency"]}}

        page = await self._update_page(item_id, props)
        return self._parse_shopping(page)

    async def archive_shopping_item(self, item_id: str) -> bool:
        return await self._archive_page(item_id)

    async def bulk_update_shopping_stock(self, in_stock: bool) -> int:
        """
        Mark ALL pending items as in-stock (or vice versa).
        Uses asyncio.gather to avoid Railway timeouts on bulk operations.
        Returns the number of items updated.
        """
        import asyncio
        pages = await self._query_db(
            "shopping",
            filter_obj={"property": "Stock", "checkbox": {"equals": not in_stock}},
            page_size=50,
        )
        if not pages:
            return 0

        async def _patch_one(page_id):
            try:
                await self._update_page(page_id, {"Stock": {"checkbox": in_stock}})
            except Exception:
                pass

        await asyncio.gather(*[_patch_one(p["id"]) for p in pages])
        return len(pages)

    # ══════════════════════════════════════════════════════════════════════
    # RECIPES
    # ══════════════════════════════════════════════════════════════════════

    def _parse_recipe(self, page: dict) -> RecipeEntry:
        props = page.get("properties", {})
        return RecipeEntry(
            id=page["id"],
            name=_get_title(props),
            source=_get_select(props, "Source") or None,
            difficulty=_get_select(props, "Difficult ") or None,
            recipe_type=_get_multi_select(props, "Type"),
            cooking_method=_get_select(props, "Coccion ") or None,
            healthy=_get_select(props, "\U0001f608 / \U0001f607") or None,
            ingredient_ids=_get_relation_ids(props, "Ingredients"),
        )

    async def search_recipe(self, name: str) -> RecipeEntry | None:
        """Search for a recipe by name."""
        pages = await self._query_db(
            "recipes",
            filter_obj={"property": "Name", "title": {"contains": name[:30]}},
            page_size=1,
        )
        if not pages:
            return None
        return self._parse_recipe(pages[0])

    async def get_recipe_ingredients(self, recipe_name: str) -> list[str] | None:
        """
        Find a recipe and return its ingredient names (via multi_select).
        Replaces search_recipe_in_notion() from main.py.
        """
        pages = await self._query_db(
            "recipes",
            filter_obj={"property": "Name", "title": {"contains": recipe_name[:30]}},
            page_size=1,
        )
        if not pages:
            return None
        props = pages[0].get("properties", {})
        ingredientes = _get_multi_select(props, "Ingredientes")
        return ingredientes if ingredientes else None

    async def create_recipe(
        self,
        data: dict,
        ingredient_relation_ids: list[str] = None,
        content_blocks: list[dict] = None,
    ) -> RecipeEntry:
        """
        Create a recipe.
        data keys: name, source, difficulty, type (list), cooking_method, healthy
        ingredient_relation_ids: list of Shopping page IDs for the relation field
        content_blocks: Notion blocks to append to the page body
        """
        props = {
            "Name": {"title": [{"text": {"content": data.get("name", "").capitalize()}}]},
            "Source": {"select": {"name": data.get("source", "Knot")}},
        }
        if data.get("difficulty") in ["Easy", "Moderate", "Hard"]:
            props["Difficult "] = {"select": {"name": data["difficulty"]}}
        if data.get("type") and isinstance(data["type"], list):
            valid = [t for t in data["type"] if t in [
                "Postre", "Cena", "Almuerzo", "Desayuno", "Snack", "Cosmetica"
            ]]
            if valid:
                props["Type"] = {"multi_select": [{"name": t} for t in valid]}
        if data.get("cooking_method") in ["Horno", "Sarten", "Pochar", "Frizzer ", "Varias prep."]:
            props["Coccion "] = {"select": {"name": data["cooking_method"]}}
        if data.get("healthy") in ["Healthy", "Fatty", "ni healthy ni fatty"]:
            props["\U0001f608 / \U0001f607"] = {"select": {"name": data["healthy"]}}
        if ingredient_relation_ids:
            props["Ingredients"] = {"relation": [{"id": rid} for rid in ingredient_relation_ids]}

        page = await self._create_page("recipes", props, emoji="\U0001f37d\ufe0f")

        if content_blocks and page.get("id"):
            await self._append_blocks(page["id"], content_blocks)

        return self._parse_recipe(page)

    # ══════════════════════════════════════════════════════════════════════
    # PLANTS
    # ══════════════════════════════════════════════════════════════════════

    def _parse_plant(self, page: dict) -> PlantEntry:
        props = page.get("properties", {})
        return PlantEntry(
            id=page["id"],
            name=_get_title(props),
            species=_get_text(props, "Species") or None,
            light=_get_select(props, "Light") or None,
            watering=_get_select(props, "Watering") or None,
            location=_get_select(props, "Location") or None,
            status=_get_select(props, "Status") or None,
            purchase_date=_get_date(props, "Purchase Date") or None,
            price=_get_number(props, "Price"),
            notes=_get_text(props, "Notes") or None,
            emoji=page.get("icon", {}).get("emoji", ""),
            last_watering=_get_date(props, "Last Watering") or None,
        )

    async def create_plant(self, data: dict) -> PlantEntry:
        """
        Register a new plant.
        data keys: name, species, purchase_date, price, light, watering,
                    location, status, notes, emoji
        """
        props = {"Name": {"title": [{"text": {"content": data.get("name", "Plant")}}]}}
        if data.get("species"):
            props["Species"] = {"rich_text": [{"text": {"content": data["species"]}}]}
        if data.get("purchase_date"):
            props["Purchase Date"] = {"date": {"start": data["purchase_date"]}}
        if data.get("price"):
            props["Price"] = {"number": float(data["price"])}
        if data.get("light"):
            props["Light"] = {"select": {"name": data["light"]}}
        if data.get("watering"):
            props["Watering"] = {"select": {"name": data["watering"]}}
        if data.get("location"):
            props["Location"] = {"select": {"name": data["location"]}}
        if data.get("status"):
            props["Status"] = {"select": {"name": data["status"]}}
        if data.get("notes"):
            props["Notes"] = {"rich_text": [{"text": {"content": data["notes"]}}]}

        emoji = data.get("emoji", "\U0001f33f")
        page = await self._create_page("plants", props, emoji=emoji)
        return self._parse_plant(page)

    async def query_plants(self) -> list[PlantEntry]:
        """List all plants."""
        pages = await self._query_db("plants", page_size=50)
        return [self._parse_plant(p) for p in pages]

    async def update_plant(self, plant_id: str, updates: dict) -> PlantEntry:
        """Update a plant. Supported update keys: name, status, watering, location, light, notes"""
        props = {}
        if "name" in updates:
            props["Name"] = {"title": [{"text": {"content": updates["name"]}}]}
        if "status" in updates:
            props["Status"] = {"select": {"name": updates["status"]}}
        if "watering" in updates:
            props["Watering"] = {"select": {"name": updates["watering"]}}
        if "location" in updates:
            props["Location"] = {"select": {"name": updates["location"]}}
        if "light" in updates:
            props["Light"] = {"select": {"name": updates["light"]}}
        if "notes" in updates:
            props["Notes"] = {"rich_text": [{"text": {"content": updates["notes"]}}]}
        if "last_watering" in updates:
            lw = updates["last_watering"]
            props["Last Watering"] = {"date": {"start": str(lw) if lw else None}}

        page = await self._update_page(plant_id, props)
        return self._parse_plant(page)

    async def archive_plant(self, plant_id: str) -> bool:
        return await self._archive_page(plant_id)

    async def search_plants(self, query: str) -> list[PlantEntry]:
        """Search plants by name (partial match)."""
        pages = await self._query_db(
            "plants",
            filter_obj={"property": "Name", "title": {"contains": query[:30]}},
            page_size=5,
        )
        return [self._parse_plant(p) for p in pages]

    # ══════════════════════════════════════════════════════════════════════
    # MEETINGS
    # ══════════════════════════════════════════════════════════════════════

    def _parse_meeting(self, page: dict) -> MeetingEntry:
        props = page.get("properties", {})
        return MeetingEntry(
            id=page["id"],
            name=_get_title(props),
            with_whom=_get_text(props, "With") or None,
            date=_get_date(props, "Date") or None,
            notes=_get_text(props, "Notes") or None,
            calendar_link=_get_url(props, "Calendar Link") or None,
        )

    async def create_meeting(self, data: dict) -> MeetingEntry:
        """
        Save meeting notes.
        data keys: name, with_whom, date, notes, calendar_link
        """
        props = {
            "Name": {"title": [{"text": {"content": data.get("name", "Meeting")}}]},
            "Source": {"select": {"name": "Knot"}},
        }
        if data.get("with_whom"):
            props["With"] = {"rich_text": [{"text": {"content": data["with_whom"]}}]}
        if data.get("notes"):
            props["Notes"] = {"rich_text": [{"text": {"content": data["notes"][:2000]}}]}
        if data.get("date"):
            props["Date"] = {"date": {"start": data["date"]}}
        if data.get("calendar_link"):
            props["Calendar Link"] = {"url": data["calendar_link"]}

        page = await self._create_page("meetings", props)
        return self._parse_meeting(page)

    async def query_meetings(self, limit: int = 20) -> list[MeetingEntry]:
        """List the most recent meetings."""
        pages = await self._query_db(
            "meetings",
            sorts=[{"property": "Date", "direction": "descending"}],
            page_size=limit,
        )
        return [self._parse_meeting(p) for p in pages]

    async def update_meeting(self, meeting_id: str, updates: dict) -> MeetingEntry:
        """Update a meeting. Supported update keys: name, notes, with_whom"""
        props = {}
        if "name" in updates:
            props["Name"] = {"title": [{"text": {"content": updates["name"]}}]}
        if "notes" in updates:
            props["Notes"] = {"rich_text": [{"text": {"content": updates["notes"][:2000]}}]}
        if "with_whom" in updates:
            props["With"] = {"rich_text": [{"text": {"content": updates["with_whom"]}}]}

        page = await self._update_page(meeting_id, props)
        return self._parse_meeting(page)

    async def archive_meeting(self, meeting_id: str) -> bool:
        return await self._archive_page(meeting_id)

    async def search_meetings(self, query: str, limit: int = 5) -> list[MeetingEntry]:
        """Search meetings by name (partial match)."""
        pages = await self._query_db(
            "meetings",
            filter_obj={"property": "Name", "title": {"contains": query[:50]}},
            sorts=[{"property": "Date", "direction": "descending"}],
            page_size=limit,
        )
        return [self._parse_meeting(p) for p in pages]

    # ══════════════════════════════════════════════════════════════════════
    # TASKS
    # ══════════════════════════════════════════════════════════════════════

    def _parse_task(self, page: dict) -> TaskEntry:
        props = page.get("properties", {})
        return TaskEntry(
            id=page["id"],
            name=_get_title(props),
            category=_get_select(props, "Category") or None,
            status=_get_status(props, "Status"),
            priority=_get_select(props, "Priority") or None,
            due_date=_get_date(props, "Due Date") or None,
            source=_get_select(props, "Source") or None,
            notes=_get_text(props, "Notes") or None,
        )

    async def get_pending_tasks(self, category: str = None) -> list[TaskEntry]:
        """Pending tasks, optionally filtered by category."""
        conditions = [{"property": "Status", "status": {"does_not_equal": "Listo"}}]
        if category:
            conditions.append({"property": "Category", "select": {"equals": category}})
        pages = await self._query_db(
            "tasks",
            filter_obj={"and": conditions},
            page_size=30,
        )
        return [self._parse_task(p) for p in pages]

    async def create_task(self, data: dict) -> TaskEntry:
        """
        Create a task.
        data keys: name, category, status, source, notes, due_date, priority, emoji
        """
        props = {
            "Name": {"title": [{"text": {"content": data.get("name", "Task")}}]},
            "Status": {"status": {"name": data.get("status", "Sin empezar")}},
        }
        if data.get("category"):
            props["Category"] = {"select": {"name": data["category"]}}
        if data.get("source"):
            props["Source"] = {"select": {"name": data["source"]}}
        if data.get("notes"):
            props["Notes"] = {"rich_text": [{"text": {"content": data["notes"]}}]}
        if data.get("due_date"):
            props["Due Date"] = {"date": {"start": data["due_date"]}}
        if data.get("priority"):
            props["Priority"] = {"select": {"name": data["priority"]}}

        page = await self._create_page("tasks", props)
        return self._parse_task(page)

    async def update_task(self, task_id: str, updates: dict) -> TaskEntry:
        """Update a task. Supported update keys: status, name, priority, notes"""
        props = {}
        if "status" in updates:
            props["Status"] = {"status": {"name": updates["status"]}}
        if "name" in updates:
            props["Name"] = {"title": [{"text": {"content": updates["name"]}}]}
        if "priority" in updates:
            props["Priority"] = {"select": {"name": updates["priority"]}}
        if "notes" in updates:
            props["Notes"] = {"rich_text": [{"text": {"content": updates["notes"]}}]}

        page = await self._update_page(task_id, props)
        return self._parse_task(page)

    async def get_pending_factura_tasks(self) -> list[dict]:
        """
        Return pending bill tasks (category Finanzas + source Matrics + not Listo).
        Returns dicts with page_id, name, due, provider, amount, period.
        Mirrors the existing function in main.py exactly.
        """
        conditions = [
            {"property": "Category", "select": {"equals": "Finanzas"}},
            {"property": "Status", "status": {"does_not_equal": "Listo"}},
            {"property": "Source", "select": {"equals": "Knot"}},
        ]
        pages = await self._query_db(
            "tasks",
            filter_obj={"and": conditions},
            page_size=20,
        )
        tasks = []
        for page in pages:
            props = page.get("properties", {})
            notes_str = _get_text(props, "Notes")
            meta = {}
            try:
                meta = json.loads(notes_str)
            except Exception:
                pass
            tasks.append({
                "page_id": page["id"],
                "name": _get_title(props),
                "due": _get_date(props, "Due Date"),
                "provider": meta.get("provider", ""),
                "amount": meta.get("amount", 0),
                "period": meta.get("period", ""),
                "finance_page_id": meta.get("finance_page_id", ""),
            })
        return tasks

    async def ensure_db_select_field(self, db_name: str, field_name: str, options: list) -> bool:
        """Add a select field to a Notion DB if it doesn't already exist."""
        try:
            r = await self._http.patch(
                f"{NOTION_API}/databases/{self._db(db_name)}",
                headers=self._headers_cache,
                json={"properties": {field_name: {"select": {"options": [{"name": o} for o in options]}}}},
            )
            return r.status_code == 200
        except Exception:
            return False

    async def load_payment_methods(self) -> list:
        """Load all rows from the Payment Methods DB."""
        try:
            pages = await self._query_db("payment_methods", page_size=50)
            results = []
            for p in pages:
                props = p.get("properties", {})
                uses_raw = props.get("Uses", {}).get("number") or 0
                results.append(PaymentMethod(
                    id=p["id"],
                    name=_get_title(props, "Name"),
                    modality=_get_select(props, "Modality") or "",
                    bank=_get_select(props, "Bank") or _get_text(props, "Bank") or None,
                    last4=_get_text(props, "Last4") or None,
                    owner=_get_text(props, "Owner") or None,
                    is_default=_get_checkbox(props, "Default"),
                    uses=int(uses_raw),
                ))
            return results
        except Exception:
            return []

    async def create_payment_method(self, name: str, modality: str, bank: str = None,
                                     last4: str = None, owner: str = None, is_default: bool = False) -> str | None:
        """Create a new payment method row. Returns page_id or None."""
        try:
            props = {
                "Name": {"title": [{"text": {"content": name}}]},
                "Modality": {"select": {"name": modality}},
                "Default": {"checkbox": is_default},
            }
            if bank:
                props["Bank"] = {"select": {"name": bank}}
            if last4:
                props["Last4"] = {"rich_text": [{"text": {"content": last4}}]}
            if owner:
                props["Owner"] = {"rich_text": [{"text": {"content": owner}}]}
            page = await self._create_page("payment_methods", props)
            return page["id"]
        except Exception:
            return None

    async def ensure_db_text_field(self, db_name: str, field_name: str) -> bool:
        """Add a rich_text field to a Notion DB if it doesn't already exist."""
        try:
            r = await self._http.patch(
                f"{NOTION_API}/databases/{self._db(db_name)}",
                headers=self._headers_cache,
                json={"properties": {field_name: {"rich_text": {}}}},
            )
            return r.status_code == 200
        except Exception:
            return False

    async def ensure_db_number_field(self, db_name: str, field_name: str) -> bool:
        """Add a number field to a Notion DB if it doesn't already exist."""
        try:
            r = await self._http.patch(
                f"{NOTION_API}/databases/{self._db(db_name)}",
                headers=self._headers_cache,
                json={"properties": {field_name: {"number": {"format": "number"}}}},
            )
            return r.status_code == 200
        except Exception:
            return False

    async def create_finance_invoice(
        self, provider: str, amount: float, period: str, due_date: str = "", category: str = "Recurrente"
    ) -> tuple[bool, str]:
        """Create an Impaga finance entry. Returns (success, page_id). Deduplicates by provider+period."""
        import re
        from datetime import date as _date, timezone
        new_digits = set(re.findall(r"\d+", period or ""))
        today = _date.today()
        # Check both impaga AND pagada records — avoid recreating an already-paid invoice
        for existing in [await self.get_impaga_facturas(provider=provider),
                         await self.get_finance_history_by_provider(provider, limit=10)]:
            for e in existing:
                if new_digits and new_digits & set(re.findall(r"\d+", e.name)):
                    return False, "duplicate"
                # Fallback: si hay un registro del mismo proveedor en los últimos 60 días, deduplicar
                if e.date:
                    try:
                        entry_date = e.date if isinstance(e.date, _date) else _date.fromisoformat(str(e.date)[:10])
                        if (today - entry_date).days <= 60:
                            return False, "duplicate"
                    except Exception:
                        pass
        now = datetime.now(timezone.utc) - timedelta(hours=3)
        entry = await self.create_expense({
            "name": f"Factura {provider} — {period}",
            "in_out": "← EGRESO →",
            "value_ars": amount or 0,
            "categories": [category],
            "method": "Payment",
            "date": now.strftime("%Y-%m-%d"),
            "estado": "Impaga",
            "emoji": "💸",
        })
        return True, entry.id

    async def get_impaga_facturas(self, provider: str = None) -> list:
        """Returns Estado=Impaga finance entries, ordered by date desc."""
        filter_obj = {"and": [{"property": "Estado", "select": {"equals": "Impaga"}}]}
        if provider:
            filter_obj["and"].append({"property": "Name", "title": {"contains": provider}})
        try:
            pages = await self._query_db(
                "finances",
                filter_obj=filter_obj,
                sorts=[{"property": "Date", "direction": "descending"}],
                page_size=20,
            )
            return [self._parse_expense(p) for p in pages]
        except Exception:
            return []

    async def get_finance_history_by_provider(self, provider: str, limit: int = 5) -> list:
        """Returns paid finance entries for a provider (Estado=Pagada or empty)."""
        filter_obj = {"and": [
            {"property": "Name", "title": {"contains": provider}},
            {"or": [
                {"property": "Estado", "select": {"equals": "Pagada"}},
                {"property": "Estado", "select": {"is_empty": True}},
            ]},
        ]}
        try:
            pages = await self._query_db(
                "finances",
                filter_obj=filter_obj,
                sorts=[{"property": "Date", "direction": "descending"}],
                page_size=limit,
            )
            return [self._parse_expense(p) for p in pages]
        except Exception:
            return []

    async def mark_finance_paid(
        self, page_id: str, paid_amount: float = None, payment_method: str = None, notes: str = None
    ) -> bool:
        """Mark a finance entry as Pagada, optionally updating amount, method, notes."""
        try:
            props = {"Estado": {"select": {"name": "Pagada"}}}
            if paid_amount is not None:
                props["Value (ars)"] = {"number": float(paid_amount)}
            if payment_method:
                props["Method"] = {"select": {"name": payment_method}}
            if notes:
                page_r = await self._http.get(
                    f"{NOTION_API}/pages/{page_id}", headers=self._headers_cache
                )
                existing_notes = ""
                if page_r.status_code == 200:
                    existing_notes = _get_text(page_r.json().get("properties", {}), "Notes") or ""
                new_notes = (existing_notes + "\n" + notes).strip() if existing_notes else notes
                props["Notes"] = {"rich_text": [{"text": {"content": new_notes[:2000]}}]}
            await self._update_page(page_id, props)
            return True
        except Exception:
            return False

    async def create_factura_task(
        self, provider: str, amount: float, due_date: str, period: str, finance_page_id: str = None
    ) -> tuple[bool, str]:
        """
        Create a pending bill task. Avoids duplicates by provider + period.
        Returns (success, page_id_or_"duplicate").
        """
        import re
        existing = await self.get_pending_factura_tasks()
        for t in existing:
            prov_low = t["provider"].lower()
            prov_match = prov_low and (prov_low in provider.lower() or provider.lower() in prov_low)
            if not prov_match and prov_low:
                prov_words = set(w for w in prov_low.split() if len(w) > 3)
                new_words = set(w for w in provider.lower().split() if len(w) > 3)
                prov_match = bool(prov_words & new_words)
            if prov_match:
                existing_digits = set(re.findall(r"\d+", t.get("period", "")))
                new_digits = set(re.findall(r"\d+", period or ""))
                period_match = not period or not t.get("period") or bool(existing_digits & new_digits)
                if period_match:
                    return False, "duplicate"

        from datetime import timezone
        now = datetime.now(timezone.utc) - timedelta(hours=3)
        meta_dict = {"provider": provider, "amount": amount, "period": period}
        if finance_page_id:
            meta_dict["finance_page_id"] = finance_page_id
        meta = json.dumps(meta_dict, ensure_ascii=False)
        priority = "Media"
        if due_date:
            try:
                days_left = (datetime.strptime(due_date, "%Y-%m-%d").date() - now.date()).days
                priority = "Alta" if days_left <= 3 else "Media"
            except Exception:
                pass

        task = await self.create_task({
            "name": f"\U0001f4b0 Pagar {provider} \u2014 {period}",
            "category": "Finanzas",
            "source": "Knot",
            "notes": meta,
            "due_date": due_date,
            "priority": priority,
        })
        return True, task.id

    async def mark_factura_task_paid(self, page_id: str) -> bool:
        """Archive a paid bill task so it disappears from Tasks DB."""
        try:
            await self._archive_page(page_id)
            return True
        except Exception:
            return False

    # ══════════════════════════════════════════════════════════════════════
    # PROJECTS
    # ══════════════════════════════════════════════════════════════════════

    def _parse_project(self, page: dict) -> ProjectEntry:
        props = page.get("properties", {})
        return ProjectEntry(
            id=page["id"],
            name=_get_title(props),
            entry_type=_get_select(props, "Entry Type"),
            area=_get_select(props, "Area"),
            status=_get_status(props, "Status"),
            priority=_get_select(props, "Priority") or None,
            description=_get_text(props, "Description") or None,
            emoji=page.get("icon", {}).get("emoji", ""),
        )

    async def create_project(self, data: dict) -> ProjectEntry:
        """
        Create a project or idea.
        data keys: name, entry_type, area, description, priority, emoji
        """
        from datetime import timezone
        now = datetime.now(timezone.utc) - timedelta(hours=3)

        props = {
            "Name": {"title": [{"text": {"content": data.get("name", "Project")}}]},
            "Entry Type": {"select": {"name": data.get("entry_type", "Proyecto")}},
            "Area": {"select": {"name": data.get("area", "Personal")}},
            "Status": {"status": {"name": "Sin empezar"}},
            "Source": {"select": {"name": "Knot"}},
            "Date": {"date": {"start": now.strftime("%Y-%m-%d")}},
        }
        if data.get("description"):
            props["Description"] = {"rich_text": [{"text": {"content": data["description"][:2000]}}]}
        if data.get("priority") in ["Alta", "Media", "Baja"]:
            props["Priority"] = {"select": {"name": data["priority"]}}

        emoji = data.get("emoji", "\U0001f4cb")
        page = await self._create_page("projects", props, emoji=emoji)
        return self._parse_project(page)

    async def query_projects(self, area: str = None) -> list[ProjectEntry]:
        """List projects, optionally filtered by area."""
        filter_obj = None
        if area:
            filter_obj = {"property": "Area", "select": {"equals": area}}
        pages = await self._query_db(
            "projects",
            filter_obj=filter_obj,
            sorts=[{"property": "Date", "direction": "descending"}],
            page_size=30,
        )
        return [self._parse_project(p) for p in pages]

    async def update_project(self, project_id: str, updates: dict) -> ProjectEntry:
        """Update a project. Supported update keys: name, status, description, priority, area"""
        props = {}
        if "name" in updates:
            props["Name"] = {"title": [{"text": {"content": updates["name"]}}]}
        if "status" in updates:
            props["Status"] = {"status": {"name": updates["status"]}}
        if "description" in updates:
            props["Description"] = {"rich_text": [{"text": {"content": updates["description"][:2000]}}]}
        if "priority" in updates:
            props["Priority"] = {"select": {"name": updates["priority"]}}
        if "area" in updates:
            props["Area"] = {"select": {"name": updates["area"]}}

        page = await self._update_page(project_id, props)
        return self._parse_project(page)

    async def archive_project(self, project_id: str) -> bool:
        return await self._archive_page(project_id)

    # ══════════════════════════════════════════════════════════════════════
    # HEALTH RECORDS
    # ══════════════════════════════════════════════════════════════════════

    async def create_health_record(self, data: dict) -> tuple[bool, str]:
        """Create a health record. data keys: name, type, date, specialty, doctor, summary, key_values, notes"""
        props = {
            "Name": {"title": [{"text": {"content": data.get("name", "Registro de salud")}}]},
        }
        if data.get("type"):
            props["Type"] = {"select": {"name": data["type"]}}
        if data.get("date"):
            props["Date"] = {"date": {"start": data["date"]}}
        if data.get("specialty"):
            props["Specialty"] = {"select": {"name": data["specialty"]}}
        if data.get("doctor"):
            props["Doctor"] = {"rich_text": [{"text": {"content": data["doctor"][:200]}}]}
        if data.get("summary"):
            props["Summary"] = {"rich_text": [{"text": {"content": data["summary"][:2000]}}]}
        if data.get("key_values"):
            kv = data["key_values"] if isinstance(data["key_values"], str) else json.dumps(data["key_values"], ensure_ascii=False)
            props["Key Values"] = {"rich_text": [{"text": {"content": kv[:2000]}}]}
        if data.get("notes"):
            props["Notes"] = {"rich_text": [{"text": {"content": data["notes"][:2000]}}]}
        try:
            page = await self._create_page("health_records", props)
            return True, page["id"]
        except Exception as e:
            return False, str(e)[:200]

    async def query_health_records(self, type_filter: str = None, specialty_filter: str = None, limit: int = 5) -> list[dict]:
        """Query health records. Returns dicts with id, name, type, date, specialty, doctor, summary, key_values, notes."""
        filters = []
        if type_filter:
            filters.append({"property": "Type", "select": {"equals": type_filter}})
        if specialty_filter:
            filters.append({"property": "Specialty", "select": {"equals": specialty_filter}})
        filter_obj = {"and": filters} if len(filters) > 1 else (filters[0] if filters else None)
        pages = await self._query_db(
            "health_records",
            filter_obj=filter_obj,
            sorts=[{"property": "Date", "direction": "descending"}],
            page_size=limit,
        )
        results = []
        for page in pages:
            p = page.get("properties", {})
            results.append({
                "id": page["id"],
                "name": _get_title(p),
                "type": _get_select(p, "Type"),
                "date": _get_date(p, "Date"),
                "specialty": _get_select(p, "Specialty"),
                "doctor": _get_text(p, "Doctor"),
                "summary": _get_text(p, "Summary"),
                "key_values": _get_text(p, "Key Values"),
                "notes": _get_text(p, "Notes"),
            })
        return results

    async def update_health_record(self, page_id: str, updates: dict) -> bool:
        """Update a health record. Supported keys: name, summary, key_values, notes, doctor."""
        props = {}
        if "name" in updates:
            props["Name"] = {"title": [{"text": {"content": updates["name"]}}]}
        if "summary" in updates:
            props["Summary"] = {"rich_text": [{"text": {"content": updates["summary"][:2000]}}]}
        if "key_values" in updates:
            props["Key Values"] = {"rich_text": [{"text": {"content": updates["key_values"][:2000]}}]}
        if "notes" in updates:
            props["Notes"] = {"rich_text": [{"text": {"content": updates["notes"][:2000]}}]}
        if "doctor" in updates:
            props["Doctor"] = {"rich_text": [{"text": {"content": updates["doctor"]}}]}
        try:
            await self._update_page(page_id, props)
            return True
        except Exception:
            return False

    async def archive_health_record(self, page_id: str) -> bool:
        return await self._archive_page(page_id)

    # ══════════════════════════════════════════════════════════════════════
    # MEDICATIONS
    # ══════════════════════════════════════════════════════════════════════

    async def create_medication(self, data: dict) -> tuple[bool, str]:
        """Create a medication entry. data keys: name, active, dose, frequency, prescribed_by, condition, start_date, end_date, notes"""
        props = {
            "Name": {"title": [{"text": {"content": data.get("name", "Medicación")}}]},
            "Active": {"checkbox": data.get("active", True)},
        }
        for field, key in [("Dose", "dose"), ("Frequency", "frequency"),
                            ("Prescribed By", "prescribed_by"), ("Condition", "condition"), ("Notes", "notes")]:
            if data.get(key):
                props[field] = {"rich_text": [{"text": {"content": str(data[key])[:500]}}]}
        if data.get("start_date"):
            props["Start Date"] = {"date": {"start": data["start_date"]}}
        if data.get("end_date"):
            props["End Date"] = {"date": {"start": data["end_date"]}}
        try:
            page = await self._create_page("medications", props)
            return True, page["id"]
        except Exception as e:
            return False, str(e)[:200]

    async def query_medications(self, only_active: bool = False) -> list[dict]:
        """Query medications. Returns dicts with id, name, active, dose, frequency, etc."""
        filter_obj = {"property": "Active", "checkbox": {"equals": True}} if only_active else None
        pages = await self._query_db("medications", filter_obj=filter_obj, page_size=50)
        results = []
        for page in pages:
            p = page.get("properties", {})
            results.append({
                "id": page["id"],
                "name": _get_title(p),
                "active": _get_checkbox(p, "Active"),
                "dose": _get_text(p, "Dose"),
                "frequency": _get_text(p, "Frequency"),
                "prescribed_by": _get_text(p, "Prescribed By"),
                "condition": _get_text(p, "Condition"),
                "start_date": _get_date(p, "Start Date"),
                "end_date": _get_date(p, "End Date"),
                "notes": _get_text(p, "Notes"),
            })
        return results

    async def update_medication(self, page_id: str, updates: dict) -> bool:
        """Update a medication. Supported keys: active, dose, frequency, notes, end_date"""
        props = {}
        if "active" in updates and updates["active"] is not None:
            props["Active"] = {"checkbox": updates["active"]}
        for field, key in [("Dose", "dose"), ("Frequency", "frequency"), ("Notes", "notes")]:
            if updates.get(key):
                props[field] = {"rich_text": [{"text": {"content": updates[key]}}]}
        if updates.get("end_date"):
            props["End Date"] = {"date": {"start": updates["end_date"]}}
        if not props:
            return True
        try:
            await self._update_page(page_id, props)
            return True
        except Exception:
            return False

    # ══════════════════════════════════════════════════════════════════════
    # FITNESS
    # ══════════════════════════════════════════════════════════════════════

    def _parse_fitness(self, page: dict) -> dict:
        p = page.get("properties", {})
        return {
            "id":         page["id"],
            "name":       _get_title(p),
            "activity":   _get_select(p, "Activity"),
            "date":       _get_date(p, "Date"),
            "duration":   _get_number(p, "Duration"),
            "distance":   _get_number(p, "Distance"),
            "calories":   _get_number(p, "Calories"),
            "avg_speed":  _get_number(p, "Avg Speed"),
            "elevation":  _get_number(p, "Elevation"),
            "notes":      _get_text(p, "Notes"),
            "source":     _get_select(p, "Source"),
            "source_app": _get_text(p, "Source App"),
        }

    async def create_fitness(self, data: dict) -> tuple[bool, str]:
        """Create a fitness entry.
        data keys: name, activity, date, duration, distance, calories, avg_speed, elevation, notes, source, source_app
        """
        props = {
            "Name": {"title": [{"text": {"content": data.get("name", "Actividad")}}]},
        }
        if data.get("activity"):
            props["Activity"] = {"select": {"name": data["activity"]}}
        if data.get("date"):
            props["Date"] = {"date": {"start": data["date"]}}
        for num_field, key in [("Duration", "duration"), ("Distance", "distance"),
                                ("Calories", "calories"), ("Avg Speed", "avg_speed"),
                                ("Elevation", "elevation")]:
            if data.get(key) is not None:
                props[num_field] = {"number": float(data[key])}
        if data.get("notes"):
            props["Notes"] = {"rich_text": [{"text": {"content": data["notes"][:2000]}}]}
        props["Source"] = {"select": {"name": data.get("source", "Manual")}}
        if data.get("source_app"):
            props["Source App"] = {"rich_text": [{"text": {"content": data["source_app"]}}]}
        try:
            page = await self._create_page("fitness", props)
            return True, page["id"]
        except Exception as e:
            return False, str(e)

    async def query_fitness(self, activity: str = None, month: str = None, limit: int = 20) -> list[dict]:
        """Query fitness entries. Optionally filter by activity type and/or month (YYYY-MM)."""
        filters = []
        if activity:
            filters.append({"property": "Activity", "select": {"equals": activity}})
        if month:
            try:
                from datetime import datetime as _dt
                start = f"{month}-01"
                import calendar as _cal
                y, m = int(month[:4]), int(month[5:7])
                end = f"{month}-{_cal.monthrange(y, m)[1]:02d}"
                filters.append({"property": "Date", "date": {"on_or_after": start}})
                filters.append({"property": "Date", "date": {"on_or_before": end}})
            except Exception:
                pass
        if len(filters) > 1:
            filter_obj = {"and": filters}
        elif filters:
            filter_obj = filters[0]
        else:
            filter_obj = None
        pages = await self._query_db(
            "fitness",
            filter_obj=filter_obj,
            sorts=[{"property": "Date", "direction": "descending"}],
            page_size=limit,
        )
        return [self._parse_fitness(p) for p in pages]

    async def update_fitness(self, entry_id: str, updates: dict) -> bool:
        """Update a fitness entry. Supported keys: name, activity, date, duration, distance, calories, avg_speed, elevation, notes"""
        props = {}
        if "name" in updates:
            props["Name"] = {"title": [{"text": {"content": updates["name"]}}]}
        if "activity" in updates:
            props["Activity"] = {"select": {"name": updates["activity"]}}
        if "date" in updates:
            props["Date"] = {"date": {"start": updates["date"]}}
        for num_field, key in [("Duration", "duration"), ("Distance", "distance"),
                                ("Calories", "calories"), ("Avg Speed", "avg_speed"),
                                ("Elevation", "elevation")]:
            if updates.get(key) is not None:
                props[num_field] = {"number": float(updates[key])}
        if "notes" in updates:
            props["Notes"] = {"rich_text": [{"text": {"content": updates["notes"][:2000]}}]}
        try:
            await self._update_page(entry_id, props)
            return True
        except Exception:
            return False

    async def archive_fitness(self, entry_id: str) -> bool:
        return await self._archive_page(entry_id)

    # ══════════════════════════════════════════════════════════════════════
    # GEO-REMINDERS
    # ══════════════════════════════════════════════════════════════════════

    def _parse_geo_reminder(self, page: dict) -> GeoReminder:
        props = page.get("properties", {})
        return GeoReminder(
            id=page["id"],
            name=_get_title(props),
            reminder_type=_get_select(props, "Type") or "place",
            shop_name=_get_text(props, "Shop Name") or None,
            lat=_get_number(props, "Latitude"),
            lon=_get_number(props, "Longitude"),
            radius=int(_get_number(props, "Radius") or 300),
            recurrent=_get_checkbox(props, "Recurrent"),
            active=_get_checkbox(props, "Active"),
        )

    async def get_active_geo_reminders(self) -> list[GeoReminder]:
        """Return all active geo-reminders."""
        pages = await self._query_db(
            "geo_reminders",
            filter_obj={"property": "Active", "checkbox": {"equals": True}},
            page_size=50,
        )
        return [self._parse_geo_reminder(p) for p in pages]

    async def create_geo_reminder(self, data: dict) -> GeoReminder:
        """
        Create a geo-reminder.
        data keys: name, type, lat, lon, shop_name, radius, recurrent
        """
        props = {
            "Name": {"title": [{"text": {"content": data.get("name", "Reminder")}}]},
            "Type": {"select": {"name": data.get("type", "place")}},
            "Active": {"checkbox": True},
            "Recurrent": {"checkbox": data.get("recurrent", False)},
            "Radius": {"number": data.get("radius", 300)},
        }
        if data.get("lat") is not None:
            props["Latitude"] = {"number": data["lat"]}
        if data.get("lon") is not None:
            props["Longitude"] = {"number": data["lon"]}
        if data.get("shop_name"):
            props["Shop Name"] = {"rich_text": [{"text": {"content": data["shop_name"]}}]}

        page = await self._create_page("geo_reminders", props)
        return self._parse_geo_reminder(page)

    async def update_geo_reminder(self, reminder_id: str, updates: dict) -> GeoReminder:
        """Update a geo-reminder. Supported update keys: name, radius, recurrent, active"""
        props = {}
        if "name" in updates:
            props["Name"] = {"title": [{"text": {"content": updates["name"]}}]}
        if "radius" in updates:
            props["Radius"] = {"number": updates["radius"]}
        if "recurrent" in updates:
            props["Recurrent"] = {"checkbox": updates["recurrent"]}
        if "active" in updates:
            props["Active"] = {"checkbox": updates["active"]}

        page = await self._update_page(reminder_id, props)
        return self._parse_geo_reminder(page)

    async def deactivate_geo_reminder(self, reminder_id: str) -> bool:
        """Deactivate a geo-reminder without deleting it."""
        try:
            await self.update_geo_reminder(reminder_id, {"active": False})
            return True
        except Exception:
            return False

    # ══════════════════════════════════════════════════════════════════════
    # CONFIG
    # ══════════════════════════════════════════════════════════════════════

    async def load_config(self, phone: str = None) -> tuple[UserConfig, str]:
        """
        Load user config from Notion.
        Returns (config, page_id) — the page_id is required for save_config.
        If phone is provided, filters by WA Number property.
        """
        filter_obj = None
        if phone:
            filter_obj = {"property": "WA Number", "rich_text": {"equals": phone}}

        pages = await self._query_db("config", filter_obj=filter_obj, page_size=1)
        if not pages:
            return UserConfig(phone=phone or ""), ""

        page = pages[0]
        props = page.get("properties", {})

        extras_raw = _get_text(props, "Resumen Extras")
        extras = [e.strip() for e in extras_raw.split("|") if e.strip()] if extras_raw else []

        topics_raw = _get_text(props, "News Topics")
        topics = [t.strip() for t in topics_raw.split(",") if t.strip()] if topics_raw else []

        providers_raw = _get_text(props, "Service Providers")
        try:
            providers = json.loads(providers_raw) if providers_raw else {}
        except Exception:
            providers = {}

        known_raw = _get_text(props, "Known Places")
        try:
            known = json.loads(known_raw) if known_raw else []
        except Exception:
            known = []

        activities_raw = _get_text(props, "Activities")
        try:
            activities = json.loads(activities_raw) if activities_raw else {}
        except Exception:
            activities = {}

        counts_raw = _get_text(props, "Purchase Counts")
        try:
            purchase_counts = json.loads(counts_raw) if counts_raw else {}
        except Exception:
            purchase_counts = {}

        generative_lists_raw = _get_text(props, "Generative Lists")
        try:
            generative_lists = json.loads(generative_lists_raw) if generative_lists_raw else {}
        except Exception:
            generative_lists = {}

        known_shops_raw = _get_text(props, "Known Shops")
        try:
            known_shops = json.loads(known_shops_raw) if known_shops_raw else {}
        except Exception:
            known_shops = {}

        feature_hints_raw = _get_text(props, "Feature Hints")
        try:
            feature_hints = json.loads(feature_hints_raw) if feature_hints_raw else {}
        except Exception:
            feature_hints = {}

        domain_profile_fields = [
            ("actividad_fisica", "Profile Actividad Fisica"),
            ("dieta",            "Profile Dieta"),
            ("supermercado",     "Profile Supermercado"),
            ("gastos",           "Profile Gastos"),
            ("salud",            "Profile Salud"),
            ("social",           "Profile Social"),
            ("hogar",            "Profile Hogar"),
            ("productividad",    "Profile Productividad"),
        ]
        domain_profiles = {}
        for key, field in domain_profile_fields:
            val = _get_text(props, field)
            if val:
                domain_profiles[key] = val

        _hour = _get_number(props, "Resumen Hour")
        _min  = _get_number(props, "Resumen Minute")
        _sem_h = _get_number(props, "Resumen Semanal Hour")
        config = UserConfig(
            phone=phone or _get_text(props, "WA Number"),
            greeting_name=_get_text(props, "Greeting Name") or None,
            daily_summary_hour=int(_hour) if _hour is not None else None,
            daily_summary_minute=int(_min) if _min is not None else None,
            resumen_nocturno_enabled=_get_checkbox(props, "Resumen Nocturno Enabled"),
            resumen_nocturno_hour=int(_get_number(props, "Resumen Nocturno Hour") or 22),
            resumen_semanal_enabled=_get_checkbox(props, "Resumen Semanal Enabled") if props.get("Resumen Semanal Enabled") else True,
            resumen_semanal_hour=int(_sem_h) if _sem_h is not None else 21,
            resumen_extras=extras,
            news_topics=topics,
            service_providers=providers,
            known_places=known,
            activities=activities,
            domain_profiles=domain_profiles,
            purchase_counts=purchase_counts,
            saved_lat=_get_number(props, "Latitude"),
            saved_lon=_get_number(props, "Longitude"),
            saved_city=_get_text(props, "City") or None,
            last_summary_date=_get_text(props, "Last Summary Date") or None,
            known_shops=known_shops or None,
            feature_hints=feature_hints or None,
            generative_lists=generative_lists or None,
            pending_invoice_confirmations=_load_json_list(props, "Pending Invoice Confirmations"),
        )
        return config, page["id"]

    async def save_config(self, page_id: str, config: UserConfig) -> bool:
        """Persist user config. Requires the page_id returned by load_config."""
        if not page_id:
            return False

        extras_str = " | ".join(config.resumen_extras or [])
        topics_str = ", ".join(config.news_topics or [])
        props = {
            "Greeting Name":     {"rich_text": [{"text": {"content": config.greeting_name or "Buenos dias"}}]},
            "Resumen Extras":    {"rich_text": [{"text": {"content": extras_str}}]},
            "News Topics":       {"rich_text": [{"text": {"content": topics_str}}]},
            "Service Providers": {"rich_text": [{"text": {"content": json.dumps(config.service_providers or {}, ensure_ascii=False)}}]},
            "Known Places":      {"rich_text": [{"text": {"content": json.dumps(config.known_places or [], ensure_ascii=False)}}]},
            "Activities":        {"rich_text": [{"text": {"content": json.dumps(config.activities or {}, ensure_ascii=False)}}]},
            "Purchase Counts":   {"rich_text": [{"text": {"content": json.dumps(config.purchase_counts or {}, ensure_ascii=False)[:2000]}}]},
            "Known Shops":       {"rich_text": [{"text": {"content": json.dumps(config.known_shops or {}, ensure_ascii=False)[:2000]}}]},
            "Feature Hints":     {"rich_text": [{"text": {"content": json.dumps(config.feature_hints or {}, ensure_ascii=False)[:2000]}}]},
            "Generative Lists":  {"rich_text": [{"text": {"content": json.dumps(config.generative_lists or {}, ensure_ascii=False)[:2000]}}]},
            "Pending Invoice Confirmations": {"rich_text": [{"text": {"content": json.dumps(config.pending_invoice_confirmations or [], ensure_ascii=False)[:2000]}}]},
            "Resumen Nocturno Enabled": {"checkbox": config.resumen_nocturno_enabled},
        }
        for key, field in [
            ("actividad_fisica", "Profile Actividad Fisica"),
            ("dieta",            "Profile Dieta"),
            ("supermercado",     "Profile Supermercado"),
            ("gastos",           "Profile Gastos"),
            ("salud",            "Profile Salud"),
            ("social",           "Profile Social"),
            ("hogar",            "Profile Hogar"),
            ("productividad",    "Profile Productividad"),
        ]:
            val = (config.domain_profiles or {}).get(key, "")
            props[field] = {"rich_text": [{"text": {"content": val[:2000]}}]}
        if config.daily_summary_hour is not None:
            props["Resumen Hour"]   = {"number": config.daily_summary_hour}
            props["Resumen Minute"] = {"number": config.daily_summary_minute or 0}
        if config.resumen_nocturno_hour is not None:
            props["Resumen Nocturno Hour"] = {"number": config.resumen_nocturno_hour}

        try:
            await self._update_page(page_id, props)
        except Exception:
            return False
        if config.last_summary_date:
            try:
                await self._update_page(page_id, {"Last Summary Date": {"rich_text": [{"text": {"content": config.last_summary_date}}]}})
            except Exception:
                pass
        return True

    async def save_location(self, page_id: str, lat: float, lon: float, city: str = None) -> bool:
        """Save coordinates and city to config. Kept separate for throttling purposes."""
        if not page_id:
            return False
        props = {
            "Latitude": {"number": lat},
            "Longitude": {"number": lon},
        }
        if city:
            props["City"] = {"rich_text": [{"text": {"content": city}}]}
        try:
            await self._update_page(page_id, props)
            return True
        except Exception:
            return False

    async def update_config_fields(self, page_id: str, fields: dict) -> bool:
        """Update arbitrary rich_text fields on the config page. fields = {notion_field_name: text_value}."""
        if not page_id:
            return False
        props = {
            k: {"rich_text": [{"text": {"content": str(v)[:2000]}}]}
            for k, v in fields.items()
        }
        try:
            await self._update_page(page_id, props)
            return True
        except Exception:
            return False
