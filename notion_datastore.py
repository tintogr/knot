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
        resumen_extras: list = None
        news_topics: list = None
        service_providers: dict = None
        known_places: list = None
        saved_lat: float = None
        saved_lon: float = None
        saved_city: str = None


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
        self._token = token
        self._db_ids = db_ids

    # ── Internal HTTP helpers ──────────────────────────────────────────────

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._token}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json",
        }

    def _db(self, name: str) -> str:
        """Return the clean database ID. Raises an error if not configured."""
        db_id = self._db_ids.get(name)
        if not db_id:
            raise DataStoreError(f"Database '{name}' not configured in db_ids")
        return _clean_db_id(db_id)

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
        async with httpx.AsyncClient(timeout=15) as http:
            r = await http.post(
                f"{NOTION_API}/databases/{self._db(db_name)}/query",
                headers=self._headers(),
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
        async with httpx.AsyncClient(timeout=15) as http:
            r = await http.post(
                f"{NOTION_API}/pages",
                headers=self._headers(),
                json=body,
            )
            if r.status_code not in (200, 201):
                raise DataStoreError(f"Create in {db_name} failed ({r.status_code}): {r.text[:200]}")
            return r.json()

    async def _update_page(self, page_id: str, props: dict) -> dict:
        """Update page properties. Returns the updated page."""
        async with httpx.AsyncClient(timeout=15) as http:
            r = await http.patch(
                f"{NOTION_API}/pages/{page_id}",
                headers=self._headers(),
                json={"properties": props},
            )
            if r.status_code != 200:
                raise DataStoreError(f"Update {page_id} failed ({r.status_code}): {r.text[:200]}")
            return r.json()

    async def _archive_page(self, page_id: str) -> bool:
        """Archive (soft-delete) a page."""
        async with httpx.AsyncClient(timeout=15) as http:
            r = await http.patch(
                f"{NOTION_API}/pages/{page_id}",
                headers=self._headers(),
                json={"archived": True},
            )
            return r.status_code == 200

    async def _append_blocks(self, page_id: str, blocks: list[dict]) -> bool:
        """Append content blocks to a page (used for recipes, etc.)."""
        async with httpx.AsyncClient(timeout=15) as http:
            r = await http.patch(
                f"{NOTION_API}/blocks/{page_id}/children",
                headers=self._headers(),
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
            "Method": {"select": {"name": data.get("method", "Payment")}},
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
        if "method" in updates:
            props["Method"] = {"select": {"name": updates["method"]}}
        if "notes" in updates:
            props["Notes"] = {"rich_text": [{"text": {"content": updates["notes"]}}]}
        if "liters" in updates:
            props["Liters"] = {"number": float(updates["liters"])}

        page = await self._update_page(entry_id, props)
        return self._parse_expense(page)

    async def archive_expense(self, entry_id: str) -> bool:
        return await self._archive_page(entry_id)

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
        """Expense entries in the 'Servicios' category for the month. Replaces query_servicios_mes()."""
        if not month:
            from datetime import timezone
            now = datetime.now(timezone.utc) - timedelta(hours=3)
            month = now.strftime("%Y-%m")

        year, mon = map(int, month.split("-"))
        last_day = monthrange(year, mon)[1]

        return await self.query_expenses(QueryFilter(
            date_range=DateRange(start=date(year, mon, 1), end=date(year, mon, last_day)),
            category="Servicios",
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
            "Source": {"select": {"name": data.get("source", "Matrics")}},
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

        page = await self._update_page(plant_id, props)
        return self._parse_plant(page)

    async def archive_plant(self, plant_id: str) -> bool:
        return await self._archive_page(plant_id)

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
            "Source": {"select": {"name": "Matrics"}},
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
            {"property": "Source", "select": {"equals": "Matrics"}},
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
            })
        return tasks

    async def create_factura_task(
        self, provider: str, amount: float, due_date: str, period: str
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
        meta = json.dumps({"provider": provider, "amount": amount, "period": period}, ensure_ascii=False)
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
            "source": "Matrics",
            "notes": meta,
            "due_date": due_date,
            "priority": priority,
        })
        return True, task.id

    async def mark_factura_task_paid(self, page_id: str) -> bool:
        """Mark a bill task as paid (Notion status: Listo)."""
        try:
            await self.update_task(page_id, {"status": "Listo"})
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
            "Source": {"select": {"name": "Matrics"}},
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

        config = UserConfig(
            phone=phone or _get_text(props, "WA Number"),
            greeting_name=_get_text(props, "Greeting Name") or None,
            daily_summary_hour=int(_get_number(props, "Resumen Hour")) if _get_number(props, "Resumen Hour") is not None else None,
            daily_summary_minute=int(_get_number(props, "Resumen Minute")) if _get_number(props, "Resumen Minute") is not None else None,
            resumen_nocturno_enabled=_get_checkbox(props, "Resumen Nocturno Enabled"),
            resumen_nocturno_hour=int(_get_number(props, "Resumen Nocturno Hour") or 22),
            resumen_extras=extras,
            news_topics=topics,
            service_providers=providers,
            known_places=known,
            saved_lat=_get_number(props, "Latitude"),
            saved_lon=_get_number(props, "Longitude"),
            saved_city=_get_text(props, "City") or None,
        )
        return config, page["id"]

    async def save_config(self, page_id: str, config: UserConfig) -> bool:
        """Persist user config. Requires the page_id returned by load_config."""
        if not page_id:
            return False

        extras_str = " | ".join(config.resumen_extras or [])
        topics_str = ", ".join(config.news_topics or [])
        props = {
            "Greeting Name": {"rich_text": [{"text": {"content": config.greeting_name or "Buenos dias"}}]},
            "Resumen Extras": {"rich_text": [{"text": {"content": extras_str}}]},
            "News Topics": {"rich_text": [{"text": {"content": topics_str}}]},
            "Service Providers": {"rich_text": [{"text": {"content": json.dumps(config.service_providers or {}, ensure_ascii=False)}}]},
            "Known Places": {"rich_text": [{"text": {"content": json.dumps(config.known_places or [], ensure_ascii=False)}}]},
            "Resumen Nocturno Enabled": {"checkbox": config.resumen_nocturno_enabled},
        }
        if config.daily_summary_hour is not None:
            props["Resumen Hour"] = {"number": config.daily_summary_hour}
            props["Resumen Minute"] = {"number": config.daily_summary_minute or 0}
        if config.resumen_nocturno_hour is not None:
            props["Resumen Nocturno Hour"] = {"number": config.resumen_nocturno_hour}

        try:
            await self._update_page(page_id, props)
            return True
        except Exception:
            return False

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
