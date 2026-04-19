# Plan de implementación: Sistema unificado de facturas

**Spec:** `2026-04-19-facturas-historial-design.md`
**Orden:** los pasos dependen entre sí, ejecutar en secuencia.

---

## Paso 1 — Agregar campo `Estado` a la DB de Finanzas en Notion

Agregar via Notion API (una sola vez, no hay forma de hacerlo desde código de la app):
- Campo: `Estado`, tipo select, opciones: `Impaga`, `Pagada`
- El campo `Method` existente se reusa para medio de pago (ya existe en la DB)

**Archivos:** solo llamada a Notion MCP / API directa. No toca código Python.

---

## Paso 2 — Actualizar `EntryResult` y `_parse_expense` en notion_datastore.py

- Agregar `estado: str = None` al dataclass `EntryResult`
- En `_parse_expense`: leer `_get_select(props, "Estado")` y asignarlo
- En `create_expense`: si `data.get("estado")`, escribir `props["Estado"] = {"select": {"name": data["estado"]}}`
- El campo `method` existente se mapea a `Method` en Notion — ya funciona para BBVA/Mercado Pago/etc.

---

## Paso 3 — Nuevos métodos en NotionDataStore (notion_datastore.py)

### `create_finance_invoice(provider, amount, period, due_date, category)`
Crea entrada Impaga. Evita duplicados: si ya existe una entrada Impaga para el mismo proveedor + período, retorna `(False, "duplicate")`. Si no, llama a `create_expense` con `estado="Impaga"` y retorna `(True, page_id)`.

### `get_impaga_facturas(provider=None) -> list[EntryResult]`
Filtra la DB de Finanzas por `Estado = Impaga`. Si `provider` está especificado, agrega filtro `name contains provider`. Orden: fecha desc.

### `get_finance_history_by_provider(provider, limit=5) -> list[EntryResult]`
Filtra por `Estado = Pagada` + `name contains provider`. Orden: fecha desc. Retorna hasta `limit` entradas.

### `mark_finance_paid(page_id, paid_amount=None, payment_method=None, notes=None) -> bool`
Actualiza la entrada: `Estado = Pagada`. Si `paid_amount`, actualiza `Value (ars)`. Si `payment_method`, actualiza `Method`. Si `notes`, concatena a Notes existente (no reemplaza). Retorna True/False.

---

## Paso 4 — Actualizar `create_factura_task` en notion_datastore.py

- Aceptar parámetro opcional `finance_page_id: str = None`
- Almacenarlo en el JSON de Notes de la Task junto con los otros campos (`provider`, `amount`, `period`, etc.)
- `get_pending_factura_tasks` ya lee Notes como JSON: agregar `finance_page_id` al dict retornado

---

## Paso 5 — `payment_methods` en user_prefs (main.py)

- En `load_user_config`: si no existe `payment_methods` en prefs, setear default `["BBVA", "Mercado Pago", "Efectivo", "Transferencia", "Débito", "Crédito", "Contado"]`
- El usuario puede modificarlos via chat (ya hay flujo de configuración, agregar a ese handler)

---

## Paso 6 — Agregar campo `Estado` a Notion DB via API (main.py helper)

Función `ensure_finances_db_fields()` que corre en startup y agrega el campo `Estado` si no existe:
```python
await _ds.ensure_db_select_field("finances", "Estado", ["Impaga", "Pagada"])
```
Agregar método correspondiente en `notion_datastore.py`. Así el campo se crea automáticamente en el primer deploy, sin intervención manual.

---

## Paso 7 — Actualizar detección de facturas en `send_daily_summary` (main.py)

Reemplazar la sección actual (líneas ~5136–5223) por:

1. `gmail_invoices = await extract_gmail_invoices()` — nueva función que usa Claude para extraer lista estructurada de facturas del Gmail (proveedor, monto, período). Separa la extracción de la decisión.
2. Para cada factura extraída:
   - `impagas = await _ds.get_impaga_facturas(provider=factura.provider)` → si existe, skip (ya pendiente)
   - `historial = await _ds.get_finance_history_by_provider(factura.provider, limit=2)` → si hay pago reciente ±10%, skip (ya pagada)
   - Si pago reciente con diff >10%: agregar a lista de "dudosos" para preguntar al usuario
   - Si nada: `await _ds.create_finance_invoice(...)` + `await _ds.create_factura_task(..., finance_page_id=page_id)`
3. Construir mensaje del resumen a partir de `get_impaga_facturas()` directamente (sin cruce Claude)

---

## Paso 8 — Actualizar handler `marcar_factura_pagada` en main.py

El tool `marcar_factura_pagada` recibe `provider` y opcionalmente `paid_amount`, `payment_method`.

Nuevo flujo:
1. Buscar Task pendiente por provider (como hoy) → obtener `finance_page_id` del JSON de Notes
2. Si `finance_page_id` existe: buscar monto original de la entrada en Finanzas
3. Si `paid_amount` difiere >10% del monto original: setear `pending_state[phone] = {type: "factura_note", finance_page_id, task_page_id, paid_amount, payment_method}` y preguntar por nota
4. Si no difiere o no hay paid_amount: llamar `mark_finance_paid(finance_page_id, ...)` + `mark_factura_task_paid(task_page_id)`

Agregar handler para `pending_state type="factura_note"`:
- Recibe la nota del usuario → llama `mark_finance_paid` con la nota → cierra Task

---

## Paso 9 — Actualizar `handle_gasto` para detectar Impaga previa (main.py)

Cuando se registra un pago nuevo (después de crear la entrada en Finanzas):
1. Buscar si existe entrada Impaga con nombre similar al del gasto recién creado
2. Si existe con monto ±10%: llamar `mark_finance_paid` + cerrar Task. Informar: *"Encontré una deuda pendiente de Movistar — la marqué como pagada."*
3. Si existe con monto diff >10%: preguntar al usuario si corresponde al mismo pago

---

## Paso 10 — Deudas genéricas: extender agente GASTO (main.py)

En el system prompt del agente GASTO, agregar:

> Si el usuario dice que *debe* algo a alguien o quiere registrar algo como *pendiente de pago*, usa la tool `registrar_deuda` en lugar de `registrar_gasto`.

Agregar tool `registrar_deuda` al agente GASTO:
- Input: `name`, `amount`, `person_or_provider`, `notes`
- Handler: llama `_ds.create_finance_invoice(...)` con `estado="Impaga"` + `_ds.create_factura_task(...)`
- Output: confirma la deuda creada y la task generada

---

## Paso 11 — Tool `consultar_deudas` en agente de chat (main.py)

Agregar al array `tools` del `handle_chat`:
```json
{
  "name": "consultar_deudas",
  "description": "Lista deudas e facturas pendientes de pago. Usar cuando el usuario pregunta qué debe, qué facturas tiene impagas, cuánto le falta pagar, etc.",
  "input_schema": {
    "properties": {
      "provider": {"type": "string", "description": "Filtrar por proveedor o persona. Opcional."}
    }
  }
}
```
Handler: llama `get_impaga_facturas(provider)` → formatea lista.

---

## Paso 12 — Historial de pagos en agente de chat (main.py)

Agregar tool `historial_pagos_proveedor`:
- Input: `provider`
- Handler: llama `get_finance_history_by_provider(provider, limit=5)` → formatea con fecha, monto, medio, notas
- Descripción: "Consulta historial de pagos a un proveedor o persona. Usar cuando el usuario pregunta cuándo pagó algo, cuánto pagó, por qué pagó de más/menos, etc."

---

## Orden de ejecución recomendado

```
Paso 1+6 (Notion DB fields)
    → Paso 2 (EntryResult + _parse_expense)
    → Paso 3 (nuevos métodos datastore)
    → Paso 4 (create_factura_task con finance_page_id)
    → Paso 5 (payment_methods en user_prefs)
    → Paso 7 (send_daily_summary)
    → Paso 8 (marcar_factura_pagada)
    → Paso 9 (handle_gasto detecta Impaga)
    → Paso 10 (registrar_deuda)
    → Paso 11+12 (tools de chat)
```

## Estimación de riesgo por paso

| Paso | Riesgo | Motivo |
|------|--------|--------|
| 1+6 | Bajo | Solo agrega campo, no toca datos existentes |
| 2 | Bajo | Campo nuevo en dataclass, retrocompat por default None |
| 3 | Bajo | Métodos nuevos, no modifica existentes |
| 4 | Bajo | Parámetro opcional, retrocompat garantizada |
| 7 | **Alto** | Reemplaza lógica central del resumen diario |
| 8 | Medio | Cambia flujo de confirmación de pago existente |
| 9 | Medio | Lógica nueva en handle_gasto, puede crear falsos positivos |
| 10-12 | Bajo | Funcionalidad aditiva |
