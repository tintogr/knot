# 🤖 KNOT — Asistente Personal por WhatsApp

Bot personal que interpreta mensajes de texto e imágenes con Claude y los guarda automáticamente en Notion. Gestiona gastos, calendario, geo-reminders, salud, fitness, shopping, listas generativas y más.

---

## Stack

- **Python / FastAPI** — servidor webhook
- **Claude (Anthropic)** — clasificador + agentes especializados
- **Notion API** — base de datos para todo
- **WhatsApp Business API** (Meta Graph API)
- **Google Calendar API** — eventos y recordatorios
- **OwnTracks** — GPS en background → geo-reminders
- **Open-Meteo** — clima en el resumen diario
- **Deploy:** Render (free tier) + UptimeRobot para evitar cold starts

---

## Variables de entorno requeridas

| Variable | Descripción |
|---|---|
| `ANTHROPIC_API_KEY` | API key de Anthropic |
| `NOTION_TOKEN` | Token `secret_...` de la integración de Notion |
| `NOTION_DATABASE_ID` | ID de la DB de finanzas |
| `WHATSAPP_TOKEN` | Token de la app de Meta/WhatsApp |
| `WHATSAPP_PHONE_ID` | Phone number ID de WhatsApp Business |
| `MY_WA_NUMBER` | Número de WhatsApp del dueño (ej: `54298154894334`) |
| `GCAL_CLIENT_ID` | OAuth client ID de Google Calendar |
| `GCAL_CLIENT_SECRET` | OAuth client secret de Google |
| `GCAL_REFRESH_TOKEN` | Refresh token de Google Calendar |
| `GOOGLE_MAPS_API_KEY` | Para geo-reminders tipo "shop" |
| `OPENWEATHER_API_KEY` | Clima (opcional, fallback a Open-Meteo) |

Variables opcionales de Notion (tienen defaults hardcodeados):
`NOTION_PLANTS_DB_ID`, `NOTION_SHOPPING_DB_ID`, `NOTION_RECIPES_DB_ID`, `NOTION_MEETINGS_DB_ID`, `NOTION_TASKS_DB_ID`, `NOTION_GEO_REMINDERS_DB_ID`, `NOTION_CONFIG_DB_ID`, `NOTION_PROJECTS_DB_ID`, `NOTION_HEALTH_RECORDS_DB_ID`, `NOTION_MEDICATIONS_DB_ID`, `NOTION_FITNESS_DB_ID`, `NOTION_PAYMENT_METHODS_DB_ID`

---

## Archivos principales

| Archivo | Descripción |
|---|---|
| `main.py` | Núcleo: webhook FastAPI, clasificador, todos los handlers y tools |
| `notion_datastore.py` | Abstracción completa de Notion API |
| `summaries.py` | Resumen diario, nocturno y semanal |
| `config.py` | Carga/guardado de UserConfig + handle_configurar |
| `state.py` | Estado compartido: user_prefs, current_location, pending_state, constantes |
| `gcal.py` | Integración con Google Calendar |
| `wa_utils.py` | Helpers de envío de WhatsApp |

---

## Flujo de mensajes

```
POST /webhook
  → process_message() [background task]
    → message_buffer (4s window, agrupa mensajes)
      → classify() [Sonnet — devuelve un tipo]
        → handler específico
```

**Tipos del clasificador:**
`GASTO`, `CORREGIR_GASTO`, `ELIMINAR_GASTO`, `DEUDA`, `PLANTA`, `EDITAR_PLANTA`, `ELIMINAR_PLANTA`, `EVENTO`, `EDITAR_EVENTO`, `ELIMINAR_EVENTO`, `RECORDATORIO`, `CANCELAR_RECORDATORIO`, `SHOPPING`, `CORREGIR_SHOPPING`, `ELIMINAR_SHOPPING`, `REUNION`, `EDITAR_REUNION`, `ELIMINAR_REUNION`, `SALUD`, `ACTIVIDAD_FISICA`, `GEO_REMINDER`, `CONFIGURAR`, `RESUMEN_DIARIO`, `LISTA`, `CHAT`

---

## Funcionalidades

- **Gastos e ingresos** — registra texto o foto de ticket/factura, deduce medio de pago por últimos 4 dígitos
- **Google Calendar** — crear, editar, eliminar eventos; eventos recurrentes; recordatorios anticipados
- **Geo-reminders** — se disparan al llegar a un lugar (por coordenadas fijas o búsqueda dinámica de comercio)
- **Shopping** — lista de compras con categorías, detección por proximidad al supermercado
- **Salud** — registra análisis, consultas, medicaciones
- **Fitness** — registra actividades, soporte para screenshots de Strava/Adidas/Nike
- **Listas generativas** — crea y gestiona listas en Notion (pelis, libros, viajes, etc.); Claude genera items automáticamente; auto-crea la DB de Notion
- **Facturas** — extrae de Gmail, crea tareas, detecta impagas
- **Resumen diario** — clima, calendario, facturas, emails importantes, extras configurables
- **Payment Methods** — DB de Notion con tarjetas y medios de pago (fuente única de verdad)
- **8 domain profiles** — contexto personalizado por área (gastos, salud, dieta, fitness, etc.)

---

## Endpoints

- `POST /webhook` — recibe mensajes de WhatsApp
- `POST /location` — recibe GPS de OwnTracks
- `GET /cron` — disparado por UptimeRobot; envía resúmenes según horarios configurados
- `GET /health` — health check

---

## Deploy en Render

1. Conectar el repo de GitHub en Render
2. Tipo: **Web Service**, runtime: Python
3. Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
4. Agregar todas las variables de entorno
5. Registrar el webhook de WhatsApp apuntando a `https://tu-app.onrender.com/webhook`
6. Configurar UptimeRobot para pingar `/health` cada 5 min (evita cold starts)
