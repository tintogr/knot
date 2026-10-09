# Knot — Contexto del proyecto

## Qué es Knot

Knot es un asistente personal de IA que opera desde WhatsApp. El usuario le habla por texto o voz y Knot ejecuta acciones reales: agenda en Google Calendar, registra gastos en Notion, lee facturas del Gmail, consulta el clima, busca en internet, dispara recordatorios por ubicación geográfica.

El motor de IA es Claude (Anthropic). La infraestructura corre en Railway. El backend hoy es Notion, pero la visión es que Notion sea una opción entre varias — el usuario podrá usar Google Sheets o una app propia de Knot que se construirá eventualmente.

**El producto no termina en Notion.** Notion es el backend más potente disponible hoy sin construir infraestructura propia, pero Knot no debe depender de él. La interfaz es el chat; el backend es un detalle de implementación.

---

## Estado actual — lo que existe y funciona

- Google Calendar completo: crear, editar, eliminar eventos en lenguaje natural
- Notion: registro de gastos e ingresos con categorías, reconciliación contra facturas de Gmail, configuración por usuario desde una DB de Notion
- Gmail: scraping automático de facturas y comprobantes, deduplicación
- Clima: datos ECMWF/GFS de alta resolución (no un widget básico)
- Búsqueda web
- Resumen diario y nocturno configurable por hora y contenido
- Geolocalización: recordatorios por proximidad geográfica vía OwnTracks
- Arquitectura tool-calling multi-ronda — puede encadenar varias acciones en un solo mensaje
- Usuario único por ahora — arquitectura multi-tenant en desarrollo

---

## Mercado — competidores analizados

### Memorae
Asistente de recordatorios 100% dentro de WhatsApp. 20.000+ usuarios. Precio: $3–$17/mes, con plan lifetime a $399.

**Qué hace bien:** frictionless total (agregar contacto y ya funciona), recordatorios únicos y recurrentes, sync Google/Outlook/Apple Calendar, listas, voz, 100+ idiomas, recordatorios a terceros.

**Qué no hace:** todo vive en WhatsApp — no construye ningún sistema externo. Sin Notion, sin Gmail, sin automatizaciones, sin memoria contextual, sin análisis de documentos. Techo bajo: el usuario que escala en complejidad lo abandona.

### Notis
Agente de IA tipo "empleado virtual". 17.000+ usuarios. Precio: $13–$99/mes (anual).

**Qué hace bien:** integración profunda con Notion (escribe en DBs con propiedades tipadas), RAG sobre el workspace propio (consulta tu Notion para responder con contexto), 800+ integraciones vía MCP, automatizaciones con webhooks y triggers, multicanal (WhatsApp + Telegram + Slack + iMessage + email), redacción de contenido (blogs, newsletters, posts, imágenes, videos vía OpenAI), análisis de documentos e imágenes, deep research, memoria a largo plazo automática.

**Qué no hace:** sin geolocalización, sin clima, sin finanzas LATAM, precio inaccesible para usuario casual, orientado a founders angloparlantes, requiere conocer Notion para sacarle valor.

**Conclusión del análisis:** Notis es la referencia de arquitectura y profundidad de features hacia la que Knot debería evolucionar. Pero el posicionamiento es distinto: Notis apunta a founders/operadores en mercados anglófonos. Knot tiene un nicho diferente: usuario de LATAM que ya usa Notion y Google Calendar, quiere su vida financiera registrada, y necesita un asistente que razone — no uno que solo parsee intenciones simples.

### El hueco de mercado donde entra Knot
Entre Memorae (demasiado simple) y Notis (demasiado caro y complejo): un asistente con profundidad real, precio accesible para LATAM, y diferenciadores que ningún competidor tiene — geolocalización, clima de alta resolución, finanzas personales con reconciliación de facturas, y motor Claude para razonamiento complejo.

**Riesgo de posicionamiento:** si Knot no comunica claramente que construye un sistema (no solo recuerda cosas), pierde contra Memorae en precio y contra Notis en features.

---

## Conceptos técnicos relevantes que se discutieron

### MCP (Model Context Protocol)
Estándar creado por Anthropic para que los modelos de IA se conecten a herramientas externas de forma uniforme. Cada app (Gmail, Notion, Google Calendar) publica un servidor MCP que expone sus funciones. El agente llama a esas funciones igual que llama a cualquier tool interna.

Knot hoy implementa sus integraciones a mano con código Python propio. La alternativa MCP permitiría conectar integraciones nuevas sin escribir código de integración, y haría el onboarding del usuario mucho más limpio (OAuth flow estándar en lugar de configuración manual de DBs).

**Conclusión práctica:** para las integraciones core actuales (Calendar, Notion, Gmail) conviene mantener el código propio porque hay control total y ya funciona. Para integraciones nuevas, MCP es el camino — no requiere código nuevo.

### Webhooks
Una URL que el sistema expone para recibir notificaciones de otros sistemas cuando pasa algo. Ejemplos útiles para Knot:
- Gmail avisa cuando llega un mail importante (vía Google Pub/Sub)
- Notion avisa cuando alguien modifica una DB
- Google Calendar avisa cuando se agrega o modifica un evento

Esto permitiría que Knot reaccione a eventos externos sin que el usuario escriba nada. Hoy las automatizaciones de Knot son todas cron (horario fijo) — con webhooks serían reactivas.

---

## Roadmap de features

### Próxima capa — antes de lanzar / primeros usuarios externos
- Múltiples usuarios: cada número de teléfono con su propia config y datos
- Onboarding guiado desde el propio chat: Knot pregunta qué querés conectar y da el link de OAuth
- Google Sheets como backend alternativo a Notion
- Webhooks entrantes: alerta por mail importante, cambio en DB de Notion
- Recordatorios inteligentes combinados: clima + calendar ("avisame 30 min antes si hay lluvia")
- Memoria explícita configurable: "recordá que no quiero reuniones antes de las 10"

### Visión — cuando el producto esté validado
- App nativa Knot: sin depender de WhatsApp Business API, sin limitaciones de Meta
- Geolocalización nativa en la app — sin OwnTracks ni apps de terceros
- Google Sheets, Notion y app Knot como backends intercambiables — el usuario elige
- Telegram como canal adicional (técnicamente trivial, mismo backend)
- Webhooks salientes: Knot avisa a otros sistemas cuando pasa algo
- Proactividad ampliada: Knot inicia conversaciones basadas en contexto, no solo responde
- Multi-idioma declarado: inglés, portugués para Brasil
- Widget o API de Knot para integraciones en otros contextos

### Características de producto no negociables (independientes de features)
- Respuesta en menos de 3 segundos — el usuario espera como si fuera una persona
- Sin falsos positivos en geolocalización — si avisa mal dos veces, el usuario lo desactiva para siempre
- Deduplicación correcta de gastos — si el mismo gasto llega por Gmail y por Notion, no duplicar
- Tono consistente — conciso, directo, sin formalidad innecesaria
- Configuración sin tocar código — todo desde Notion, Sheets o desde el propio chat
- Transparencia: si Knot no sabe algo o falla, lo dice sin inventar

---

## Deuda técnica y limitaciones actuales conocidas
- Canal único: solo WhatsApp
- Sin análisis de imágenes ni documentos PDF
- Sin deep research ni generación de contenido largo
- Sin soporte Outlook / Apple Calendar
- Sin automatizaciones reactivas (webhooks entrantes)
- Sin CRM (base de contactos con historial) — irrelevante por ahora
- Multi-tenant en desarrollo — hoy usuario único
