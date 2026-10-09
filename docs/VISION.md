# Knot — Visión, principios y lo que falta

> Reconstruido el 09/10/2026 a partir de las charlas de claude.ai (marzo–abril 2026, "Matrics 1–12",
> "MEMORY", "Comparación Memorae y Knot") más lo hablado en octubre. Los documentos originales están en
> [`docs/historia/`](historia/): **[WORKSPACE_SPEC](historia/2026-04-25_WORKSPACE_SPEC.md)** (las 5 áreas,
> el diseño más completo), su [diagrama](historia/2026-04-25_WORKSPACE_DIAGRAMA.html),
> [contexto y mercado](historia/2026-04-16_KNOT_CONTEXTO_Y_MERCADO.md) y la
> [arquitectura multiusuario](historia/2026-04-09_ARCHITECTURE_BLUEPRINT.md).
> Nombre: Matrics hasta el 15/04/2026, después **Knot**.

## Qué es (en palabras de Martin)
Un lugar donde vive **toda la información de su vida**, manejado por chat (WhatsApp) para eliminar la
fricción de abrir apps y cargar datos. Tres etapas: **Captura** ("compré pan 1500" → dato) →
**Memoria** ("¿cuándo regué los potus?", "¿cómo evolucionó mi colesterol?") → **Proactividad**
(avisos útiles sin ser pesado). Notion es *una* de las formas de ver los datos; a futuro, una app
propia de Knot (y quizás Supabase) — Knot no termina en Notion.
Prioridad original (19/03): 1 Finanzas · 2 Hábitos & Objetivos · 3 Calendario & Eventos · 4 Salud & Bienestar.

## Principios (no negociables, repetidos muchas veces)
1. **La IA decide leyendo el mensaje, nunca por palabras clave.** Keywords solo de respaldo.
   "Que Knot piense: ¿qué puedo hacer yo para saber esto?" (mirar el mail, Notion, el calendario).
2. **Ningún dato del usuario en el código.** Todo vive en su configuración (Notion), para que sirva a
   cualquier usuario (otro usa Edersa, no CALF; no todos viven en Neuquén).
3. **Inferir y confirmar**: lo que Knot deduce (quién es la empresa de luz, a qué factura corresponde un
   pago) lo confirma con el usuario la primera vez y lo guarda.
4. **Preguntar si falta algo o hay ambigüedad** ("¿la del dentista o la de tu mamá?"), nunca inventar,
   nunca decir que hizo algo que no hizo, nunca negar capacidades que tiene.
5. **Respuestas generadas por IA según contexto**; mensajes fijos solo para errores. Conciso, sin jerga,
   mensajes cortos ("un mensaje largo es de lo peor").
6. **Presente pero no pesado**: proactivo con cosas útiles ("este mes no subiste el pago de la luz"),
   sin spam.
7. **Fechas exactas**: errar un día invalida todo.
8. **Configurable desde el chat** (horarios, qué incluye cada resumen) y persistente.
9. **Interno en inglés** (bases, propiedades, etiquetas); nombres, descripciones e interfaz en español.
10. **Íconos como lenguaje**: el emoji va como ícono de la página (no en el nombre), consistente por tipo.
11. **Seguridad y simplicidad** para cuando haya más usuarios: alta = loguearse y dar permisos, nada de tokens.

## El workspace objetivo (diseño del 25/04, nunca implementado)
Organizado por **lo que el usuario hace**, en 5 áreas + sistema:
- **Finanzas** — Finances (+ vista Servicios), relación con Shopping ("compré lo de la lista, $X") y Tasks (pagos).
- **Agenda** — Tasks (una sola, transversal, con origen), Meetings, Geo-reminders, Google Calendar.
- **Proyectos** — Projects como "imanes de contexto": una vez creado, Knot archiva ahí lo relacionado;
  sugiere crear uno cuando hay 2+ notas del mismo tema ("Ducato + Ecosport → Vehículos"); agrupados
  por tronco (Educación / Trabajo / Personal-Hobby).
- **Colecciones** — listas que crecen siempre: Plants, Recipes (también no-cocina: cosmética), Shopping;
  a futuro libros, pelis/series, música, **contactos/personas**.
- **Documentos** — **Medical** (análisis, recetas, consultas), **Legal/Fiscal** (AFIP, IIBB, contratos),
  **Files**. Knot recibe la foto/PDF por WhatsApp y lo archiva solo.
- Transversales: **Notes universal** (una nota existe una vez y se enlaza a lo que sea) y la propiedad
  **Area of life** (Laboral / Personal / Educación / Hobby).
- Sistema: Config (hoy "Knot Config"), User Registry y OAuth (futuro).
Regla: ninguna base suelta en la raíz; cada una pertenece a un área.

## Estado al 09/10/2026
**Hecho:** WhatsApp (texto, fotos, PDF, audio), agente de entrada que entiende contexto (sept), gastos con
emojis y categorías, facturas del mail con PDF y reconciliación, una factura = un registro, débitos
automáticos, servicios recurrentes, préstamos y deudas, Calendar (crear/editar/borrar), recordatorios con
posponer, Resumen Diario y nocturno configurables, clima, Gmail, búsqueda web, geo-recordatorios,
Shopping, Recipes, Plants, Meetings, Projects, configuración en Notion, personas.

**A medias:** "nada de datos en el código" (quedan `PERSONAS_INICIALES` y referencias a Neuquén);
perfiles automáticos poco confiables (reemplazar por datos verificables + cuestionario); Tasks solo
para facturas (falta la to-do general y la sync con Google Tasks); aprender de correcciones.

**Pendiente (diseñado, no hecho):**
1. Reorganizar Notion en las 5 áreas + pasar lo interno a inglés.
2. **Documentos** (Medical, Legal/Fiscal, Files) → base del historial de salud que pidió en octubre.
3. **Notes universal** + proyectos como imanes de contexto + Area of life.
4. **Contactos/Personas** como colección, enlazados a lugares de Google Maps ("la casa de Juampi").
5. **Hábitos & Objetivos** (era la prioridad #2) y digitalizar su hoja de sueño a mano desde una foto.
6. Ayuda natural ("¿qué podés hacer?"), confirmación antes de borrar, deshacer.
7. Multiusuario: onboarding por login, un solo número, DataStore intercambiable (Notion/Supabase), app propia.
