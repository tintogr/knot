# Matrics — Workspace Structure & App Interface Spec

## Purpose of this document

Reference spec for organizing Matrics' Notion workspace and guiding future development of the Matrics web app. Use this when adding new databases, reorganizing existing ones, or building user-facing views.

---

## Core principle

The workspace is organized around **what the user is doing**, not what type of data it is. The 5 areas below are the "rooms" the user walks into — each one has a clear purpose that's obvious without explanation.

---

## The 5 user-facing areas

### 1. Finanzas

**Purpose**: Everything related to money.

| Database | Description | Key properties |
|---|---|---|
| **Finances** | Every expense and income entry. The core financial DB. | Name, Value (ars), In-Out (INGRESO/EGRESO), Category (multi_select), Method (Payment/Suscription), Date, Client, Exchange Rate, Liters, Notes, emoji icon |
| **Servicios** | NOT a separate DB. It's a filtered view of Finances where Category = "Servicios". Shows recurring payments (electricity, internet, gas, etc.) with paid/pending status. | Same as Finances |

**Interrelations**:
- When the user buys items from the Shopping list, the expense links to those items ("compré todo lo de la lista, me salió $45.000")
- Tasks can originate from Finanzas (payment reminders: "pagar CALFIBRA antes del 15")
- Documents area stores related files (invoices, AFIP certificates, contracts)

**Expense categories** (current, may expand): Supermercado, Sueldo, Servicios, Transporte, Vianda, Salud, Salud Mental, Salida, Birra, Ocio, Compras, Depto, Plantas, Viajes, Venta.

**Clients**: LBL, OPERA, ALPATACO, Juan Martin, Depto, Work, Santi Vales, Jorge, Barbara, Vanguardia, Alejo, Dinamo, Paula Diaz, Labti, PlanA, JGA, ATE.

---

### 2. Agenda

**Purpose**: Everything with a date, deadline, or location trigger.

| Database | Description | Key properties |
|---|---|---|
| **Tasks** | Transversal task list. Receives tasks from ALL areas. Each task has a tag indicating its origin (Finanzas, Proyecto, Agenda, Colección). | Name, Category (select: Finanzas/Proyecto/Agenda/etc.), Status (status: Sin empezar/En progreso/Listo), Priority (Alta/Media/Baja), Due Date, Source (Matrics/Manual), Notes |
| **Meetings** | Meeting notes. Can be linked to a Project or standalone. | Name, With (who attended), Date, Notes, Calendar Link, Source |
| **Geo-reminders** | Location-based reminders. Triggered when user enters a geographic area. | Name, Type (place/shop), Shop Name, Latitude, Longitude, Radius, Recurrent (bool), Active (bool) |
| **Calendar** | NOT a Notion DB. This is Google Calendar, accessed via API. Lives here conceptually because it's about scheduling. | — |

**Key design decision**: Tasks is a single unified DB, not one per area. A task tagged "Finanzas" (pay electricity bill) and a task tagged "Proyecto" (send renders to client) live in the same table. The tag tells the user where it came from. This avoids scattering tasks across multiple databases.

---

### 3. Proyectos

**Purpose**: Things with a goal and an end. Each project is a "context magnet" — once created, Matrics automatically detects related messages and accumulates data there.

| Database | Description | Key properties |
|---|---|---|
| **Projects** | Each project = one page that accumulates notes, links, tasks, and references over time. | Name, Entry Type (Proyecto/Idea/Reunión), Area (Laboral/Personal/Hobby/Educación), Status (status: Sin empezar/En progreso/Listo/Pausado), Priority, Description, Date, Source, emoji icon |

**Grouped by "troncos" (life area branches)**:

When the user opens Proyectos, they don't see a flat list of everything mixed together. Projects are visually grouped by area:

- **Educación**: courses, workshops, certifications ("Curso iluminación", "Workshop SketchUp")
- **Trabajo**: professional projects, freelance, client work ("Freelance portfolio", "Renders LBL")
- **Personal / Hobby**: personal ideas, side projects ("App ruta arquitectónica", "Organizar viaje Roma-Estambul")

This grouping prevents a class note from appearing next to "quiero comer más sano" — context stays clean.

**Context magnet behavior**:
1. User mentions a topic casually → Matrics saves it as a loose note
2. User creates a project ("creame un proyecto Ducato") → project is created
3. From that point on, Matrics detects messages related to "Ducato" and files them under that project automatically
4. Matrics can proactively suggest creating a project when it detects 2+ notes about the same topic ("Tenés 3 notas sobre la Ducato y 2 sobre la Ecosport — ¿querés que arme un proyecto Vehículos?")
5. Matrics can suggest grouping related projects under a parent project ("Ducato + Ecosport → Vehículos", where each vehicle has its own service history, documentation, photos, etc.)

**What goes inside a project** (as sub-content, not separate DBs):
- Notes (linked from the universal Notes DB)
- Tasks (linked from Tasks with tag "Proyecto")
- Links and references
- Photos and files
- Timeline of activity

---

### 4. Colecciones

**Purpose**: Things that grow indefinitely — they never "finish". The user's personal notebook.

| Database | Description | Key properties |
|---|---|---|
| **Plants** | Plant registry. Each plant the user owns. | Name, Species, Light, Watering, Location, Status, Purchase Date, Price, Notes, emoji icon |
| **Recipes** | Recipe collection. Linked to Shopping via ingredient relations. | Name, Source, Difficulty, Type (multi_select: Postre/Cena/Almuerzo/etc.), Cooking method, Healthy rating, Ingredients (relation to Shopping), formatted recipe text as page content |
| **Shopping** | Shopping list. Items can be in stock or missing. | Name, Stock (checkbox), Category, Store (multi_select), Frequency (status), Notes, emoji icon |

**Interrelations**:
- Recipes ↔ Shopping: a recipe's ingredients are items in the Shopping list (Notion relation). Creating a recipe can auto-add missing ingredients to Shopping.
- Shopping → Finanzas: "compré todo lo de la lista, me salió $X" creates an expense entry linked to the items.
- Plants can generate Tasks: "trasplantar potus esta semana"

**Shopping categories**: Frutas y verduras, Enlatado, Infusion, Lacteo, Especias, Limpieza, Panificado, Herramienta, Construccion, Higiene, Electronica, Carne, Galletitas, Alcohol, Bebida, Fiambre, Grano, Comida, Cosmetica.

**Shopping stores**: Super, Panaderia, Verduleria, Dietetica, Farmacia, Drogueria, Ferreteria.

**This area can grow** — future collections might include: books/reading list, movies/series watchlist, music, contacts/people, etc. Any "living list" that the user maintains over time belongs here.

---

### 5. Documentos

**Purpose**: Vault for important information, files, and records that the user needs to store and retrieve.

| Database | Description | Key properties |
|---|---|---|
| **Medical** | Health records: lab results, prescriptions, doctor visits, medical history. | Name, Date, Type (select: Análisis/Receta/Consulta/etc.), Doctor, Notes, Files |
| **Legal / Fiscal** | AFIP status, contracts, tax records, legal documents. | Name, Date, Type, Notes, Files |
| **Files** | General document storage. PDFs, images, important files that don't fit elsewhere. | Name, Date, Category, Source, Notes, File attachments |

**This area is new** — it doesn't exist in the current Notion workspace. It needs to be created. The user currently has no structured place to store a lab result photo or an AFIP certificate. Matrics can receive these via WhatsApp (photo of a document) and file them automatically by detecting the content.

**Linking**: Documents can be linked to any other area. An invoice links to Finanzas. A medical prescription links to a Tasks entry ("buy medication"). An AFIP certificate links to a Legal/Fiscal entry.

---

## Cross-cutting concepts

### Notes (universal database)

A single Notes DB that links to any area via Notion relations. This solves the "where does this note go?" problem:

- A note about potus watering → linked to the plant in Colecciones
- A note from class 7 of a course → linked to the project in Proyectos
- A loose note about something interesting → stays unlinked until the user creates a relevant project

Notes don't duplicate based on where they "belong" — they exist once and connect to whatever they're about.

**Properties**: Name, Content, Date, Linked Project (relation), Linked Collection item (relation), Tags, Source.

### Area of life (transversal property)

"Educación", "Trabajo", "Personal" are NOT sidebar sections — they're a property that items in multiple DBs can have. A meeting can be "Educación" (a class) or "Trabajo" (a client call). A project can be "Hobby" or "Laboral".

This enables cross-area queries: "mostrame todo lo de educación" pulls projects, meetings, notes, and tasks tagged with that area — regardless of which DB they live in.

### Suggested values for Area of life:
- Laboral (work, freelance, clients)
- Personal (home, health, daily life)
- Educación (courses, learning, workshops)
- Hobby (side projects, interests, exploration)

---

## System area (not user-facing)

| Database | Description |
|---|---|
| **Config** | Per-user preferences: greeting name, summary schedule, known places, service providers, news topics, location cache. Stored as Notion properties including JSON strings in rich_text fields. |
| **User Registry** (future) | Central user database for multi-user support. Maps phone → backend type, credentials, DB IDs. |
| **OAuth Tokens** (future) | Google OAuth refresh tokens per user. Currently hardcoded in env vars for single user. |

---

## Guidelines for adding new databases

When deciding where to put a new DB, ask these questions in order:

1. **Does it involve money?** → Finanzas
2. **Does it have dates, deadlines, or location triggers?** → Agenda
3. **Does it have a goal and will eventually end?** → Proyectos (as a project type or sub-content)
4. **Is it a "living list" that grows indefinitely?** → Colecciones
5. **Is it important information/files to store and retrieve?** → Documentos
6. **Is it system configuration?** → Sistema (not user-facing)

If it crosses boundaries (a course has dates AND is a project AND generates notes), the primary item goes where it makes most sense (Proyectos for a course), and it LINKS to other areas (events in Calendar, tasks in Agenda, notes in Notes DB).

**Rule**: Never create a standalone DB at the root level. Every DB belongs to one of the 5 areas. If none fit, it's either a new Collection in Colecciones, a new document type in Documentos, or a signal that the area structure needs revision.

---

## App interface vision (future)

The Matrics web app (PWA) would show these same 5 areas as the primary navigation. Each area renders its data in the most useful format:

- **Finanzas**: dashboard with monthly totals, category breakdown chart, trend lines, recent transactions list, servicios status (paid/pending)
- **Agenda**: calendar view (week/month), task list sorted by priority/due date, geo-reminders on a map
- **Proyectos**: card grid grouped by tronco (Educación/Trabajo/Personal), each card shows project name, status, note count, last activity
- **Colecciones**: list views with filters (shopping: missing items first; plants: by watering schedule; recipes: by type/difficulty)
- **Documentos**: file browser with category tabs, search, preview

The app reads the same data as the bot (via DataStore interface). Today that data lives in Notion. Tomorrow it could live in Supabase (Postgres) for better performance and richer queries. The DataStore abstraction makes this swap transparent.

---

## Current state of the Notion workspace

The current workspace is FLAT — all 9 databases at the same level with no grouping. The reorganization into 5 areas has been designed but NOT yet implemented. The order of operations is:

1. First: decouple code from Notion (NotionDataStore — in progress)
2. Then: reorganize the Notion workspace into the 5 areas
3. Later: add new DBs (Documentos area, Notes universal DB)
4. Eventually: Supabase backend + Matrics App

This document describes the TARGET state, not the current state.


=====

