# Workspace

## Overview

pnpm workspace monorepo using TypeScript. Each package manages its own dependencies.

## Stack

- **Monorepo tool**: pnpm workspaces
- **Node.js version**: 24
- **Package manager**: pnpm
- **TypeScript version**: 5.9
- **API framework**: Express 5
- **Database**: PostgreSQL + Drizzle ORM
- **Validation**: Zod (`zod/v4`), `drizzle-zod`
- **API codegen**: Orval (from OpenAPI spec)
- **Build**: esbuild (CJS bundle)

## Structure

```text
artifacts-monorepo/
├── artifacts/              # Deployable applications
│   └── api-server/         # Express API server
├── lib/                    # Shared libraries
│   ├── api-spec/           # OpenAPI spec + Orval codegen config
│   ├── api-client-react/   # Generated React Query hooks
│   ├── api-zod/            # Generated Zod schemas from OpenAPI
│   └── db/                 # Drizzle ORM schema + DB connection
├── scripts/                # Utility scripts (single workspace package)
│   └── src/                # Individual .ts scripts, run via `pnpm --filter @workspace/scripts run <script>`
├── pnpm-workspace.yaml     # pnpm workspace (artifacts/*, lib/*, lib/integrations/*, scripts)
├── tsconfig.base.json      # Shared TS options (composite, bundler resolution, es2022)
├── tsconfig.json           # Root TS project references
└── package.json            # Root package with hoisted devDeps
```

## TypeScript & Composite Projects

Every package extends `tsconfig.base.json` which sets `composite: true`. The root `tsconfig.json` lists all packages as project references. This means:

- **Always typecheck from the root** — run `pnpm run typecheck` (which runs `tsc --build --emitDeclarationOnly`). This builds the full dependency graph so that cross-package imports resolve correctly. Running `tsc` inside a single package will fail if its dependencies haven't been built yet.
- **`emitDeclarationOnly`** — we only emit `.d.ts` files during typecheck; actual JS bundling is handled by esbuild/tsx/vite...etc, not `tsc`.
- **Project references** — when package A depends on package B, A's `tsconfig.json` must list B in its `references` array. `tsc --build` uses this to determine build order and skip up-to-date packages.

## Root Scripts

- `pnpm run build` — runs `typecheck` first, then recursively runs `build` in all packages that define it
- `pnpm run typecheck` — runs `tsc --build --emitDeclarationOnly` using project references

## Multi-OTA Hotel Scraper (Python/Streamlit)

Standalone Python app at `agoda-scraper/` supporting **Agoda**, **Trip.com**, and **Mytour.vn**.

- `app.py` — Streamlit UI: radio OTA selector (Agoda/Trip.com/Mytour), themed hero, input forms, preview table with filters, Excel/CSV download.
- `scraper.py` — Agoda GraphQL-based scraper. City search via `/graphql/search`, VND currency injection, 11 data columns including meal plan and landmarks.
- `scraper_tripcom.py` — Trip.com DOM-based scraper (no CAPTCHA). Scrapes `div.hotel-card` elements. Covers 30+ Vietnamese cities via hard-coded Trip.com city IDs.
- `scraper_mytour.py` — Mytour.vn scraper. Loads mytour.vn to capture live `apphash`, then calls `apis.tripi.vn/hotels/v3/hotels/availability` with hard-coded `provinceId` per city. Full pagination (up to 400+ hotels per city). Falls back to page-intercept mode for cities without a known province ID.
- `.streamlit/config.toml` — Streamlit server config (port 5000)
- `requirements.txt` — Python dependencies
- Run via workflow "artifacts/agoda-scraper-web: web": `cd agoda-scraper && streamlit run app.py --server.port 5000`
- **Traveloka**: NOT supported — blocked by Cloudflare visual CAPTCHA.

### Trip.com City IDs (Vietnam, countryId=111)
Hà Nội=286, HCM=301, Đà Nẵng=1356, Đà Lạt=5204, Nha Trang=1777, Phú Quốc=5649, Vũng Tàu=7529, Hội An=5206, Huế=5207, Hạ Long=5201

### Mytour.vn Province IDs (Tripi internal IDs)
Huế=1, Kiên Giang(Phú Quốc)=2, Hải Phòng=3, Bình Định(Quy Nhơn)=5, Quảng Ninh(Hạ Long)=10, Hà Nội=11, Bà Rịa-Vũng Tàu=15, Lâm Đồng(Đà Lạt)=20, Lào Cai(Sa Pa)=21, Bình Thuận(Mũi Né)=23, Quảng Nam(Hội An)=28, Hồ Chí Minh=33, Cần Thơ=38, Khánh Hòa(Nha Trang)=43, Đà Nẵng=50

### Notes
- Mytour apphash rotates per session — scraped live from browser page load each time.
- The tripi.vn location-suggest API is IP-blocked from cloud IPs — hence hard-coded province IDs.
- OTA selector uses st.radio() (not st.button) for reliability in Streamlit iframes.

## Packages

### `artifacts/api-server` (`@workspace/api-server`)

Express 5 API server. Routes live in `src/routes/` and use `@workspace/api-zod` for request and response validation and `@workspace/db` for persistence.

- Entry: `src/index.ts` — reads `PORT`, starts Express
- App setup: `src/app.ts` — mounts CORS, JSON/urlencoded parsing, routes at `/api`
- Routes: `src/routes/index.ts` mounts sub-routers; `src/routes/health.ts` exposes `GET /health` (full path: `/api/health`)
- Depends on: `@workspace/db`, `@workspace/api-zod`
- `pnpm --filter @workspace/api-server run dev` — run the dev server
- `pnpm --filter @workspace/api-server run build` — production esbuild bundle (`dist/index.cjs`)
- Build bundles an allowlist of deps (express, cors, pg, drizzle-orm, zod, etc.) and externalizes the rest

### `lib/db` (`@workspace/db`)

Database layer using Drizzle ORM with PostgreSQL. Exports a Drizzle client instance and schema models.

- `src/index.ts` — creates a `Pool` + Drizzle instance, exports schema
- `src/schema/index.ts` — barrel re-export of all models
- `src/schema/<modelname>.ts` — table definitions with `drizzle-zod` insert schemas (no models definitions exist right now)
- `drizzle.config.ts` — Drizzle Kit config (requires `DATABASE_URL`, automatically provided by Replit)
- Exports: `.` (pool, db, schema), `./schema` (schema only)

Production migrations are handled by Replit when publishing. In development, we just use `pnpm --filter @workspace/db run push`, and we fallback to `pnpm --filter @workspace/db run push-force`.

### `lib/api-spec` (`@workspace/api-spec`)

Owns the OpenAPI 3.1 spec (`openapi.yaml`) and the Orval config (`orval.config.ts`). Running codegen produces output into two sibling packages:

1. `lib/api-client-react/src/generated/` — React Query hooks + fetch client
2. `lib/api-zod/src/generated/` — Zod schemas

Run codegen: `pnpm --filter @workspace/api-spec run codegen`

### `lib/api-zod` (`@workspace/api-zod`)

Generated Zod schemas from the OpenAPI spec (e.g. `HealthCheckResponse`). Used by `api-server` for response validation.

### `lib/api-client-react` (`@workspace/api-client-react`)

Generated React Query hooks and fetch client from the OpenAPI spec (e.g. `useHealthCheck`, `healthCheck`).

### `scripts` (`@workspace/scripts`)

Utility scripts package. Each script is a `.ts` file in `src/` with a corresponding npm script in `package.json`. Run scripts via `pnpm --filter @workspace/scripts run <script>`. Scripts can import any workspace package (e.g., `@workspace/db`) by adding it as a dependency in `scripts/package.json`.
