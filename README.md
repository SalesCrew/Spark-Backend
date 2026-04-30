# Coke Spark Backend

## Local setup

1. Copy env template:
   - `cp .env.example .env`
2. Fill all values in `.env`:
   - `SUPABASE_URL`
   - `SUPABASE_ANON_KEY`
   - `SUPABASE_SERVICE_ROLE_KEY`
   - `DATABASE_URL`
   - `JWT_SECRET`
   - `PORT`
   - `CORS_ORIGIN`
3. Install and run:
   - `npm install`
   - `npm run dev`

## Drizzle database workflow (mandatory)

1. Edit schema:
   - `src/lib/schema.ts`
2. Generate SQL migration:
   - `npm run db:generate`
3. Push schema:
   - `npm run db:push`
4. Optional seed:
   - `npm run db:seed`

### Migration hygiene checklist

Before running `db:push`, always verify:

- Generated SQL contains only intended table/index changes for the current task.
- No unrelated `ALTER TABLE` statements were introduced by snapshot drift.
- Identity columns (`standard_market_number`, `coke_master_number`, `flex_number`) keep `NULL` for missing values (never empty strings).

## Railway deployment

Set these Railway environment variables:

- `NODE_ENV=production`
- `PORT` (Railway provides this automatically)
- `CORS_ORIGIN` (frontend URL)
- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`
- `SUPABASE_SERVICE_ROLE_KEY`
- `DATABASE_URL`
- `JWT_SECRET`

Start command:

- `npm run start`

Build command:

- `npm run build`

## API endpoints

- `GET /health`
- `POST /auth/login`
- `GET /markets` (admin/gm/sm token required)
- `POST /admin/markets/import` (admin token required)
- `POST /admin/markets` (admin token required)
- `PATCH /admin/markets/:id` (admin token required)
- `PATCH /admin/markets/:id/delete` (admin token required)
- `GET /admin/users?role=gm|sm|admin` (admin token required)
- `POST /admin/users` (admin token required)
- `PATCH /admin/users/:id` (admin token required)
- `PATCH /admin/users/:id/deactivate` (admin token required)
