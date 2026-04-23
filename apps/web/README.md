# apps/web

Next.js frontend for spesto.bg, deployed on Vercel.

Not yet scaffolded. Expected: Next.js (App Router), TypeScript, Tailwind. Talks to
`apps/api/` over HTTPS; never queries BigQuery or Cloud SQL directly.

Likely also hosts the internal match-review admin surface (authenticated routes) unless
it grows large enough to warrant its own app.
