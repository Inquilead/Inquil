-- Legacy / optional: only if you still call public."User" from the browser with the anon key.
-- Current MiroFish flow: login & register go through POST /api/auth/* (service role on the server),
-- so you normally do NOT need to grant anon access to "User".

-- grant usage on schema public to anon, authenticated;
-- grant select, insert on table public."User" to anon;
