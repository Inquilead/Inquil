-- Run once if you already had simulation_runs.user_id as uuid referencing auth.users.
-- After this, the API can store string user ids from public."User" (e.g. "12").

alter table public.simulation_runs drop constraint if exists simulation_runs_user_id_fkey;

alter table public.simulation_runs
  alter column user_id type text using user_id::text;

drop policy if exists "simulation_runs_select_own" on public.simulation_runs;
create policy "simulation_runs_select_own"
  on public.simulation_runs for select
  to authenticated
  using (auth.uid()::text = user_id);
