import { sql } from 'kysely'

type AnySelect = any

// Feeds routed through the BE politician-or-party eligibility filter: the K/M
// study feeds plus the legacy BE-1/2/3 during overlap. Matching a disabled
// feed is inert by design (mission D6), so `[123]` stays in the pattern after
// retirement and needs no redeploy to remove.
const BE_FILTER_RKEY = /^newsflow-be-(k|m|[123])$/

// Kill-switch: an explicit falsy value disables the filter entirely (declared
// degraded state — BE feeds then serve UNFILTERED). Default (unset) is enabled.
const KILL_SWITCH_OFF = ['false', '0', 'no', 'off']

function killSwitchDisabled(): boolean {
  return KILL_SWITCH_OFF.includes(
    String(process.env.FEEDGEN_BE_POLITICIAN_FILTER ?? '').trim().toLowerCase(),
  )
}

/** True when the rkey is a BE feed the filter routes to (independent of kill-switch). */
export function isPoliticianFilterRouted(rkey: string): boolean {
  return BE_FILTER_RKEY.test(String(rkey || ''))
}

/** True when the filter is active: a routed BE feed AND the kill-switch is not off. */
export function isPoliticianFilterEnabled(rkey: string): boolean {
  if (!isPoliticianFilterRouted(rkey)) return false
  return !killSwitchDisabled()
}

/** One-line startup summary of the filter state (logged by index.ts at boot). */
export function politicianFilterStartupSummary(): string {
  return killSwitchDisabled()
    ? 'politician-filter: DISABLED via FEEDGEN_BE_POLITICIAN_FILTER kill-switch — BE feeds (newsflow-be-{k,m,1,2,3}) serve UNFILTERED (declared degraded)'
    : 'politician-filter: ENABLED — BE feeds (newsflow-be-{k,m,1,2,3}) apply politician-or-party eligibility (ranker_prod.post_political_eligibility)'
}

// ponytail: 5-min per-process throttle so the degraded state is visible on
// request paths without flooding logs (the filter runs twice per BE request).
let lastDegradedLogMs = 0
function logDegradedOnce(rkey: string): void {
  const now = Date.now()
  if (now - lastDegradedLogMs < 5 * 60 * 1000) return
  lastDegradedLogMs = now
  console.warn(
    `[${new Date().toISOString()}] - politician-filter: DEGRADED — FEEDGEN_BE_POLITICIAN_FILTER kill-switch off; BE feed ${rkey} served UNFILTERED (politician-or-party eligibility not applied)`,
  )
}

export function applyPoliticianFilterIfEnabled(query: AnySelect, rkey: string): AnySelect {
  if (!isPoliticianFilterRouted(rkey)) return query
  if (killSwitchDisabled()) {
    logDegradedOnce(rkey)
    return query
  }
  // Politician-OR-party eligibility. Fail-open: a post with no eligibility row
  // (surface missing/behind) is kept, exactly as the politician-only filter did.
  // Feedgen consumes only `uri` + `eligible` from the BSR-owned surface.
  return query
    .leftJoin('ranker_prod.post_political_eligibility as pe', 'pe.uri', 'post.uri')
    .where((eb: any) =>
      eb.or([
        eb('pe.eligible', '=', true),
        sql`"pe"."uri" is null`,
      ]),
    )
}
