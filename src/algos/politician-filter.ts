import { sql } from 'kysely'

type AnySelect = any

// Feeds routed through the BE politician-or-party eligibility filter: the K/M
// study feeds only. newsflow-be-1/2/3 are fully decommissioned (PRD.md D2,
// 2026-07-25) — they no longer route through this filter and, once their
// catalog rows are removed, fall through to feedgen's standard unknown/
// disabled-feed handling like any other unregistered rkey.
const BE_FILTER_RKEY = /^newsflow-be-(k|m)$/

// Kill-switch: an explicit falsy value disables the filter entirely (declared
// degraded state — BE feeds then serve UNFILTERED). Default (unset) is enabled.
const KILL_SWITCH_OFF = ['false', '0', 'no', 'off']

// Exported for the config-activation manifest (src/util/config-manifest.ts) —
// the shared resolver, not a re-parse of FEEDGEN_BE_POLITICIAN_FILTER.
export function killSwitchDisabled(): boolean {
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
    ? 'politician-filter: DISABLED via FEEDGEN_BE_POLITICIAN_FILTER kill-switch — BE feeds (newsflow-be-{k,m}) serve UNFILTERED (declared degraded)'
    : 'politician-filter: ENABLED — BE feeds (newsflow-be-{k,m}) apply politician-or-party eligibility (ranker_prod.post_political_eligibility)'
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
