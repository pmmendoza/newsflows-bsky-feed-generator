import { sql } from 'kysely'

type AnySelect = any

function truthy(value: string | undefined): boolean {
  return ['true', '1', 'yes', 'on'].includes(String(value ?? '').trim().toLowerCase())
}

export function isPoliticianFilterEnabled(rkey: string): boolean {
  if (!/^newsflow-be-[123]$/.test(String(rkey || ''))) return false
  return truthy(process.env.FEEDGEN_BE_POLITICIAN_FILTER)
}

function freshnessHours(): number {
  const raw = process.env.FEEDGEN_BE_POLITICIAN_FILTER_MAX_AGE_HOURS
  const parsed = raw ? Number(raw) : 168
  if (!Number.isFinite(parsed) || parsed <= 0) return 168
  return parsed
}

export function politicianFilterFreshnessCutoffIso(now = new Date()): string {
  return new Date(now.getTime() - freshnessHours() * 60 * 60 * 1000).toISOString()
}

export function applyPoliticianFilterIfEnabled(query: AnySelect, rkey: string): AnySelect {
  if (!isPoliticianFilterEnabled(rkey)) return query
  const cutoffIso = politicianFilterFreshnessCutoffIso()
  return query
    .leftJoin('ranker_prod.post_politician as pp', (join: any) =>
      join
        .onRef('pp.post_uri', '=', 'post.uri')
        .on('pp.classified_at', '>=', cutoffIso),
    )
    .where((eb: any) =>
      eb.or([
        eb('pp.has_politician', '=', true),
        sql`"pp"."post_uri" is null`,
      ]),
    )
}
