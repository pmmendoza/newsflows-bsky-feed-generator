import { isSupportedContentTimeVersion } from './content-time'

export function assessEngagementScienceEligibility(input: {
  contentTime: boolean
  explicitBounds: boolean
  contractVersion: string | null
  expectedContractVersion: string
  transitionExpiresAt: string | null
  referenceMs: number
  minimumValidShare: number
  numerator: number
  denominator: number
  allowEmptyPopulation: boolean
}) {
  const emptyPopulation = input.allowEmptyPopulation && input.denominator === 0 && input.numerator === 0
  const observedValidShare = input.denominator > 0 ? input.numerator / input.denominator : null
  const validPopulation = emptyPopulation || (
    input.denominator > 0
    && observedValidShare !== null
    && observedValidShare >= input.minimumValidShare
  )
  const isSupported = isSupportedContentTimeVersion(input.contractVersion) && isSupportedContentTimeVersion(input.expectedContractVersion)
  return {
    emptyPopulation,
    observedValidShare,
    scienceEligible: Boolean(
      input.contentTime
      && input.explicitBounds
      && isSupported
      && input.contractVersion === input.expectedContractVersion
      // FT-FU-6: an ABSENT expiry means the content-time arrangement is permanent, so
      // the export stays science-eligible. A PRESENT expiry must still be well-formed
      // and unexpired -- a stale or corrupt deadline disqualifies exactly as before.
      && (input.transitionExpiresAt === null
          || (Number.isFinite(Date.parse(input.transitionExpiresAt))
              && Date.parse(input.transitionExpiresAt) > input.referenceMs))
      && Number.isFinite(input.minimumValidShare)
      && input.minimumValidShare > 0
      && validPopulation,
    ),
  }
}
