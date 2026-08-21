import { isSupportedContentTimeVersion } from './content-time'

export function assessEngagementScienceEligibility(input: {
  contentTime: boolean
  explicitBounds: boolean
  contractVersion: string | null
  expectedContractVersion: string
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
      && Number.isFinite(input.minimumValidShare)
      && input.minimumValidShare > 0
      && validPopulation,
    ),
  }
}
