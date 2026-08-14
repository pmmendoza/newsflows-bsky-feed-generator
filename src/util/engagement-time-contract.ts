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
}) {
  const observedValidShare = input.denominator > 0 ? input.numerator / input.denominator : 0
  return {
    observedValidShare,
    scienceEligible: Boolean(
      input.contentTime
      && input.explicitBounds
      && input.contractVersion === input.expectedContractVersion
      && input.transitionExpiresAt !== null
      && Number.isFinite(Date.parse(input.transitionExpiresAt))
      && Date.parse(input.transitionExpiresAt) > input.referenceMs
      && Number.isFinite(input.minimumValidShare)
      && input.minimumValidShare > 0
      && input.denominator > 0
      && observedValidShare >= input.minimumValidShare,
    ),
  }
}
