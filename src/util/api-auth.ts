import express from 'express'

export type ApiKeyAuthConfig = {
  primaryEnv: string[]
}

function parseHeaderValue(value: unknown): string | undefined {
  if (typeof value === 'string') return value
  if (Array.isArray(value) && typeof value[0] === 'string') return value[0]
  return undefined
}

function configuredKeys(envNames: string[]): Set<string> {
  const keys = new Set<string>()
  for (const envName of envNames) {
    const value = process.env[envName]?.trim()
    if (value) keys.add(value)
  }
  return keys
}

export function getConfiguredApiKeys(config: ApiKeyAuthConfig): Set<string> {
  return configuredKeys(config.primaryEnv)
}

export function hasConfiguredApiKey(config: ApiKeyAuthConfig): boolean {
  return getConfiguredApiKeys(config).size > 0
}

/** Return only the configured key's environment-name class, never key material. */
export function getAuthorizedApiKeyEnv(
  req: express.Request,
  config: ApiKeyAuthConfig,
): string | null {
  const presented = parseHeaderValue(req.headers['api-key'])?.trim()
  if (!presented) return null

  for (const envName of config.primaryEnv) {
    const configured = process.env[envName]?.trim()
    if (configured && configured === presented) return envName
  }
  return null
}

export function isApiKeyAuthorized(
  req: express.Request,
  config: ApiKeyAuthConfig,
): boolean {
  return getAuthorizedApiKeyEnv(req, config) !== null
}

export function logUnauthorized(endpoint: string) {
  console.log(
    `[${new Date().toISOString()}] - Attempted unauthorized access to ${endpoint}`,
  )
}
