export type LinkFieldInput = {
  link_uri?: string | null
  link_title?: string | null
  link_description?: string | null
  linkUrl?: string | null
  linkTitle?: string | null
  linkDescription?: string | null
}

function reconcile(
  canonical: string | null | undefined,
  legacy: string | null | undefined,
  name: string,
): string {
  if (canonical != null && legacy != null && canonical !== legacy) {
    throw new Error(`conflicting ${name} values`)
  }
  return canonical ?? legacy ?? ''
}

export function dualWriteLinkFields(input: LinkFieldInput) {
  const link_uri = reconcile(input.link_uri, input.linkUrl, 'link_uri/linkUrl')
  const link_title = reconcile(input.link_title, input.linkTitle, 'link_title/linkTitle')
  const link_description = reconcile(
    input.link_description,
    input.linkDescription,
    'link_description/linkDescription',
  )

  return {
    link_uri,
    link_title,
    link_description,
    linkUrl: link_uri,
    linkTitle: link_title,
    linkDescription: link_description,
  }
}
