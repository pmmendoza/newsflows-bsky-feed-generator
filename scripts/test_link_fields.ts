import assert from 'assert'
import { dualWriteLinkFields } from '../src/util/link-fields'

const canonical = dualWriteLinkFields({
  link_uri: 'https://example.com/a',
  link_title: 'A\u0000title',
  link_description: '',
})
assert.deepEqual(canonical, {
  link_uri: 'https://example.com/a',
  link_title: 'A\u0000title',
  link_description: '',
  linkUrl: 'https://example.com/a',
  linkTitle: 'A\u0000title',
  linkDescription: '',
})

assert.deepEqual(
  dualWriteLinkFields({
    linkUrl: 'legacy-uri',
    linkTitle: 'legacy-title',
    linkDescription: 'legacy-description',
  }),
  {
    link_uri: 'legacy-uri',
    link_title: 'legacy-title',
    link_description: 'legacy-description',
    linkUrl: 'legacy-uri',
    linkTitle: 'legacy-title',
    linkDescription: 'legacy-description',
  },
)

assert.throws(
  () => dualWriteLinkFields({ link_uri: 'canonical', linkUrl: 'legacy' }),
  /conflicting link_uri\/linkUrl values/,
)

console.log('link field compatibility tests passed')
