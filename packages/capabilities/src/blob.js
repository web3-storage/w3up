/**
 * Blob Capabilities.
 *
 * Blob is a fixed size byte array addressed by the multihash.
 * Usually blobs are used to represent set of IPLD blocks at different byte ranges.
 *
 * These can be imported directly with:
 * ```js
 * import * as Blob from '@web3-storage/capabilities/blob'
 * ```
 *
 * @module
 */
import { capability, Link, Schema, ok, fail } from '@ucanto/validator'
import {
  equal,
  equalBlob,
  equalContent,
  equalWith,
  checkLink,
  SpaceDID,
  and,
} from './utils.js'

/**
 * Agent capabilities for Blob protocol
 */

/**
 * Capability can only be delegated (but not invoked) allowing audience to
 * derived any `blob/` prefixed capability for the (memory) space identified
 * by DID in the `with` field.
 */
export const blob = capability({
  can: 'blob/*',
  /**
   * DID of the (memory) space where Blob is intended to
   * be stored.
   */
  with: SpaceDID,
  derives: equalWith,
})

/**
 * Blob description for being ingested by the service.
 */
export const blobStruct = Schema.struct({
  /**
   * A multihash digest of the blob payload bytes, uniquely identifying blob.
   */
  content: Schema.bytes(),
  /**
   * Size of the Blob file to be stored. Service will provision write target
   * for this exact size. Attempt to write a larger Blob file will fail.
   */
  size: Schema.integer(),
})

/**
 * `blob/add` capability allows agent to store a Blob into a (memory) space
 * identified by did:key in the `with` field. Agent must precompute Blob locally
 * and provide it's multihash and size using `nb.content` and `nb.size` fields, allowing
 * a service to provision a write location for the agent to PUT or POST desired
 * Blob into.
 */
export const add = capability({
  can: 'blob/add',
  /**
   * DID of the (memory) space where Blob is intended to
   * be stored.
   */
  with: SpaceDID,
  nb: Schema.struct({
    /**
     * Blob to allocate on the space.
     */
    blob: blobStruct,
  }),
  derives: equalBlob,
})

/**
 * `blob/remove` capability can be used to remove the stored Blob from the (memory)
 * space identified by `with` field.
 */
export const remove = capability({
  can: 'blob/remove',
  /**
   * DID of the (memory) space where Blob is intended to
   * be stored.
   */
  with: SpaceDID,
  nb: Schema.struct({
    /**
     * A multihash digest of the blob payload bytes, uniquely identifying blob.
     */
    content: Schema.bytes(),
  }),
  derives: equalContent,
})

/**
 * `blob/list` capability can be invoked to request a list of stored Blobs in the
 * (memory) space identified by `with` field.
 */
export const list = capability({
  can: 'blob/list',
  /**
   * DID of the (memory) space where Blob is intended to
   * be stored.
   */
  with: SpaceDID,
  nb: Schema.struct({
    /**
     * A pointer that can be moved back and forth on the list.
     * It can be used to paginate a list for instance.
     */
    cursor: Schema.string().optional(),
    /**
     * Maximum number of items per page.
     */
    size: Schema.integer().optional(),
    /**
     * If true, return page of results preceding cursor. Defaults to false.
     */
    pre: Schema.boolean().optional(),
  }),
  derives: (claimed, delegated) => {
    if (claimed.with !== delegated.with) {
      return fail(
        `Expected 'with: "${delegated.with}"' instead got '${claimed.with}'`
      )
    }
    return ok({})
  },
})

/**
 * Service capabilities for Blob protocol
 */
/**
 * Capability can only be delegated (but not invoked) allowing audience to
 * derived any `web3.storage/blob/` prefixed capability for the (memory) space identified
 * by DID in the `with` field.
 */
export const serviceBlob = capability({
  can: 'web3.storage/blob/*',
  /**
   * DID of the (memory) space where Blob is intended to
   * be stored.
   */
  with: SpaceDID,
  derives: equalWith,
})

/**
 * `web3.storage/blob//allocate` capability can be invoked to create a memory
 * address where blob content can be written via HTTP PUT request.
 */
export const allocate = capability({
  can: 'web3.storage/blob/allocate',
  /**
   * DID of the (memory) space where Blob is intended to
   * be stored.
   */
  with: SpaceDID,
  nb: Schema.struct({
    /**
     * Blob to allocate on the space.
     */
    blob: blobStruct,
    /**
     * The Link for an Add Blob task, that caused an allocation
     */
    cause: Link,
    /**
     * DID of the user space where allocation takes place
     */
    space: SpaceDID,
  }),
  derives: (claim, from) => {
    return (
      and(equalWith(claim, from)) ||
      and(equalBlob(claim, from)) ||
      and(checkLink(claim.nb.cause, from.nb.cause, 'cause')) ||
      and(equal(claim.nb.space, from.nb.space, 'space')) ||
      ok({})
    )
  },
})

/**
 * `blob/accept` capability invocation should either succeed when content is
 * delivered on allocated address or fail if no content is allocation expires
 * without content being delivered.
 */
export const accept = capability({
  can: 'web3.storage/blob/accept',
  /**
   * DID of the (memory) space where Blob is intended to
   * be stored.
   */
  with: SpaceDID,
  nb: Schema.struct({
    /**
     * Blob to accept.
     */
    blob: blobStruct,
    /**
     * Expiration..
     */
    exp: Schema.integer(),
  }),
  derives: (claim, from) => {
    const result = equalBlob(claim, from)
    if (result.error) {
      return result
    } else if (claim.nb.exp !== undefined && from.nb.exp !== undefined) {
      return claim.nb.exp > from.nb.exp
        ? fail(`exp constraint violation: ${claim.nb.exp} > ${from.nb.exp}`)
        : ok({})
    } else {
      return ok({})
    }
  },
})

// ⚠️ We export imports here so they are not omitted in generated typedes
// @see https://github.com/microsoft/TypeScript/issues/51548
export { Schema, Link }
