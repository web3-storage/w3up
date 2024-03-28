import * as Server from '@ucanto/server'
import * as Blob from '@web3-storage/capabilities/blob'
import * as API from '../types.js'
import { BlobItemNotFound } from './lib.js'

/**
 * @param {API.W3ServiceContext} context
 * @returns {API.ServiceMethod<API.BlobAccept, API.BlobAcceptSuccess, API.BlobAcceptFailure>}
 */
export function blobAcceptProvider(context) {
  return Server.provide(Blob.accept, async ({ capability }) => {
    const { blob } = capability.nb
    // If blob is not stored, we must fail
    const hasBlob = await context.blobStorage.has(blob.content)
    if (hasBlob.error) {
      return {
        error: new BlobItemNotFound(),
      }
    }

    // TODO: location claim

    return {
      error: new BlobItemNotFound(),
    }
  })
}
