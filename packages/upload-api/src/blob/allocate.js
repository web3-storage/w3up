import * as Server from '@ucanto/server'
import * as Blob from '@web3-storage/capabilities/blob'
import * as API from '../types.js'
import { BlobItemNotFound } from './lib.js'
import { ensureRateLimitAbove } from '../utils/rate-limits.js'

/**
 * @param {API.W3ServiceContext} context
 * @returns {API.ServiceMethod<API.BlobAllocate, API.BlobAllocateSuccess, API.BlobAllocateFailure>}
 */
export function blobAllocateProvider(context) {
  return Server.provide(Blob.allocate, async ({ capability, invocation }) => {
    const { blob, cause, space } = capability.nb

    // Rate limiting validation
    const rateLimitResult = await ensureRateLimitAbove(
      context.rateLimitsStorage,
      [space],
      0
    )
    if (rateLimitResult.error) {
      return {
        error: {
          name: 'InsufficientStorage',
          message: `${space} is blocked`,
        },
      }
    }

    // Has Storage provider validation
    const result = await context.provisionsStorage.hasStorageProvider(space)
    if (result.error) {
      return {
        /** @type {API.AllocationError} */
        error: {
          name: 'InsufficientStorage',
          message: `${space} has no storage provider`,
        },
      }
    }

    // If blob is stored, we can just allocate it to the space
    const hasBlob = await context.blobStorage.has(blob.content)
    if (hasBlob.error) {
      return {
        error: new BlobItemNotFound(space),
      }
    }
    // Get presigned URL for the write target
    const createUploadUrl = await context.blobStorage.createUploadUrl(
      blob.content,
      blob.size
    )
    if (createUploadUrl.error) {
      return {
        error: new Server.Failure('failed to provide presigned url'),
      }
    }

    // Allocate in space, ignoring if already allocated
    const allocationInsert = await context.allocationStorage.insert({
      space,
      blob,
      invocation: cause,
      // TODO: add write target here
      // will the URL be enough to track?
    })
    if (allocationInsert.error) {
      return {
        error: new Server.Failure('failed to allocate blob bytes'),
      }
    }

    if (hasBlob) {
      return {
        ok: { size: blob.size },
      }
    }

    return {
      ok: {
        size: blob.size,
        address: {
          url: createUploadUrl.ok.url.toString(),
          headers: createUploadUrl.ok.headers,
        },
      },
    }
  })
}
