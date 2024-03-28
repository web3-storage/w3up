import * as Server from '@ucanto/server'
import * as Blob from '@web3-storage/capabilities/blob'
import * as API from '../types.js'

import { BlobItemSizeExceeded } from './lib.js'

/**
 * @param {API.BlobServiceContext} context
 * @returns {API.ServiceMethod<API.BlobAdd, API.BlobAddSuccess, API.BlobAddFailure>}
 */
export function blobAddProvider(context) {
  return Server.provideAdvanced({
    capability: Blob.add,
    handler: async ({ capability, invocation }) => {
      const { id, allocationStorage, maxUploadSize, taskScheduler } = context
      const { blob } = capability.nb
      const space = /** @type {import('@ucanto/interface').DIDKey} */ (
        Server.DID.parse(capability.with).did()
      )

      if (blob.size > maxUploadSize) {
        return {
          error: new BlobItemSizeExceeded(maxUploadSize)
        }
      }

      // Create effects for receipt
      const [allocatefx, acceptfx] = await Promise.all([
        Blob.allocate
          .invoke({
            issuer: id,
            audience: id,
            with: id.toDIDKey(),
            nb: {
              blob,
              cause: invocation.link(),
              space,
            },
            expiration: Infinity,
          })
          .delegate(),
        Blob.accept
          .invoke({
            issuer: id,
            audience: id,
            with: id.toDIDKey(),
            nb: {
              blob,
              exp: Number.MAX_SAFE_INTEGER,
            },
            expiration: Infinity,
          })
          .delegate(),
      ])

      // Schedule allocation if not allocated
      const allocated = await allocationStorage.exists(space, blob.content)
      if (!allocated.ok) {
        const { error: allocateScheduleError } = await taskScheduler.schedule(allocatefx)
        if (allocateScheduleError) {
          return {
            error: allocateScheduleError
          }
        }
      }

      // Schedule accept if allocated
      if (allocated.ok) {
        const { error: acceptScheduleError } = await taskScheduler.schedule(acceptfx)
        if (acceptScheduleError) {
          return {
            error: acceptScheduleError
          }
        }
      }

      /** @type {API.OkBuilder<API.BlobAddSuccess, API.BlobAddFailure>} */
      const result = Server.ok({
        claim: {
          'await/ok': acceptfx.link(),
        },
      })
      return result.fork(allocatefx.link()).join(acceptfx.link())
    },
  })
}
