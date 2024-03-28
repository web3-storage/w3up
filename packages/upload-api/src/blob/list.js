import * as Server from '@ucanto/server'
import * as Blob from '@web3-storage/capabilities/blob'
import * as API from '../types.js'

/**
 * @param {API.BlobServiceContext} context
 * @returns {API.ServiceMethod<API.BlobList, API.BlobListSuccess, API.Failure>}
 */
export function blobListProvider(context) {
  return Server.provide(Blob.list, async ({ capability }) => {
    const { cursor, size, pre } = capability.nb
    const space = Server.DID.parse(capability.with).did()
    return await context.allocationStorage.list(space, { size, cursor, pre })
  })
}
