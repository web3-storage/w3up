import * as Server from '@ucanto/server'
import * as Store from '@web3-storage/capabilities/store'
import * as API from '../types.js'
import { allocate } from '../space-allocate.js'

/**
 * @param {API.StoreServiceContext} context
 * @returns {API.ServiceMethod<API.StoreAdd, API.StoreAddSuccess, API.Failure>}
 */
export function storeAddProvider(context) {
  const { storeTable, carStoreBucket, maxUploadSize } = context
  return Server.provide(Store.add, async ({ capability, invocation }) => {
    const { link, origin, size } = capability.nb

    if (size > maxUploadSize) {
      return {
        error: new Server.Failure(
          `Maximum size exceeded: ${maxUploadSize}, split DAG into smaller shards.`
        ),
      }
    }

    const space = /** @type {import('@ucanto/interface').DIDKey} */ (
      Server.DID.parse(capability.with).did()
    )

    const [allocated, carExists] = await Promise.all([
      allocate(
        {
          capability: {
            with: space,
          },
        },
        context
      ),
      carStoreBucket.has(link),
    ])

    // If failed to allocate space, fail with allocation error
    if (allocated.error) {
      return allocated
    }

    let allocatedSize = size
    const res = await storeTable.insert({
      space,
      link,
      size,
      origin,
      invocation: invocation.cid,
    })
    if (res.error) {
      // if the insert failed with conflict then this item has already been
      // added to the space and there is no allocation change.
      if (res.error.name === 'RecordKeyConflict') {
        allocatedSize = 0
      } else {
        return res
      }
    }

    if (carExists) {
      return {
        ok: {
          status: 'done',
          allocated: allocatedSize,
          with: space,
          link,
        },
      }
    }

    const { url, headers } = await carStoreBucket.createUploadUrl(link, size)
    return {
      ok: {
        status: 'upload',
        allocated: allocatedSize,
        with: space,
        link,
        url: url.toString(),
        headers,
      },
    }
  })
}
