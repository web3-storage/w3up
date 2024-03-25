import { blobAddProvider } from './blob/add.js'
import { blobListProvider } from './blob/list.js'
import { blobRemoveProvider } from './blob/remove.js'
import * as API from './types.js'

/**
 * @param {API.BlobServiceContext} context
 */
export function createService(context) {
  return {
    add: blobAddProvider(context),
    list: blobListProvider(context),
    remove: blobRemoveProvider(context),
  }
}
