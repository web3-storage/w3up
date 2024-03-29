import { storeAddProvider } from './store/add.js'
import { storeGetProvider } from './store/get.js'
import { storeListProvider } from './store/list.js'
import { storeRemoveProvider } from './store/remove.js'
import * as API from './types.js'

/**
 * @param {API.StoreServiceContext} context
 */
export function createService(context) {
  return {
    add: storeAddProvider(context),
    get: storeGetProvider(context),
    list: storeListProvider(context),
    remove: storeRemoveProvider(context),
  }
}
