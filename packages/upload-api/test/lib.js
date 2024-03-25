import * as AccessAuthorize from './handlers/access/authorize.js'
import * as AccessClaim from './handlers/access/claim.js'
import * as AccessDelegate from './handlers/access/delegate.js'
import * as AdminStoreInspect from './handlers/admin/store/inspect.js'
import * as AdminUploadInspect from './handlers/admin/upload/inspect.js'
import * as RateLimitAdd from './handlers/rate-limit/add.js'
import * as RateLimitList from './handlers/rate-limit/list.js'
import * as RateLimitRemove from './handlers/rate-limit/remove.js'
import * as Store from './handlers/store.js'
import * as Blob from './handlers/blob.js'
import * as Subscription from './handlers/subscription.js'
import * as Upload from './handlers/upload.js'
import * as Plan from './handlers/plan.js'
import * as Usage from './handlers/usage.js'
import { test as delegationsStorageTests } from './storage/delegations-storage-tests.js'
import { test as provisionsStorageTests } from './storage/provisions-storage-tests.js'
import { test as rateLimitsStorageTests } from './storage/rate-limits-storage-tests.js'
import { test as revocationsStorageTests } from './storage/revocations-storage-tests.js'
import { test as plansStorageTests } from './storage/plans-storage-tests.js'
import { DebugEmail } from '../src/utils/email.js'
export * as Context from './helpers/context.js'

export * from './util.js'

export const test = {
  ...Store.test,
  ...Blob.test,
  ...Upload.test,
}

export const storageTests = {
  ...delegationsStorageTests,
  ...provisionsStorageTests,
  ...rateLimitsStorageTests,
  ...revocationsStorageTests,
  ...plansStorageTests,
}

export const handlerTests = {
  ...AccessAuthorize,
  ...AccessClaim,
  ...AccessDelegate,
  ...AdminStoreInspect,
  ...AdminUploadInspect,
  ...RateLimitAdd,
  ...RateLimitList,
  ...RateLimitRemove,
  ...Store.test,
  ...Blob.test,
  ...Subscription.test,
  ...Upload.test,
  ...Plan.test,
  ...Usage.test,
}

export {
  Store,
  Upload,
  delegationsStorageTests,
  provisionsStorageTests,
  rateLimitsStorageTests,
  revocationsStorageTests,
  plansStorageTests,
  DebugEmail,
}
