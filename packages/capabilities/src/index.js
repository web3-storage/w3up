import * as Provider from './provider.js'
import * as Space from './space.js'
import * as Top from './top.js'
import * as Store from './store.js'
import * as Upload from './upload.js'
import * as Access from './access.js'
import * as Utils from './utils.js'
import * as Consumer from './consumer.js'
import * as Customer from './customer.js'
import * as Console from './console.js'
import * as RateLimit from './rate-limit.js'
import * as Admin from './admin.js'
import * as Subscription from './subscription.js'
import * as Filecoin from './filecoin/index.js'
import * as Storefront from './filecoin/storefront.js'
import * as Aggregator from './filecoin/aggregator.js'
import * as Dealer from './filecoin/dealer.js'
import * as DealTracker from './filecoin/deal-tracker.js'
import * as UCAN from './ucan.js'
import * as Plan from './plan.js'
import * as Usage from './usage.js'
import * as Blob from './blob.js'

export {
  Access,
  Provider,
  Space,
  Top,
  Store,
  Upload,
  Consumer,
  Customer,
  Console,
  Utils,
  RateLimit,
  Subscription,
  Filecoin,
  Storefront,
  Aggregator,
  Dealer,
  DealTracker,
  Admin,
  UCAN,
  Plan,
  Usage,
}

/** @type {import('./types.js').AbilitiesArray} */
export const abilitiesAsStrings = [
  Top.top.can,
  Provider.add.can,
  Space.space.can,
  Space.info.can,
  Upload.upload.can,
  Upload.add.can,
  Upload.get.can,
  Upload.remove.can,
  Upload.list.can,
  Store.store.can,
  Store.add.can,
  Store.get.can,
  Store.remove.can,
  Store.list.can,
  Access.access.can,
  Access.authorize.can,
  UCAN.attest.can,
  Customer.get.can,
  Consumer.has.can,
  Consumer.get.can,
  Subscription.get.can,
  Subscription.list.can,
  RateLimit.add.can,
  RateLimit.remove.can,
  RateLimit.list.can,
  Storefront.filecoinOffer.can,
  Storefront.filecoinSubmit.can,
  Storefront.filecoinAccept.can,
  Storefront.filecoinInfo.can,
  Aggregator.pieceOffer.can,
  Aggregator.pieceAccept.can,
  Dealer.aggregateOffer.can,
  Dealer.aggregateAccept.can,
  DealTracker.dealInfo.can,
  Admin.admin.can,
  Admin.upload.inspect.can,
  Admin.store.inspect.can,
  Plan.get.can,
  Usage.usage.can,
  Usage.report.can,
  Blob.blob.can,
  Blob.add.can,
  Blob.remove.can,
  Blob.list.can,
  Blob.serviceBlob.can,
  Blob.allocate.can,
  Blob.accept.can,
]
