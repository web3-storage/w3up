import * as Server from '@ucanto/server'
import * as Client from '@ucanto/client'
import * as Types from './types.js'
import * as Legacy from '@ucanto/transport/legacy'
import * as CAR from '@ucanto/transport/car'
import { create as createRevocationChecker } from './utils/revocation.js'
import { createService as createBlobService } from './blob.js'
import { createService as createStoreService } from './store.js'
import { createService as createUploadService } from './upload.js'
import { createService as createConsoleService } from './console.js'
import { createService as createAccessService } from './access.js'
import { createService as createConsumerService } from './consumer.js'
import { createService as createCustomerService } from './customer.js'
import { createService as createSpaceService } from './space.js'
import { createService as createProviderService } from './provider.js'
import { createService as createSubscriptionService } from './subscription.js'
import { createService as createAdminService } from './admin.js'
import { createService as createRateLimitService } from './rate-limit.js'
import { createService as createUcanService } from './ucan.js'
import { createService as createW3sService } from './service.js'
import { createService as createPlanService } from './plan.js'
import { createService as createUsageService } from './usage.js'
import { createService as createFilecoinService } from '@web3-storage/filecoin-api/storefront/service'

export * from './types.js'

/**
 * @param {Omit<Types.UcantoServerContext, 'validateAuthorization'>} options
 */
export const createServer = ({ id, codec = Legacy.inbound, ...context }) =>
  Server.create({
    ...createRevocationChecker(context),
    id,
    codec,
    service: createService({
      ...context,
      id,
    }),
    catch: (error) => context.errorReporter.catch(error),
  })

/**
 * @param {Types.ServiceContext} context
 * @returns {Types.Service}
 */
export const createService = (context) => ({
  access: createAccessService(context),
  blob: createBlobService(context),
  console: createConsoleService(context),
  consumer: createConsumerService(context),
  customer: createCustomerService(context),
  provider: createProviderService(context),
  'rate-limit': createRateLimitService(context),
  admin: createAdminService(context),
  space: createSpaceService(context),
  store: createStoreService(context),
  subscription: createSubscriptionService(context),
  upload: createUploadService(context),
  ucan: createUcanService(context),
  plan: createPlanService(context),
  ['web3.storage']: createW3sService(context),
  // storefront of filecoin pipeline
  filecoin: createFilecoinService(context).filecoin,
  usage: createUsageService(context),
})

/**
 * @param {object} options
 * @param {Types.Principal} options.id
 * @param {Types.Transport.Channel<Types.Service>} options.channel
 * @param {Types.OutboundCodec} [options.codec]
 */
export const connect = ({ id, channel, codec = CAR.outbound }) =>
  Client.connect({
    id,
    channel,
    codec,
  })

export {
  createService as createUploadService,
  createServer as createUploadServer,
  connect as createUploadClient,
}
