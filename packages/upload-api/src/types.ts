import type {
  Failure,
  ServiceMethod,
  UCANLink,
  HandlerExecutionError,
  Signer,
  DID,
  DIDKey,
  InboundCodec,
  Result,
  CapabilityParser,
  Match,
  ParsedCapability,
  InferInvokedCapability,
  RevocationChecker,
  ToString,
  UnknownLink,
  Unit,
} from '@ucanto/interface'
import type { ProviderInput, ConnectionView } from '@ucanto/server'

import { Signer as EdSigner } from '@ucanto/principal/ed25519'
import { StorefrontService } from '@web3-storage/filecoin-api/types'
import { ServiceContext as FilecoinServiceContext } from '@web3-storage/filecoin-api/storefront/api'
import { DelegationsStorage as Delegations } from './types/delegations.js'
import { ProvisionsStorage as Provisions } from './types/provisions.js'
import { RateLimitsStorage as RateLimits } from './types/rate-limits.js'

export type ValidationEmailSend = {
  to: string
  url: string
}

export type SpaceDID = DIDKey
export type ServiceDID = DID<'web'>
export type ServiceSigner = Signer<ServiceDID>
export interface SpaceProviderRegistry {
  hasStorageProvider(space: SpaceDID): Promise<Result<boolean, never>>
}

export interface InsufficientStorage extends Failure {
  name: 'InsufficientStorage'
}

export type AllocationError = InsufficientStorage

export interface Email {
  sendValidation: (input: { to: string; url: string }) => Promise<void>
}

export interface DebugEmail extends Email {
  emails: Array<ValidationEmailSend>
  take: () => Promise<ValidationEmailSend>
}

import {
  StoreAdd,
  StoreGet,
  StoreAddSuccess,
  StoreRemove,
  StoreRemoveSuccess,
  StoreRemoveFailure,
  StoreList,
  StoreListSuccess,
  StoreListItem,
  UploadAdd,
  UploadGet,
  UploadAddSuccess,
  UploadRemove,
  UploadRemoveSuccess,
  UploadList,
  UploadListSuccess,
  UploadListItem,
  AccessAuthorize,
  AccessAuthorizeSuccess,
  AccessDelegate,
  AccessDelegateFailure,
  AccessDelegateSuccess,
  AccessClaim,
  AccessClaimSuccess,
  AccessClaimFailure,
  AccessConfirm,
  AccessConfirmSuccess,
  AccessConfirmFailure,
  ConsumerHas,
  ConsumerHasSuccess,
  ConsumerHasFailure,
  ConsumerGet,
  ConsumerGetSuccess,
  ConsumerGetFailure,
  CustomerGet,
  CustomerGetSuccess,
  CustomerGetFailure,
  SubscriptionGet,
  SubscriptionGetSuccess,
  SubscriptionGetFailure,
  SubscriptionList,
  SubscriptionListSuccess,
  SubscriptionListFailure,
  RateLimitAdd,
  RateLimitAddSuccess,
  RateLimitAddFailure,
  RateLimitRemove,
  RateLimitRemoveSuccess,
  RateLimitRemoveFailure,
  RateLimitList,
  RateLimitListSuccess,
  RateLimitListFailure,
  AdminStoreInspect,
  AdminStoreInspectSuccess,
  AdminStoreInspectFailure,
  AdminUploadInspect,
  AdminUploadInspectSuccess,
  AdminUploadInspectFailure,
  ProviderAdd,
  ProviderAddSuccess,
  ProviderAddFailure,
  SpaceInfo,
  ProviderDID,
  StoreGetFailure,
  UploadGetFailure,
  ListResponse,
  CARLink,
  StoreGetSuccess,
  UploadGetSuccess,
  UCANRevoke,
  UCANRevokeSuccess,
  UCANRevokeFailure,
  PlanGet,
  PlanGetSuccess,
  PlanGetFailure,
  AccessAuthorizeFailure,
  UsageReportSuccess,
  UsageReportFailure,
  UsageReport,
  PlanSetSuccess,
  PlanSetFailure,
  PlanSet,
} from '@web3-storage/capabilities/types'
import * as Capabilities from '@web3-storage/capabilities'
import { RevocationsStorage } from './types/revocations.js'

export * from '@web3-storage/capabilities/types'
export * from '@ucanto/interface'

export type { ProvisionsStorage, Provision } from './types/provisions.js'
export type {
  DelegationsStorage,
  Query as DelegationsStorageQuery,
} from './types/delegations.js'
export type {
  Revocation,
  RevocationQuery,
  MatchingRevocations,
  RevocationsStorage,
} from './types/revocations.js'
export type { RateLimitsStorage, RateLimit } from './types/rate-limits.js'
import { PlansStorage } from './types/plans.js'
export type { PlansStorage } from './types/plans.js'
import { SubscriptionsStorage } from './types/subscriptions.js'
export type { SubscriptionsStorage }
import { UsageStorage } from './types/usage.js'
export type { UsageStorage }

export interface Service extends StorefrontService {
  store: {
    add: ServiceMethod<StoreAdd, StoreAddSuccess, Failure>
    get: ServiceMethod<StoreGet, StoreGetSuccess, StoreGetFailure>
    remove: ServiceMethod<StoreRemove, StoreRemoveSuccess, StoreRemoveFailure>
    list: ServiceMethod<StoreList, StoreListSuccess, Failure>
  }
  upload: {
    add: ServiceMethod<UploadAdd, UploadAddSuccess, Failure>
    get: ServiceMethod<UploadGet, UploadGetSuccess, UploadGetFailure>
    remove: ServiceMethod<UploadRemove, UploadRemoveSuccess, Failure>
    list: ServiceMethod<UploadList, UploadListSuccess, Failure>
  }
  console: {
    log: ServiceMethod<
      InferInvokedCapability<typeof Capabilities.Console.log>,
      Unit,
      never
    >
    error: ServiceMethod<
      InferInvokedCapability<typeof Capabilities.Console.error>,
      never,
      Failure & { cause: unknown }
    >
  }
  access: {
    authorize: ServiceMethod<
      AccessAuthorize,
      AccessAuthorizeSuccess,
      AccessAuthorizeFailure
    >
    claim: ServiceMethod<AccessClaim, AccessClaimSuccess, AccessClaimFailure>
    confirm: ServiceMethod<
      AccessConfirm,
      AccessConfirmSuccess,
      AccessConfirmFailure
    >
    delegate: ServiceMethod<
      AccessDelegate,
      AccessDelegateSuccess,
      AccessDelegateFailure
    >
  }
  consumer: {
    has: ServiceMethod<ConsumerHas, ConsumerHasSuccess, ConsumerHasFailure>
    get: ServiceMethod<ConsumerGet, ConsumerGetSuccess, ConsumerGetFailure>
  }
  customer: {
    get: ServiceMethod<CustomerGet, CustomerGetSuccess, CustomerGetFailure>
  }
  subscription: {
    get: ServiceMethod<
      SubscriptionGet,
      SubscriptionGetSuccess,
      SubscriptionGetFailure
    >
    list: ServiceMethod<
      SubscriptionList,
      SubscriptionListSuccess,
      SubscriptionListFailure
    >
  }
  'rate-limit': {
    add: ServiceMethod<RateLimitAdd, RateLimitAddSuccess, RateLimitAddFailure>
    remove: ServiceMethod<
      RateLimitRemove,
      RateLimitRemoveSuccess,
      RateLimitRemoveFailure
    >
    list: ServiceMethod<
      RateLimitList,
      RateLimitListSuccess,
      RateLimitListFailure
    >
  }

  ucan: {
    revoke: ServiceMethod<UCANRevoke, UCANRevokeSuccess, UCANRevokeFailure>
  }

  admin: {
    store: {
      inspect: ServiceMethod<
        AdminStoreInspect,
        AdminStoreInspectSuccess,
        AdminStoreInspectFailure
      >
    }
    upload: {
      inspect: ServiceMethod<
        AdminUploadInspect,
        AdminUploadInspectSuccess,
        AdminUploadInspectFailure
      >
    }
  }
  provider: {
    add: ServiceMethod<ProviderAdd, ProviderAddSuccess, ProviderAddFailure>
  }
  space: {
    info: ServiceMethod<SpaceInfo, SpaceInfoSuccess, SpaceInfoFailure>
  }
  plan: {
    get: ServiceMethod<PlanGet, PlanGetSuccess, PlanGetFailure>
    set: ServiceMethod<PlanSet, PlanSetSuccess, PlanSetFailure>
  }
  usage: {
    report: ServiceMethod<UsageReport, UsageReportSuccess, UsageReportFailure>
  }
}

export type StoreServiceContext = SpaceServiceContext & {
  maxUploadSize: number

  storeTable: StoreTable
  carStoreBucket: CarStoreBucket
}

export type UploadServiceContext = ConsumerServiceContext &
  SpaceServiceContext &
  RevocationServiceContext & {
    signer: EdSigner.Signer
    uploadTable: UploadTable
    dudewhereBucket: DudewhereBucket
  }

export interface AccessClaimContext {
  delegationsStorage: Delegations
}

export type AccessServiceContext = AccessClaimContext & {
  signer: EdSigner.Signer
  email: Email
  url: URL
  provisionsStorage: Provisions
  rateLimitsStorage: RateLimits
}

export interface ConsumerServiceContext {
  signer: EdSigner.Signer
  provisionsStorage: Provisions
}

export interface CustomerServiceContext {
  signer: EdSigner.Signer
  provisionsStorage: Provisions
}

export interface AdminServiceContext {
  signer: EdSigner.Signer
  uploadTable: UploadTable
  storeTable: StoreTable
}

export interface ConsoleServiceContext {}

export interface SpaceServiceContext {
  provisionsStorage: Provisions
  delegationsStorage: Delegations
  rateLimitsStorage: RateLimits
}

export interface ProviderServiceContext {
  provisionsStorage: Provisions
  rateLimitsStorage: RateLimits
  plansStorage: PlansStorage
  requirePaymentPlan?: boolean
}

export interface SubscriptionServiceContext {
  signer: EdSigner.Signer
  provisionsStorage: Provisions
  subscriptionsStorage: SubscriptionsStorage
}

export interface RateLimitServiceContext {
  rateLimitsStorage: RateLimits
}

export interface RevocationServiceContext {
  revocationsStorage: RevocationsStorage
}

export interface PlanServiceContext {
  plansStorage: PlansStorage
}

export interface UsageServiceContext {
  provisionsStorage: Provisions
  usageStorage: UsageStorage
}

export interface ServiceContext
  extends AccessServiceContext,
    ConsoleServiceContext,
    ConsumerServiceContext,
    CustomerServiceContext,
    ProviderServiceContext,
    SpaceServiceContext,
    StoreServiceContext,
    SubscriptionServiceContext,
    RateLimitServiceContext,
    RevocationServiceContext,
    PlanServiceContext,
    UploadServiceContext,
    FilecoinServiceContext,
    UsageServiceContext {}

export interface UcantoServerContext extends ServiceContext, RevocationChecker {
  id: Signer
  codec?: InboundCodec
  errorReporter: ErrorReporter
}

export interface UcantoServerTestContext
  extends UcantoServerContext,
    StoreTestContext,
    UploadTestContext {
  connection: ConnectionView<Service>
  mail: DebugEmail
  service: Signer<ServiceDID>
  fetch: typeof fetch

  grantAccess: (mail: { url: string | URL }) => Promise<void>
}

export interface StoreTestContext {}

export interface UploadTestContext {}

export interface ErrorReporter {
  catch: (error: HandlerExecutionError) => void
}

export interface CarStoreBucket {
  has: (link: UnknownLink) => Promise<boolean>
  createUploadUrl: (
    link: UnknownLink,
    size: number
  ) => Promise<{
    url: URL
    headers: {
      'x-amz-checksum-sha256': string
      'content-length': string
    } & Record<string, string>
  }>
}

export interface CarStoreBucketOptions {
  accessKeyId?: string
  secretAccessKey?: string
  region?: string
  bucket?: string
  sessionToken?: string
  expires?: number
}

export interface CarStoreBucketService {
  use(options?: CarStoreBucketOptions): Promise<CarStoreBucket>
}

export interface DudewhereBucket {
  put: (dataCid: string, carCid: string) => Promise<void>
}

/**
 * Indicates the requested record was not present in the table.
 */
export interface RecordNotFound extends Failure {
  name: 'RecordNotFound'
}

/**
 * Indicates the inserted record key conflicts with an existing key of a record
 * that already exists in the table.
 */
export interface RecordKeyConflict extends Failure {
  name: 'RecordKeyConflict'
}

export interface StoreTable {
  inspect: (link: UnknownLink) => Promise<Result<StoreInspectSuccess, Failure>>
  exists: (space: DID, link: UnknownLink) => Promise<Result<boolean, Failure>>
  get: (
    space: DID,
    link: UnknownLink
  ) => Promise<Result<StoreGetSuccess, RecordNotFound>>
  /** Inserts an item in the table if it does not already exist. */
  insert: (
    item: StoreAddInput
  ) => Promise<Result<StoreAddOutput, RecordKeyConflict>>
  /** Removes an item from the table but fails if the item does not exist. */
  remove: (
    space: DID,
    link: UnknownLink
  ) => Promise<Result<StoreRemoveSuccess, RecordNotFound>>
  list: (
    space: DID,
    options?: ListOptions
  ) => Promise<Result<ListResponse<StoreListItem>, Failure>>
}

export interface UploadTable {
  inspect: (link: UnknownLink) => Promise<Result<UploadInspectSuccess, Failure>>
  exists: (space: DID, root: UnknownLink) => Promise<Result<boolean, Failure>>
  get: (
    space: DID,
    link: UnknownLink
  ) => Promise<Result<UploadGetSuccess, RecordNotFound>>
  /**
   * Inserts an item in the table if it does not already exist or updates an
   * existing item if it does exist.
   */
  upsert: (item: UploadAddInput) => Promise<Result<UploadAddSuccess, Failure>>
  /** Removes an item from the table but fails if the item does not exist. */
  remove: (
    space: DID,
    root: UnknownLink
  ) => Promise<Result<UploadRemoveSuccess, RecordNotFound>>
  list: (
    space: DID,
    options?: ListOptions
  ) => Promise<Result<ListResponse<UploadListItem>, Failure>>
}

export type SpaceInfoSuccess = {
  did: SpaceDID
  providers: ProviderDID[]
}
export type SpaceInfoFailure = Failure | SpaceUnknown

export interface UnknownProvider extends Failure {
  name: 'UnknownProvider'
}
export type CustomerGetResult = Result<CustomerGetSuccess, CustomerGetFailure>
export type SubscriptionGetResult = Result<
  SubscriptionGetSuccess,
  SubscriptionGetFailure
>
export type AdminStoreInspectResult = Result<
  AdminStoreInspectSuccess,
  AdminStoreInspectFailure
>
export type AdminUploadInspectResult = Result<
  AdminUploadInspectSuccess,
  AdminUploadInspectFailure
>

export interface StoreAddInput {
  space: DID
  link: UnknownLink
  size: number
  origin?: UnknownLink

  /**
   * @deprecated - Issuer of the invocation is irrelevant as long as
   * they have authorization to invoke on subject `space`.
   */
  issuer?: DID
  invocation: UCANLink
}

export interface StoreAddOutput
  extends Omit<StoreAddInput, 'space' | 'issuer' | 'invocation'> {}

export interface StoreInspectSuccess {
  spaces: Array<{ did: DID; insertedAt: string }>
}

export interface UploadAddInput {
  space: DID
  root: UnknownLink
  shards?: CARLink[]
  issuer: DID
  invocation: UCANLink
}

export interface UploadInspectSuccess {
  spaces: Array<{ did: DID; insertedAt: string }>
}

export interface ListOptions {
  size?: number
  cursor?: string
  pre?: boolean
}

export interface TestSpaceRegistry {
  /**
   * Registers space with the registry.
   */
  registerSpace: (space: DID) => Promise<void>
}

export interface LinkJSON<T extends UnknownLink = UnknownLink> {
  '/': ToString<T>
}
export interface SpaceUnknown extends Failure {
  name: 'SpaceUnknown'
}

export type Input<C extends CapabilityParser<Match<ParsedCapability>>> =
  ProviderInput<InferInvokedCapability<C> & ParsedCapability>

export interface Assert {
  equal: <Actual, Expected extends Actual>(
    actual: Actual,
    expected: Expected,
    message?: string
  ) => unknown
  deepEqual: <Actual, Expected extends Actual>(
    actual: Actual,
    expected: Expected,
    message?: string
  ) => unknown
  ok: <Actual>(actual: Actual, message?: string) => unknown
}

export type Test = (assert: Assert, context: UcantoServerTestContext) => unknown
export type Tests = Record<string, Test>
