import type { TupleToUnion } from 'type-fest'
import * as Ucanto from '@ucanto/interface'
import type { Schema } from '@ucanto/core'
import {
  InferInvokedCapability,
  Unit,
  DID,
  DIDKey,
  ToString,
  Link,
  Failure,
  UnknownLink,
} from '@ucanto/interface'
import { CAR } from '@ucanto/transport'
import {
  Phantom,
  PieceLink,
  ProofData,
  uint64,
} from '@web3-storage/data-segment'
import { space, info } from './space.js'
import * as provider from './provider.js'
import { top } from './top.js'
import * as BlobCaps from './blob.js'
import * as StoreCaps from './store.js'
import * as UploadCaps from './upload.js'
import * as AccessCaps from './access.js'
import * as CustomerCaps from './customer.js'
import * as ConsumerCaps from './consumer.js'
import * as SubscriptionCaps from './subscription.js'
import * as RateLimitCaps from './rate-limit.js'
import * as StorefrontCaps from './filecoin/storefront.js'
import * as AggregatorCaps from './filecoin/aggregator.js'
import * as DealTrackerCaps from './filecoin/deal-tracker.js'
import * as DealerCaps from './filecoin/dealer.js'
import * as AdminCaps from './admin.js'
import * as UCANCaps from './ucan.js'
import * as PlanCaps from './plan.js'
import * as UsageCaps from './usage.js'

export type ISO8601Date = string

export type { Unit, PieceLink }

/**
 * An IPLD Link that has the CAR codec code.
 */
export type CARLink = Link<unknown, typeof CAR.codec.code>

export type AccountDID = DID<'mailto'>
export type SpaceDID = DID<'key'>

/**
 * Error for cases where an interface implementation needs to return an
 * error that isn't defined explicitly in the interface.
 */
export interface UnexpectedError extends Ucanto.Failure {
  name: 'UnexpectedError'
  cause: unknown
}

/**
 * failure due to a resource not having enough storage capacity.
 */
export interface InsufficientStorage {
  name: 'InsufficientStorage'
  message: string
}

export interface UnknownProvider extends Failure {
  name: 'UnknownProvider'
  did: DID
}

/**
 * @see https://github.com/filecoin-project/FIPs/pull/758/files
 */
export type PieceLinkSchema = Schema.Schema<PieceLink>

// Access
export type Access = InferInvokedCapability<typeof AccessCaps.access>
export type AccessAuthorize = InferInvokedCapability<
  typeof AccessCaps.authorize
>

// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface AccessAuthorizeSuccess {
  request: Link
  expiration: number
}

export interface AccessAuthorizeFailure extends Ucanto.Failure {}

export type AccessClaim = InferInvokedCapability<typeof AccessCaps.claim>
export interface AccessClaimSuccess {
  delegations: Record<string, Ucanto.ByteView<Ucanto.Delegation>>
}
export interface AccessClaimFailure extends Ucanto.Failure {
  name: 'AccessClaimFailure'
  message: string
}

export interface AccessConfirmSuccess {
  delegations: Record<string, Ucanto.ByteView<Ucanto.Delegation>>
}
export interface AccessConfirmFailure extends Ucanto.Failure {}

export type AccessDelegate = InferInvokedCapability<typeof AccessCaps.delegate>
export type AccessDelegateSuccess = Unit
export type AccessDelegateFailure = InsufficientStorage | DelegationNotFound

export interface DelegationNotFound extends Ucanto.Failure {
  name: 'DelegationNotFound'
}

export type AccessConfirm = InferInvokedCapability<typeof AccessCaps.confirm>

// Usage

export type Usage = InferInvokedCapability<typeof UsageCaps.usage>
export type UsageReport = InferInvokedCapability<typeof UsageCaps.report>
export type UsageReportSuccess = Record<ProviderDID, UsageData>
export type UsageReportFailure = Ucanto.Failure

export interface UsageData {
  /** Provider the report concerns, e.g. `did:web:web3.storage` */
  provider: ProviderDID
  /** Space the report concerns. */
  space: SpaceDID
  /** Period the report applies to. */
  period: {
    /** ISO datetime the report begins from (inclusive). */
    from: ISO8601Date
    /** ISO datetime the report ends at (inclusive). */
    to: ISO8601Date
  }
  /** Observed space size for the period. */
  size: {
    /** Size at the beginning of the report period. */
    initial: number
    /** Size at the end of the report period. */
    final: number
  }
  /** Events that caused the size to change during the period. */
  events: Array<{
    /** CID of the invoked task that caused the size to change. */
    cause: Link
    /** Number of bytes that were added or removed. */
    delta: number
    /** ISO datetime that the receipt was issued for the change. */
    receiptAt: ISO8601Date
  }>
}

// Provider
export type ProviderAdd = InferInvokedCapability<typeof provider.add>
// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface ProviderAddSuccess {}
export type ProviderAddFailure = InvalidProvider | Ucanto.Failure
export type ProviderDID = DID<'web'>

export interface InvalidProvider extends Ucanto.Failure {
  name: 'InvalidProvider'
}

// Customer
export type CustomerGet = InferInvokedCapability<typeof CustomerCaps.get>
export interface CustomerGetSuccess {
  did: AccountDID
  subscriptions: string[]
}
export interface CustomerNotFound extends Ucanto.Failure {
  name: 'CustomerNotFound'
}
export type CustomerGetFailure = CustomerNotFound | Ucanto.Failure

// Consumer
export type ConsumerHas = InferInvokedCapability<typeof ConsumerCaps.has>
export type ConsumerHasSuccess = boolean
export type ConsumerHasFailure = Ucanto.Failure
export type ConsumerGet = InferInvokedCapability<typeof ConsumerCaps.get>
export interface ConsumerGetSuccess {
  did: DIDKey
  allocated: number
  limit: number
  subscription: string
}
export interface ConsumerNotFound extends Ucanto.Failure {
  name: 'ConsumerNotFound'
}
export type ConsumerGetFailure = ConsumerNotFound | Ucanto.Failure

// Subscription
export type SubscriptionGet = InferInvokedCapability<
  typeof SubscriptionCaps.get
>
export interface SubscriptionGetSuccess {
  customer: AccountDID
  consumer?: DIDKey
}
export interface SubscriptionNotFound extends Ucanto.Failure {
  name: 'SubscriptionNotFound'
}
export type SubscriptionGetFailure =
  | SubscriptionNotFound
  | UnknownProvider
  | Ucanto.Failure

export type SubscriptionList = InferInvokedCapability<
  typeof SubscriptionCaps.list
>
export interface SubscriptionListSuccess {
  results: Array<SubscriptionListItem>
}
export interface SubscriptionListItem {
  subscription: string
  provider: ProviderDID
  consumers: SpaceDID[]
}
export type SubscriptionListFailure = Ucanto.Failure

// Rate Limit
export type RateLimitAdd = InferInvokedCapability<typeof RateLimitCaps.add>
export interface RateLimitAddSuccess {
  id: string
}
export type RateLimitAddFailure = Ucanto.Failure

export type RateLimitRemove = InferInvokedCapability<
  typeof RateLimitCaps.remove
>
export type RateLimitRemoveSuccess = Unit

export interface RateLimitsNotFound extends Ucanto.Failure {
  name: 'RateLimitsNotFound'
}
export type RateLimitRemoveFailure = RateLimitsNotFound | Ucanto.Failure

export type RateLimitList = InferInvokedCapability<typeof RateLimitCaps.list>
export interface RateLimitSubject {
  id: string
  rate: number
}
export interface RateLimitListSuccess {
  limits: RateLimitSubject[]
}
export type RateLimitListFailure = Ucanto.Failure

// Space
export type Space = InferInvokedCapability<typeof space>
export type SpaceInfo = InferInvokedCapability<typeof info>

// filecoin
export interface DealMetadata {
  dataType: uint64
  dataSource: SingletonMarketSource
}
/** @see https://github.com/filecoin-project/go-data-segment/blob/e3257b64fa2c84e0df95df35de409cfed7a38438/datasegment/verifier.go#L8-L14 */
export interface DataAggregationProof {
  /**
   * Proof the piece is included in the aggregate.
   */
  inclusion: InclusionProof
  /**
   * Filecoin deal metadata.
   */
  aux: DealMetadata
}
/** @see https://github.com/filecoin-project/go-data-segment/blob/e3257b64fa2c84e0df95df35de409cfed7a38438/datasegment/inclusion.go#L30-L39 */
export interface InclusionProof {
  /**
   * Proof of inclusion of the client's data segment in the data aggregator's
   * Merkle tree (includes position information). i.e. a proof that the root
   * node of the subtree containing all the nodes (leafs) of a data segment is
   * contained in CommDA.
   */
  subtree: ProofData
  /**
   * Proof that an entry for the user's data is contained in the index of the
   * aggregator's deal. i.e. a proof that the data segment index constructed
   * from the root of the user's data segment subtree is contained in the index
   * of the deal tree.
   */
  index: ProofData
}
export interface SingletonMarketSource {
  dealID: uint64
}

export interface FilecoinOfferSuccess {
  /**
   * Commitment proof for piece.
   */
  piece: PieceLink
}
export type FilecoinOfferFailure = ContentNotFound | Ucanto.Failure

export interface ContentNotFound extends Ucanto.Failure {
  name: 'ContentNotFound'
  content: Link
}

export interface FilecoinSubmitSuccess {
  /**
   * Commitment proof for piece.
   */
  piece: PieceLink
}

export type FilecoinSubmitFailure = InvalidPieceCID | Ucanto.Failure

export interface FilecoinAcceptSuccess extends DataAggregationProof {
  aggregate: PieceLink
  piece: PieceLink
}

export type FilecoinAcceptFailure =
  | InvalidContentPiece
  | ProofNotFound
  | Ucanto.Failure

export interface InvalidContentPiece extends Ucanto.Failure {
  name: 'InvalidContentPiece'
  content: PieceLink
}

export interface ProofNotFound extends Ucanto.Failure {
  name: 'ProofNotFound'
}

export interface FilecoinInfoSuccess {
  piece: PieceLink
  aggregates: FilecoinInfoAcceptedAggregate[]
  deals: FilecoinInfoAcceptedDeal[]
}

export interface FilecoinInfoAcceptedAggregate {
  /**
   * Aggregate piece CID.
   */
  aggregate: PieceLink
  /**
   * Proof the piece is included in the aggregate.
   */
  inclusion: InclusionProof
}

export interface FilecoinInfoAcceptedDeal
  extends Omit<DataAggregationProof, 'inclusion'>,
    DealDetails {
  aggregate: PieceLink
}

export type FilecoinInfoFailure =
  | ContentNotFound
  | InvalidContentPiece
  | Ucanto.Failure

// filecoin aggregator
export interface PieceOfferSuccess {
  /**
   * Commitment proof for piece.
   */
  piece: PieceLink
}
export type PieceOfferFailure = Ucanto.Failure

export interface PieceAcceptSuccess {
  /**
   * Commitment proof for piece.
   */
  piece: PieceLink
  /**
   * Commitment proof for aggregate.
   */
  aggregate: PieceLink
  /**
   * Proof the piece is included in the aggregate.
   */
  inclusion: InclusionProof
}
export type PieceAcceptFailure = Ucanto.Failure

// filecoin dealer
export interface AggregateOfferSuccess {
  /**
   * Commitment proof for aggregate.
   */
  aggregate: PieceLink
}
export type AggregateOfferFailure = Ucanto.Failure

export interface AggregateAcceptSuccess extends DealMetadata {
  aggregate: PieceLink
}
export type AggregateAcceptFailure = InvalidPiece | Ucanto.Failure

export interface InvalidPiece extends Ucanto.Failure {
  name: 'InvalidPiece'
  /**
   * Commitment proof for aggregate.
   */
  aggregate: PieceLink
  cause: InvalidPieceCID[]
}

export interface InvalidPieceCID extends Ucanto.Failure {
  name: 'InvalidPieceCID'
  piece: PieceLink
}

// filecoin deal tracker
export interface DealInfoSuccess {
  deals: Record<string & Phantom<uint64>, DealDetails>
}

export interface DealDetails {
  provider: FilecoinAddress
  // TODO: start/end epoch? etc.
}

export type FilecoinAddress = string

export type DealInfoFailure = DealNotFound | Ucanto.Failure

export interface DealNotFound extends Ucanto.Failure {
  name: 'DealNotFound'
}

// Upload
export type Upload = InferInvokedCapability<typeof UploadCaps.upload>
export type UploadAdd = InferInvokedCapability<typeof UploadCaps.add>
export type UploadGet = InferInvokedCapability<typeof UploadCaps.get>
export type UploadRemove = InferInvokedCapability<typeof UploadCaps.remove>
export type UploadList = InferInvokedCapability<typeof UploadCaps.list>

export interface UploadNotFound extends Ucanto.Failure {
  name: 'UploadNotFound'
}

export type UploadGetFailure = UploadNotFound | Ucanto.Failure

// Blob
export type Blob = InferInvokedCapability<typeof BlobCaps.blob>
export type BlobAdd = InferInvokedCapability<typeof BlobCaps.add>
export type BlobRemove = InferInvokedCapability<typeof BlobCaps.remove>
export type BlobList = InferInvokedCapability<typeof BlobCaps.list>
export type ServiceBlob = InferInvokedCapability<typeof BlobCaps.serviceBlob>
export type BlobAllocate = InferInvokedCapability<typeof BlobCaps.allocate>
export type BlobAccept = InferInvokedCapability<typeof BlobCaps.accept>

export type BlobMultihash = Uint8Array

// Blob add
export interface BlobAddSuccess {
  claim: {
    'await/ok': Link
  }
}

export interface BlobItemSizeExceeded extends Ucanto.Failure {
  name: 'BlobItemSizeExceeded'
}
export type BlobAddFailure = BlobItemSizeExceeded | Ucanto.Failure

// Blob remove
export interface BlobRemoveSuccess {
  size: number
}

export interface BlobItemNotFound extends Ucanto.Failure {
  name: 'BlobItemNotFound'
}

export type BlobRemoveFailure = BlobItemNotFound | Ucanto.Failure

// Blob list
export interface BlobListSuccess extends ListResponse<BlobListItem> {}
export interface BlobListItem {
  blob: { content: Uint8Array; size: number }
  insertedAt: ISO8601Date
}

export type BlobListFailure = Ucanto.Failure

// Blob allocate
export interface BlobAllocateSuccess {
  size: number
  address?: BlobAddress
}

export interface BlobAddress {
  url: ToString<URL>
  headers: Record<string, string>
}

export interface BlobItemNotFound extends Ucanto.Failure {
  name: 'BlobItemNotFound'
}

export interface BlobNotAllocableToSpace extends Ucanto.Failure {
  name: 'BlobNotAllocableToSpace'
}

export type BlobAllocateFailure =
  | BlobItemNotFound
  | BlobNotAllocableToSpace
  | Ucanto.Failure

// Blob accept
export interface BlobAcceptSuccess {
  claim: Link
}

export type BlobAcceptFailure = BlobItemNotFound | Ucanto.Failure

// Store
export type Store = InferInvokedCapability<typeof StoreCaps.store>
export type StoreAdd = InferInvokedCapability<typeof StoreCaps.add>
export type StoreGet = InferInvokedCapability<typeof StoreCaps.get>
export type StoreRemove = InferInvokedCapability<typeof StoreCaps.remove>
export type StoreList = InferInvokedCapability<typeof StoreCaps.list>

export type StoreAddSuccess = StoreAddSuccessDone | StoreAddSuccessUpload

export type StoreAddSuccessStatusUpload = 'upload'
export type StoreAddSuccessStatusDone = 'done'

export interface StoreAddSuccessResult {
  /**
   * Status of the item to store. A "done" status indicates that it is not
   * necessary to upload the item. An "upload" status indicates that the item
   * should be uploaded to the provided URL.
   */
  status: StoreAddSuccessStatusUpload | StoreAddSuccessStatusDone
  /**
   * Total bytes allocated in the space to accommodate this stored item.
   * May be zero if the item is _already_ stored in _this_ space.
   */
  allocated: number
  /** DID of the space this item will be stored in. */
  with: DID
  /** CID of the item. */
  link: UnknownLink
}

export interface StoreAddSuccessDone extends StoreAddSuccessResult {
  status: StoreAddSuccessStatusDone
}

export interface StoreAddSuccessUpload extends StoreAddSuccessResult {
  status: StoreAddSuccessStatusUpload
  url: ToString<URL>
  headers: Record<string, string>
}

export interface StoreRemoveSuccess {
  size: number
}

export interface StoreItemNotFound extends Ucanto.Failure {
  name: 'StoreItemNotFound'
}

export type StoreRemoveFailure = StoreItemNotFound | Ucanto.Failure

export type StoreGetSuccess = StoreListItem

export type StoreGetFailure = StoreItemNotFound | Ucanto.Failure

export interface StoreListSuccess extends ListResponse<StoreListItem> {}

export interface ListResponse<R> {
  cursor?: string
  before?: string
  after?: string
  size: number
  results: R[]
}

export interface StoreListItem {
  link: UnknownLink
  size: number
  origin?: UnknownLink
  insertedAt: ISO8601Date
}

export interface UploadListItem {
  root: UnknownLink
  shards?: CARLink[]
  insertedAt: ISO8601Date
  updatedAt: ISO8601Date
}

// TODO: (olizilla) make this an UploadListItem too?
export type UploadAddSuccess = Omit<UploadListItem, 'insertedAt' | 'updatedAt'>

export type UploadGetSuccess = UploadListItem

export type UploadRemoveSuccess = UploadAddSuccess

export interface UploadListSuccess extends ListResponse<UploadListItem> {}

// UCAN core events

export type UCANRevoke = InferInvokedCapability<typeof UCANCaps.revoke>
export type UCANAttest = InferInvokedCapability<typeof UCANCaps.attest>

export interface Timestamp {
  /**
   * Unix timestamp in seconds.
   */
  time: number
}

export type UCANRevokeSuccess = Timestamp

/**
 * Error is raised when `UCAN` being revoked is not supplied or it's proof chain
 * leading to supplied `scope` is not supplied.
 */
export interface UCANNotFound extends Ucanto.Failure {
  name: 'UCANNotFound'
}

/**
 * Error is raised when `UCAN` being revoked does not have provided `scope` in
 * the proof chain.
 */
export interface InvalidRevocationScope extends Ucanto.Failure {
  name: 'InvalidRevocationScope'
}

/**
 * Error is raised when `UCAN` revocation is issued by unauthorized principal,
 * that is `with` field is not an `iss` of the `scope`.
 */
export interface UnauthorizedRevocation extends Ucanto.Failure {
  name: 'UnauthorizedRevocation'
}

/**
 * Error is raised when `UCAN` revocation cannot be stored. This
 * is usually not a client error.
 */
export interface RevocationsStoreFailure extends Ucanto.Failure {
  name: 'RevocationsStoreFailure'
}

export type UCANRevokeFailure =
  | UCANNotFound
  | InvalidRevocationScope
  | UnauthorizedRevocation
  | RevocationsStoreFailure

// Admin
export type Admin = InferInvokedCapability<typeof AdminCaps.admin>
export type AdminUploadInspect = InferInvokedCapability<
  typeof AdminCaps.upload.inspect
>
export type AdminStoreInspect = InferInvokedCapability<
  typeof AdminCaps.store.inspect
>
export interface SpaceAdmin {
  did: DID
  insertedAt: string
}
export interface AdminUploadInspectSuccess {
  spaces: SpaceAdmin[]
}
export type AdminUploadInspectFailure = Ucanto.Failure
export interface AdminStoreInspectSuccess {
  spaces: SpaceAdmin[]
}
export type AdminStoreInspectFailure = Ucanto.Failure
// Filecoin
export type FilecoinOffer = InferInvokedCapability<
  typeof StorefrontCaps.filecoinOffer
>
export type FilecoinSubmit = InferInvokedCapability<
  typeof StorefrontCaps.filecoinSubmit
>
export type FilecoinAccept = InferInvokedCapability<
  typeof StorefrontCaps.filecoinAccept
>
export type FilecoinInfo = InferInvokedCapability<
  typeof StorefrontCaps.filecoinInfo
>
export type PieceOffer = InferInvokedCapability<
  typeof AggregatorCaps.pieceOffer
>
export type PieceAccept = InferInvokedCapability<
  typeof AggregatorCaps.pieceAccept
>
export type AggregateOffer = InferInvokedCapability<
  typeof DealerCaps.aggregateOffer
>
export type AggregateAccept = InferInvokedCapability<
  typeof DealerCaps.aggregateAccept
>
export type DealInfo = InferInvokedCapability<typeof DealTrackerCaps.dealInfo>

// Plan

export type PlanGet = InferInvokedCapability<typeof PlanCaps.get>
export interface PlanGetSuccess {
  updatedAt: ISO8601Date
  product: DID
}

export interface PlanNotFound extends Ucanto.Failure {
  name: 'PlanNotFound'
}

export type PlanGetFailure = PlanNotFound | UnexpectedError

export type PlanSet = InferInvokedCapability<typeof PlanCaps.set>

export type PlanSetSuccess = Unit

/**
 * @deprecate currently unused - used to be part of PlanSetFailure but we switched to CustomerNotFound
 */
export interface AccountNotFound extends Ucanto.Failure {
  name: 'AccountNotFound'
}

export interface InvalidPlanName extends Ucanto.Failure {
  name: 'InvalidPlanName'
}

export interface PlanUpdateError extends Ucanto.Failure {
  name: 'PlanUpdateError'
}

export type PlanSetFailure =
  | CustomerNotFound
  | PlanUpdateError
  | UnexpectedError

// Top
export type Top = InferInvokedCapability<typeof top>

export type ServiceAbility = TupleToUnion<ServiceAbilityArray>

export type ServiceAbilityArray = [
  Top['can'],
  ProviderAdd['can'],
  Space['can'],
  SpaceInfo['can'],
  Upload['can'],
  UploadAdd['can'],
  UploadGet['can'],
  UploadRemove['can'],
  UploadList['can'],
  Store['can'],
  StoreAdd['can'],
  StoreGet['can'],
  StoreRemove['can'],
  StoreList['can'],
  Access['can'],
  AccessAuthorize['can'],
  UCANAttest['can'],
  CustomerGet['can'],
  ConsumerHas['can'],
  ConsumerGet['can'],
  SubscriptionGet['can'],
  SubscriptionList['can'],
  RateLimitAdd['can'],
  RateLimitRemove['can'],
  RateLimitList['can'],
  FilecoinOffer['can'],
  FilecoinSubmit['can'],
  FilecoinAccept['can'],
  FilecoinInfo['can'],
  PieceOffer['can'],
  PieceAccept['can'],
  AggregateOffer['can'],
  AggregateAccept['can'],
  DealInfo['can'],
  Admin['can'],
  AdminUploadInspect['can'],
  AdminStoreInspect['can'],
  PlanGet['can'],
  Usage['can'],
  UsageReport['can'],
  Blob['can'],
  BlobAdd['can'],
  BlobRemove['can'],
  BlobList['can'],
  ServiceBlob['can'],
  BlobAllocate['can'],
  BlobAccept['can']
]

/**
 * @deprecated use ServiceAbility
 */
export type Abilities = ServiceAbility

/**
 * @deprecated use ServiceAbilityArray
 */
export type AbilitiesArray = ServiceAbilityArray
