import type { TupleToUnion } from 'type-fest'
import * as Ucanto from '@ucanto/interface'
import type { Schema } from '@ucanto/core'
import { InferInvokedCapability, Unit, DID } from '@ucanto/interface'
import type { PieceLink } from '@web3-storage/data-segment'
import { space, info, recover, recoverValidation } from './space.js'
import * as provider from './provider.js'
import { top } from './top.js'
import { add, list, remove, store } from './store.js'
import * as UploadCaps from './upload.js'
import { claim, redeem } from './voucher.js'
import * as AccessCaps from './access.js'
import * as FilecoinCaps from './filecoin.js'

export type { Unit }
/**
 * failure due to a resource not having enough storage capacity.
 */
export interface InsufficientStorage {
  name: 'InsufficientStorage'
  message: string
}

export type PieceLinkSchema = Schema.Schema<PieceLink>

// Access
export type Access = InferInvokedCapability<typeof AccessCaps.access>
export type AccessAuthorize = InferInvokedCapability<
  typeof AccessCaps.authorize
>

// eslint-disable-next-line @typescript-eslint/no-empty-interface
export type AccessAuthorizeSuccess = Unit
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

export type AccessSession = InferInvokedCapability<typeof AccessCaps.session>
export type AccessConfirm = InferInvokedCapability<typeof AccessCaps.confirm>

// Provider
export type ProviderAdd = InferInvokedCapability<typeof provider.add>
// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface ProviderAddSuccess {}
export type ProviderAddFailure = InvalidProvider | Ucanto.Failure
export type ProviderDID = DID<'web'>

export interface InvalidProvider extends Ucanto.Failure {
  name: 'InvalidProvider'
}

// Space
export type Space = InferInvokedCapability<typeof space>
export type SpaceInfo = InferInvokedCapability<typeof info>
export type SpaceRecoverValidation = InferInvokedCapability<
  typeof recoverValidation
>
export type SpaceRecover = InferInvokedCapability<typeof recover>

// filecoin
export type QUEUE_STATUS = 'queued' | 'accepted' | 'rejected'
export interface FilecoinAddSuccess {
  status: QUEUE_STATUS
  piece: PieceLink
}
export interface FilecoinAddFailure extends Ucanto.Failure {
  reason: string
}

export interface PieceAddSuccess {
  status: QUEUE_STATUS
  piece: PieceLink
  aggregate?: PieceLink
}
export interface PieceAddFailure extends Ucanto.Failure {
  reason: string
}

export interface AggregateAddSuccess {
  status: QUEUE_STATUS
  piece?: PieceLink
}

export type AggregateAddFailure = AggregateAddParseFailure | AggregateAddFailureWithBadPiece

export interface AggregateAddParseFailure extends Ucanto.Failure {
  reason: string
}

export interface AggregateAddFailureWithBadPiece extends Ucanto.Failure {
  piece?: PieceLink
  cause?: AggregateAddFailureCause[] | unknown
}

export interface AggregateAddFailureCause {
  piece: PieceLink
  reason: string
}

// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface ChainInfoSuccess {
  // TODO
}

export interface ChainInfoFailure extends Ucanto.Failure {
  // TODO
}

// Voucher Protocol
export type VoucherRedeem = InferInvokedCapability<typeof redeem>
export type VoucherClaim = InferInvokedCapability<typeof claim>
// Upload
export type Upload = InferInvokedCapability<typeof UploadCaps.upload>
export type UploadAdd = InferInvokedCapability<typeof UploadCaps.add>
export type UploadRemove = InferInvokedCapability<typeof UploadCaps.remove>
export type UploadList = InferInvokedCapability<typeof UploadCaps.list>
// Store
export type Store = InferInvokedCapability<typeof store>
export type StoreAdd = InferInvokedCapability<typeof add>
export type StoreRemove = InferInvokedCapability<typeof remove>
export type StoreList = InferInvokedCapability<typeof list>
// Filecoin
export type FilecoinAdd = InferInvokedCapability<
  typeof FilecoinCaps.filecoinAdd
>
export type PieceAdd = InferInvokedCapability<typeof FilecoinCaps.pieceAdd>
export type AggregateAdd = InferInvokedCapability<
  typeof FilecoinCaps.aggregateAdd
>
export type ChainInfo = InferInvokedCapability<typeof FilecoinCaps.chainInfo>
// Top
export type Top = InferInvokedCapability<typeof top>

export type Abilities = TupleToUnion<AbilitiesArray>

export type AbilitiesArray = [
  Top['can'],
  ProviderAdd['can'],
  Space['can'],
  SpaceInfo['can'],
  SpaceRecover['can'],
  SpaceRecoverValidation['can'],
  Upload['can'],
  UploadAdd['can'],
  UploadRemove['can'],
  UploadList['can'],
  Store['can'],
  StoreAdd['can'],
  StoreRemove['can'],
  StoreList['can'],
  VoucherClaim['can'],
  VoucherRedeem['can'],
  Access['can'],
  AccessAuthorize['can'],
  AccessSession['can'],
  FilecoinAdd['can'],
  PieceAdd['can'],
  AggregateAdd['can'],
  ChainInfo['can']
]
