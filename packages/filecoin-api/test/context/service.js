import * as Client from '@ucanto/client'
import * as Server from '@ucanto/server'
import * as CAR from '@ucanto/transport/car'

import * as StorefrontCaps from '@web3-storage/capabilities/filecoin/storefront'
import * as AggregatorCaps from '@web3-storage/capabilities/filecoin/aggregator'
import * as DealerCaps from '@web3-storage/capabilities/filecoin/dealer'
import * as DealTrackerCaps from '@web3-storage/capabilities/filecoin/deal-tracker'

// eslint-disable-next-line no-unused-vars
import * as API from '../../src/types.js'

import { validateAuthorization } from '../utils.js'
import { mockService } from './mocks.js'

export { getStoreImplementations } from './store-implementations.js'
export { getQueueImplementations } from './queue-implementations.js'

/**
 * Mocked w3filecoin services
 */
export function getMockService() {
  return mockService({
    aggregate: {
      offer: Server.provideAdvanced({
        capability: DealerCaps.aggregateOffer,
        handler: async ({ invocation, context }) => {
          const invCap = invocation.capabilities[0]
          if (!invCap.nb?.pieces || !invCap.nb.aggregate) {
            throw new Error()
          }
          const fx = await DealerCaps.aggregateAccept
            .invoke({
              issuer: context.id,
              audience: context.id,
              with: context.id.did(),
              nb: {
                aggregate: invCap.nb.aggregate,
                pieces: invCap.nb?.pieces,
              },
              expiration: Infinity,
            })
            .delegate()

          return Server.ok({ aggregate: invCap.nb.aggregate }).join(fx.link())
        },
      }),
      accept: Server.provideAdvanced({
        capability: DealerCaps.aggregateAccept,
        handler: async ({ invocation, context }) => {
          const invCap = invocation.capabilities[0]
          if (!invCap.nb?.aggregate) {
            throw new Error()
          }

          return Server.ok({
            aggregate: invCap.nb.aggregate,
            dataSource: {
              dealID: 15151n,
            },
            dataType: 1n,
          })
        },
      }),
    },
    piece: {
      offer: Server.provideAdvanced({
        capability: AggregatorCaps.pieceOffer,
        handler: async ({ invocation, context }) => {
          const invCap = invocation.capabilities[0]
          if (!invCap.nb?.piece) {
            throw new Error()
          }

          // Create effect for receipt with self signed queued operation
          const fx = await AggregatorCaps.pieceAccept
            .invoke({
              issuer: context.id,
              audience: context.id,
              with: context.id.did(),
              nb: {
                ...invCap.nb,
              },
            })
            .delegate()

          return Server.ok({
            piece: invCap.nb?.piece,
          }).join(fx.link())
        },
      }),
      accept: Server.provideAdvanced({
        capability: AggregatorCaps.pieceAccept,
        // @ts-expect-error inclusion types
        handler: async ({ invocation, context }) => {
          const invCap = invocation.capabilities[0]
          if (!invCap.nb?.piece) {
            throw new Error()
          }

          // Create effect for receipt
          const fx = await DealerCaps.aggregateOffer
            .invoke({
              issuer: context.id,
              audience: context.id,
              with: context.id.did(),
              nb: {
                aggregate: invCap.nb.piece,
                pieces: invCap.nb.piece,
              },
            })
            .delegate()

          return Server.ok({
            piece: invCap.nb.piece,
            aggregate: invCap.nb.piece,
            inclusion: {
              subtree:
                /** @type {import('@web3-storage/data-segment').ProofData} */ [
                  0n,
                  /** @type {import('@web3-storage/data-segment').MerkleTreePath} */ ([]),
                ],
              index:
                /** @type {import('@web3-storage/data-segment').ProofData} */ [
                  0n,
                  /** @type {import('@web3-storage/data-segment').MerkleTreePath} */ ([]),
                ],
            },
          }).join(fx.link())
        },
      }),
    },
    filecoin: {
      submit: Server.provideAdvanced({
        capability: StorefrontCaps.filecoinSubmit,
        handler: async ({ invocation, context }) => {
          const invCap = invocation.capabilities[0]
          if (!invCap.nb?.piece) {
            throw new Error()
          }

          // Create effect for receipt with self signed queued operation
          const fx = await AggregatorCaps.pieceOffer
            .invoke({
              issuer: context.id,
              audience: context.id,
              with: context.id.did(),
              nb: {
                ...invCap.nb,
                group: context.id.did(),
              },
            })
            .delegate()

          return Server.ok({
            piece: invCap.nb?.piece,
          }).join(fx.link())
        },
      }),
      accept: Server.provideAdvanced({
        capability: StorefrontCaps.filecoinAccept,
        handler: async ({ invocation, context }) => {
          const invCap = invocation.capabilities[0]
          if (!invCap.nb?.piece) {
            throw new Error()
          }

          /** @type {API.UcantoInterface.OkBuilder<API.FilecoinAcceptSuccess, API.FilecoinAcceptFailure>} */
          const result = Server.ok({
            piece: invCap.nb.piece,
            aggregate: invCap.nb.piece,
            inclusion: {
              subtree:
                /** @type {import('@web3-storage/data-segment').ProofData} */ [
                  0n,
                  /** @type {import('@web3-storage/data-segment').MerkleTreePath} */ ([]),
                ],
              index:
                /** @type {import('@web3-storage/data-segment').ProofData} */ [
                  0n,
                  /** @type {import('@web3-storage/data-segment').MerkleTreePath} */ ([]),
                ],
            },
            aux: {
              dataType: 0n,
              dataSource: {
                dealID: 1138n,
              },
            },
          })

          return result
        },
      }),
      info: Server.provideAdvanced({
        capability: StorefrontCaps.filecoinInfo,
        handler: async ({ invocation, context }) => {
          const invCap = invocation.capabilities[0]
          if (!invCap.nb?.piece) {
            throw new Error()
          }
          return Server.ok({
            piece: invCap.nb.piece,
            deals: []
          })
        }})
    },
    deal: {
      info: Server.provideAdvanced({
        capability: DealTrackerCaps.dealInfo,
        handler: async ({ invocation, context }) => {
          const invCap = invocation.capabilities[0]
          if (!invCap.nb?.piece) {
            throw new Error()
          }

          /** @type {API.UcantoInterface.OkBuilder<API.DealInfoSuccess, API.DealInfoFailure>} */
          const result = Server.ok({
            deals: {
              111: {
                provider: 'f11111',
              },
            },
          })

          return result
        },
      }),
    },
  })
}

/**
 * @param {any} service
 * @param {any} id
 */
export function getConnection(id, service) {
  const server = Server.create({
    id: id,
    service,
    codec: CAR.inbound,
    validateAuthorization,
  })
  const connection = Client.connect({
    id: id,
    codec: CAR.outbound,
    channel: server,
  })

  return { connection }
}
