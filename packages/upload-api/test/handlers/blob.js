import * as API from '../../src/types.js'
import pDefer from 'p-defer'
import * as Server from '@ucanto/server'
import { equals } from 'uint8arrays'
import { sha256 } from 'multiformats/hashes/sha2'
import * as BlobCapabilities from '@web3-storage/capabilities/blob'

import { createServer, connect } from '../../src/lib.js'
import { alice, registerSpace } from '../util.js'
import { BlobItemSizeExceededName } from '../../src/blob/lib.js'

/**
 * @type {API.Tests}
 */
export const test = {
  'blob/add schedules allocation and returns effects for allocation and accept': async (assert, context) => {
    const deferredSchedule = pDefer()
    const { proof, spaceDid } = await registerSpace(alice, context)
    /** @type {import('@ucanto/interface').Capability[]} */
    const scheduledTasks = []

    // prepare data
    const data = new Uint8Array([11, 22, 34, 44, 55])
    const multihash = await sha256.digest(data)
    const content = multihash.bytes
    const size = data.byteLength

    // create service connection
    const connection = connect({
      id: context.id,
      channel: createServer({
        ...context,
        // successful task scheduling
        taskScheduler: {
          schedule: async (invocation) => {
            scheduledTasks.push(...invocation.capabilities)
            deferredSchedule.resolve()
            return Promise.resolve({ ok: {} })
          }
        }
      }),
    })

    // invoke `blob/add`
    const invocation = BlobCapabilities.add.invoke({
      issuer: alice,
      audience: context.id,
      with: spaceDid,
      nb: {
        blob: {
          content,
          size
        },
      },
      proofs: [proof],
    })
    const blobAdd = await invocation.execute(connection)
    if (!blobAdd.out.ok) {
      throw new Error('invocation failed', { cause: blobAdd })
    }

    assert.ok(blobAdd.out.ok.claim)
    assert.ok(blobAdd.fx.fork.length)
    assert.ok(blobAdd.fx.join)
    assert.ok(blobAdd.out.ok.claim['await/ok'].equals(blobAdd.fx.join))

    // validate scheduled task ran
    await deferredSchedule.promise
    assert.equal(scheduledTasks.length, 1)
    const [blobAllocateInvocation] = scheduledTasks
    assert.equal(blobAllocateInvocation.can, BlobCapabilities.allocate.can)
    assert.equal(blobAllocateInvocation.nb.space, spaceDid)
    assert.equal(blobAllocateInvocation.nb.blob.size, size)
    assert.ok(equals(blobAllocateInvocation.nb.blob.content, content))
  },
  'blob/add fails when a blob with size bigger than maximum size is added': async (assert, context) => {
    const { proof, spaceDid } = await registerSpace(alice, context)

    // prepare data
    const data = new Uint8Array([11, 22, 34, 44, 55])
    const multihash = await sha256.digest(data)
    const content = multihash.bytes

    // create service connection
    const connection = connect({
      id: context.id,
      channel: createServer({
        ...context,
        // successful task scheduling
        taskScheduler: {
          schedule: async () => {
            throw new Error('no task should be scheduled')
          }
        }
      }),
    })

    // invoke `blob/add`
    const invocation = BlobCapabilities.add.invoke({
      issuer: alice,
      audience: context.id,
      with: spaceDid,
      nb: {
        blob: {
          content,
          size: Number.MAX_SAFE_INTEGER
        },
      },
      proofs: [proof],
    })
    const blobAdd = await invocation.execute(connection)
    if (!blobAdd.out.error) {
      throw new Error('invocation should have failed')
    }
    assert.ok(blobAdd.out.error, 'invocation should have failed')
    assert.equal(blobAdd.out.error.name, BlobItemSizeExceededName)
  },
  'blob/add fails when allocate task cannot be scheduled': async (assert, context) => {
    const { proof, spaceDid } = await registerSpace(alice, context)

    // prepare data
    const data = new Uint8Array([11, 22, 34, 44, 55])
    const multihash = await sha256.digest(data)
    const content = multihash.bytes
    const size = data.byteLength

    // create service connection
    const connection = connect({
      id: context.id,
      channel: createServer({
        ...context,
        // successful task scheduling
        taskScheduler: {
          schedule: async (invocation) => {
            const capability = invocation.capabilities[0]
            return Promise.resolve({
              error: new Server.Failure(`failed to schedule task for ${capability.can}`)
            })
          }
        }
      }),
    })

    // invoke `blob/add`
    const invocation = BlobCapabilities.add.invoke({
      issuer: alice,
      audience: context.id,
      with: spaceDid,
      nb: {
        blob: {
          content,
          size
        },
      },
      proofs: [proof],
    })
    const blobAdd = await invocation.execute(connection)
    if (!blobAdd.out.error) {
      throw new Error('invocation should have failed')
    }
    assert.ok(blobAdd.out.error, 'invocation should have failed')
    assert.ok(blobAdd.out.error.message.includes(BlobCapabilities.allocate.can))
    assert.equal(blobAdd.out.error.name, 'Error')
  },
  'blob/add schedules accept and returns effects for allocation and accept': async (assert, context) => {
    const deferredSchedule = pDefer()
    const { proof, spaceDid } = await registerSpace(alice, context)
    /** @type {import('@ucanto/interface').Capability[]} */
    const scheduledTasks = []

    // prepare data
    const data = new Uint8Array([11, 22, 34, 44, 55])
    const multihash = await sha256.digest(data)
    const content = multihash.bytes
    const size = data.byteLength

    // create service connection
    const connection = connect({
      id: context.id,
      channel: createServer({
        ...context,
        // successful task scheduling
        taskScheduler: {
          schedule: async (invocation) => {
            scheduledTasks.push(...invocation.capabilities)
            deferredSchedule.resolve()
            return Promise.resolve({ ok: {} })
          }
        }
      }),
    })

    // create invocation for `blob/add`
    const invocation = BlobCapabilities.add.invoke({
      issuer: alice,
      audience: context.id,
      with: spaceDid,
      nb: {
        blob: {
          content,
          size
        },
      },
      proofs: [proof],
    })

    // allocate data from a fake previous `blob/add` schedule allocation task 
    await context.allocationStorage.insert({
      space: spaceDid,
      invocation: (await invocation.delegate()).cid,
      blob: {
        content,
        size
      },
    })

    const blobAdd = await invocation.execute(connection)
    if (!blobAdd.out.ok) {
      throw new Error('invocation failed', { cause: blobAdd })
    }

    assert.ok(blobAdd.out.ok.claim)
    assert.ok(blobAdd.fx.fork.length)
    assert.ok(blobAdd.fx.join)
    assert.ok(blobAdd.out.ok.claim['await/ok'].equals(blobAdd.fx.join))

    // validate scheduled task ran
    await deferredSchedule.promise
    assert.equal(scheduledTasks.length, 1)
    const [blobAcceptInvocation] = scheduledTasks
    assert.equal(blobAcceptInvocation.can, BlobCapabilities.accept.can)
    assert.ok(blobAcceptInvocation.nb.exp)
    assert.equal(blobAcceptInvocation.nb.blob.size, size)
    assert.ok(equals(blobAcceptInvocation.nb.blob.content, content))
  },
  'blob/add fails when accept task cannot be scheduled': async (assert, context) => {
    const { proof, spaceDid } = await registerSpace(alice, context)

    // prepare data
    const data = new Uint8Array([11, 22, 34, 44, 55])
    const multihash = await sha256.digest(data)
    const content = multihash.bytes
    const size = data.byteLength

    // create service connection
    const connection = connect({
      id: context.id,
      channel: createServer({
        ...context,
        // successful task scheduling
        taskScheduler: {
          schedule: async (invocation) => {
            const capability = invocation.capabilities[0]
            return Promise.resolve({
              error: new Server.Failure(`failed to schedule task for ${capability.can}`)
            })
          }
        }
      }),
    })

    // create invocation for `blob/add`
    const invocation = BlobCapabilities.add.invoke({
      issuer: alice,
      audience: context.id,
      with: spaceDid,
      nb: {
        blob: {
          content,
          size
        },
      },
      proofs: [proof],
    })

    // allocate data from a fake previous `blob/add` schedule allocation task 
    await context.allocationStorage.insert({
      space: spaceDid,
      invocation: (await invocation.delegate()).cid,
      blob: {
        content,
        size
      },
    })

    const blobAdd = await invocation.execute(connection)
    if (!blobAdd.out.error) {
      throw new Error('invocation should have failed')
    }
    assert.ok(blobAdd.out.error, 'invocation should have failed')
    assert.ok(blobAdd.out.error.message.includes(BlobCapabilities.accept.can))
    assert.equal(blobAdd.out.error.name, 'Error')
  },
  'skip blob/allocate allocates to space and returns presigned url': async (assert, context) => {
    const deferredSchedule = pDefer()
    const { proof, spaceDid } = await registerSpace(alice, context)
    /** @type {import('@ucanto/interface').Capability[]} */
    const scheduledTasks = []

    // prepare data
    const data = new Uint8Array([11, 22, 34, 44, 55])
    const multihash = await sha256.digest(data)
    const content = multihash.bytes
    const size = data.byteLength

    // create service connection
    const connection = connect({
      id: context.id,
      channel: createServer({
        ...context,
        // successful task scheduling
        taskScheduler: {
          schedule: async (invocation) => {
            scheduledTasks.push(...invocation.capabilities)
            deferredSchedule.resolve()
            return Promise.resolve({ ok: {} })
          }
        }
      }),
    })

    // invoke `blob/add`
    const invocation = BlobCapabilities.add.invoke({
      issuer: alice,
      audience: context.id,
      with: spaceDid,
      nb: {
        blob: {
          content,
          size
        },
      },
      proofs: [proof],
    })
    const blobAdd = await invocation.execute(connection)
    if (!blobAdd.out.ok) {
      throw new Error('invocation failed', { cause: blobAdd })
    }

    assert.ok(blobAdd.out.ok.claim)
    assert.ok(blobAdd.fx.fork.length)
    assert.ok(blobAdd.fx.join)
    assert.ok(blobAdd.out.ok.claim['await/ok'].equals(blobAdd.fx.join))

    // validate scheduled task ran
    await deferredSchedule.promise
    assert.equal(scheduledTasks.length, 1)
    const [blobAllocateInvocation] = scheduledTasks
    assert.equal(blobAllocateInvocation.can, BlobCapabilities.allocate.can)
    assert.equal(blobAllocateInvocation.nb.space, spaceDid)
    assert.equal(blobAllocateInvocation.nb.blob.size, size)
    assert.ok(equals(blobAllocateInvocation.nb.blob.content, content))
  },
}
