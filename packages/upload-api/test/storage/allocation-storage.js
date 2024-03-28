import * as Types from '../../src/types.js'
import { equals } from 'uint8arrays/equals'

/**
 * @implements {Types.AllocationStorage}
 */
export class AllocationStorage {
  constructor() {
    /** @type {(Types.BlobAddInput & Types.BlobListItem)[]} */
    this.items = []
  }

  /**
   * @param {Types.BlobAddInput} input
   * @returns {ReturnType<Types.AllocationStorage['insert']>}
   */
  async insert({ space, invocation, ...output }) {
    if (
      this.items.some(
        (i) => i.space === space && equals(i.blob.content, output.blob.content)
      )
    ) {
      return {
        error: { name: 'RecordKeyConflict', message: 'record key conflict' },
      }
    }
    this.items.unshift({
      space,
      invocation,
      ...output,
      insertedAt: new Date().toISOString(),
    })
    return { ok: output }
  }

  /**
   * @param {Types.DID} space
   * @param {Uint8Array} blobMultihash
   * @returns {ReturnType<Types.AllocationStorage['exists']>}
   */
  async exists(space, blobMultihash) {
    const item = this.items.find(
      (i) => i.space === space && equals(i.blob.content, blobMultihash)
    )
    return { ok: !!item }
  }

  /**
   * @param {Types.DID} space
   * @param {Uint8Array} blobMultihash
   * @returns {ReturnType<Types.AllocationStorage['remove']>}
   */
  async remove(space, blobMultihash) {
    const item = this.items.find(
      (i) => i.space === space && equals(i.blob.content, blobMultihash)
    )
    if (!item) {
      return { error: { name: 'RecordNotFound', message: 'record not found' } }
    }
    this.items = this.items.filter((i) => i !== item)
    return {
      ok: {
        size: item.blob.size,
      },
    }
  }

  /**
   * @param {Types.DID} space
   * @param {Types.ListOptions} options
   * @returns {ReturnType<Types.AllocationStorage['list']>}
   */
  async list(
    space,
    { cursor = '0', pre = false, size = this.items.length } = {}
  ) {
    const offset = parseInt(cursor, 10)
    const items = pre ? this.items.slice(0, offset) : this.items.slice(offset)

    const matches = [...items.entries()]
      .filter(([n, item]) => item.space === space)
      .slice(0, size)

    if (matches.length === 0) {
      return { ok: { size: 0, results: [] } }
    }

    const first = matches[0]
    const last = matches[matches.length - 1]

    const start = first[0] || 0
    const end = last[0] || 0
    const values = matches.map(([_, item]) => item)

    const [before, after, results] = pre
      ? [`${start}`, `${end + 1}`, values]
      : [`${start + offset}`, `${end + 1 + offset}`, values]

    return {
      ok: {
        size: values.length,
        before,
        after,
        cursor: after,
        results,
      },
    }
  }
}
