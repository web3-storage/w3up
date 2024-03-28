import * as Types from '../../src/types.js'

/**
 * @implements {Types.TaskScheduler}
 */
export class TaskScheduler {
  /**
   * @param {import('@ucanto/interface').Invocation} invocation
   */
  async schedule (invocation) {
    return Promise.resolve({
      ok: {}
    })
  }
}
