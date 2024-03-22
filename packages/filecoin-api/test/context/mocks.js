import * as Server from '@ucanto/server'

const notImplemented = () => {
  throw new Server.Failure('not implemented')
}

/**
 * @param {Partial<{
 * filecoin: Partial<import('../../src/types.js').StorefrontService['filecoin']>
 * piece: Partial<import('../../src/types.js').AggregatorService['piece']>
 * aggregate: Partial<import('../../src/types.js').DealerService['aggregate']>
 * deal: Partial<import('../../src/types.js').DealTrackerService['deal']>
 * }>} impl
 */
export function mockService(impl) {
  return {
    filecoin: {
      offer: withCallParams(impl.filecoin?.offer ?? notImplemented),
      submit: withCallParams(impl.filecoin?.submit ?? notImplemented),
      accept: withCallParams(impl.filecoin?.accept ?? notImplemented),
      info: withCallParams(impl.filecoin?.info ?? notImplemented),
    },
    piece: {
      offer: withCallParams(impl.piece?.offer ?? notImplemented),
      accept: withCallParams(impl.piece?.accept ?? notImplemented),
    },
    aggregate: {
      offer: withCallParams(impl.aggregate?.offer ?? notImplemented),
      accept: withCallParams(impl.aggregate?.accept ?? notImplemented),
    },
    deal: {
      info: withCallParams(impl.deal?.info ?? notImplemented),
    },
  }
}

/**
 * @template {Function} T
 * @param {T} fn
 */
function withCallParams(fn) {
  /** @param {T extends (...args: infer A) => any ? A : never} args */
  const annotatedParamsFn = (...args) => {
    // @ts-expect-error not typed param
    annotatedParamsFn._params.push(args[0].capabilities[0])
    annotatedParamsFn.called = true
    annotatedParamsFn.callCount++
    return fn(...args)
  }
  /** @type {any[]} */
  annotatedParamsFn._params = []
  annotatedParamsFn.called = false
  annotatedParamsFn.callCount = 0
  return annotatedParamsFn
}
