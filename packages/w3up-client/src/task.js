import * as API from '@ucanto/interface'

/**
 * @template T
 * @param {unknown|PromiseLike<T>} value
 * @returns {value is PromiseLike<T>}
 */
const isPromiseLike = (value) =>
  value != null &&
  typeof (/** @type {{then?:unknown}} */ (value).then) === 'function'

/**
 * @typedef {PromiseLike<void>} Wait
 */

/**
 * @template T
 * @param {T} source
 * @returns {Generator<Wait, Awaited<T>, void>}
 */
export const wait = function* (source) {
  if (isPromiseLike(source)) {
    let ok
    yield source.then((value) => {
      ok = value
    })
    return /** @type {Awaited<T>} */ (ok)
  } else {
    return /** @type {Awaited<T>} */ (source)
  }
}

/**
 * @template {API.Result} R
 * @param {PromiseLike<R>|R} source
 * @returns {Generator<Wait|R, Required<R>['ok']>}
 */
export const join = function* (source) {
  const { ok, error } = yield* wait(source)
  if (ok) {
    return ok
  } else {
    throw error
  }
}

/**
 * @template {API.Result} R
 * @template Ok
 * @template {globalThis.Error} [Error=never]
 * @param {() => Generator<R|Wait, API.Result<Ok & {}, Error>, void>} task
 * @returns {Promise<API.Result<Ok & {}, Required<R>['error'] | Error>>}
 */
const execute = async (task) => {
  const process = task()
  let state = process.next()
  try {
    while (!state.done) {
      if (isPromiseLike(state.value)) {
        await state.value
        state = process.next()
      } else if (state.value.error) {
        return state.value
      } else {
        state = process.next()
      }
    }
    return state.value
  } catch (cause) {
    return { error: /** @type {Error} */ (cause) }
  }
}

export { execute as try }

/**
 * @template {API.Result} R
 * @template Ok
 * @template {globalThis.Error} [Error=never]
 * @param {() => Generator<R|Wait, API.Result<Ok & {}, Error>, void>} task
 */
export const perform = async (task) => {
  const result = await execute(task)
  if (result.ok) {
    return result.ok
  }
  throw result.error
}
