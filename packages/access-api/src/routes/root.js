import * as Server from '@ucanto/server'
import * as CAR from '@ucanto/transport/car'
import * as Legacy from '@ucanto/transport/legacy'
import * as Codec from '@ucanto/transport/codec'
import { provide } from '../service/index.js'
import * as json from '../ucanto/server-codec.js'
import * as API from '../bindings.js'

/**
 * We define a ucanto codec that will switch encoder / decoder based on the `content-type`
 * and `accept` headers respectively. If the `content-type` and `accept` headers are set to
 * `application/vnd.ipld.car` CAR encoder / decoder is used. If the `content-type` is set to
 * `application/car` legacy CAR decoder (which different from current CAR decoder) is used.
 * Legacy clients did not set `accept` header which is handled by `*/*;q=0.1` case that uses
 * `Legacy.response` that encodes in (CBOR) format expected by those clients. If `content-type`
 * and `accept` are set to `application/json`  JSON encoder and decoder are used.
 */
const codec = Codec.inbound({
  decoders: {
    [CAR.contentType]: CAR.request,
    [Legacy.contentType]: Legacy.request,
    [json.contentType]: json,
  },
  encoders: {
    '*/*;q=0.1': Legacy.response,
    [CAR.contentType]: CAR.response,
    [json.contentType]: json,
  },
})

/**
 * @param {{headers: Record<string, string>, body: Uint8Array}} request
 * @param {API.RouteContext} env
 * @param {API.HandlerContext} ctx
 */
export async function post({ headers, body }, env, ctx) {
  const server = Server.create({
    id: env.signer,
    codec,
    service: provide(env),
    catch: (/** @type {string | Error} */ err) => {
      env.log.error(err)
    },
  })

  const channel = server.codec.accept({ headers, body })

  // If we were unable to select a codec we do not support request encoding
  // so we simply return an error response to the client.
  if (channel.error) {
    const { status, headers = {}, message } = channel.error
    return new Response(message, {
      status,
      headers,
    })
  } else {
    const { encoder, decoder } = channel.ok
    /** @type {API.AgentMessage} */
    const message = await decoder.decode({ headers, body })

    // We block until we can log the UCAN invocation if this fails we return a 500
    // to the client. That is because in the future we expect that invocations will
    // be written to a queue first and then processed asynchronously, so if we
    // fail to enqueue the invocation we should fail the request.
    await env.ucanLog.log(CAR.request.encode(message))

    const result = await Server.execute(message, server)

    ctx.waitUntil(env.ucanLog.log(CAR.response.encode(result)))

    const { body: responseBody, ...response } = await encoder.encode(result)
    return new Response(responseBody, response)
  }
}

/**
 * Post request implicitly in JSON encoding.
 *
 * @param {import('@web3-storage/worker-utils/router').ParsedRequest} request
 * @param {API.RouteContext} env
 * @param {API.HandlerContext} ctx
 */
export const postJSON = async (request, env, ctx) =>
  post(
    {
      headers: {
        'content-type': json.contentType,
        accept: json.contentType,
        ...Object.fromEntries(request.headers.entries()),
      },
      body: new Uint8Array(await request.arrayBuffer()),
    },
    env,
    ctx
  )

/**
 * Post request in arbitrary encoding.
 *
 * @param {import('@web3-storage/worker-utils/router').ParsedRequest} request
 * @param {API.RouteContext} env
 * @param {API.HandlerContext} ctx
 */
export const postRoot = async (request, env, ctx) =>
  post(
    {
      headers: Object.fromEntries(request.headers.entries()),
      body: new Uint8Array(await request.arrayBuffer()),
    },
    env,
    ctx
  )
