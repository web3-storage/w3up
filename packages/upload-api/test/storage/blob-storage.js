import * as Types from '../../src/types.js'

import { base64pad } from 'multiformats/bases/base64'
import { SigV4 } from '@web3-storage/sigv4'
import { base58btc } from 'multiformats/bases/base58'
import { sha256 } from 'multiformats/hashes/sha2'

/**
 * @implements {Types.BlobStorage}
 */
export class BlobStorage {
  /**
   * @param {Types.CarStoreBucketOptions & {http?: import('http')}} options
   */
  static async activate({ http, ...options } = {}) {
    const content = new Map()
    if (http) {
      const server = http.createServer(async (request, response) => {
        if (request.method === 'PUT') {
          const buffer = new Uint8Array(
            parseInt(request.headers['content-length'] || '0')
          )
          let offset = 0

          for await (const chunk of request) {
            buffer.set(chunk, offset)
            offset += chunk.length
          }

          const hash = await sha256.digest(buffer)
          const checksum = base64pad.baseEncode(hash.digest)

          if (checksum !== request.headers['x-amz-checksum-sha256']) {
            response.writeHead(400, `checksum mismatch`)
          } else {
            const { pathname } = new URL(request.url || '/', url)
            content.set(pathname, buffer)
            response.writeHead(200)
          }
        } else {
          response.writeHead(405)
        }

        response.end()
        // otherwise it keep connection lingering
        response.destroy()
      })
      await new Promise((resolve) => server.listen(resolve))

      // @ts-ignore - this is actually what it returns on http
      const port = server.address().port
      const url = new URL(`http://localhost:${port}`)

      return new BlobStorage({
        ...options,
        content,
        url,
        server,
      })
    } else {
      return new BlobStorage({
        ...options,
        content,
        url: new URL(`http://localhost:8989`),
      })
    }
  }

  /**
   * @returns {Promise<void>}
   */
  async deactivate() {
    const { server } = this
    if (server) {
      await new Promise((resolve, reject) => {
        // does not exist in node 16
        if (typeof server.closeAllConnections === 'function') {
          server.closeAllConnections()
        }

        server.close((error) => {
          if (error) {
            reject(error)
          } else {
            resolve(undefined)
          }
        })
      })
    }
  }

  /**
   * @param {Types.CarStoreBucketOptions & { server?: import('http').Server, url: URL, content: Map<string, Uint8Array> }} options
   */
  constructor({
    content,
    url,
    server,
    accessKeyId = 'id',
    secretAccessKey = 'secret',
    bucket = 'my-bucket',
    region = 'eu-central-1',
    expires,
  }) {
    this.server = server
    this.baseURL = url
    this.accessKeyId = accessKeyId
    this.secretAccessKey = secretAccessKey
    this.bucket = bucket
    this.region = region
    this.expires = expires
    this.content = content
  }

  /**
   * @param {Uint8Array} multihash
   */
  async has(multihash) {
    const encodedMultihash = base58btc.encode(multihash)
    return {
      ok: this.content.has(
        `/${this.bucket}/${encodedMultihash}/${encodedMultihash}.blob`
      ),
    }
  }

  /**
   * @param {Uint8Array} multihash
   * @param {number} size
   */
  async createUploadUrl(multihash, size) {
    const { bucket, expires, accessKeyId, secretAccessKey, region, baseURL } =
      this
    const encodedMultihash = base58btc.encode(multihash)
    // sigv4
    const sig = new SigV4({
      accessKeyId,
      secretAccessKey,
      region,
    })

    const checksum = base64pad.baseEncode(multihash)
    const { pathname, search, hash } = sig.sign({
      key: `${encodedMultihash}/${encodedMultihash}.blob`,
      checksum,
      bucket,
      expires,
    })

    const url = new URL(baseURL)
    url.search = search
    url.pathname = `/${bucket}${pathname}`
    url.hash = hash
    url.searchParams.set(
      'X-Amz-SignedHeaders',
      ['content-length', 'host', 'x-amz-checksum-sha256'].join(';')
    )

    return {
      ok: {
        url,
        headers: {
          'x-amz-checksum-sha256': checksum,
          'content-length': String(size),
        },
      },
    }
  }
}
