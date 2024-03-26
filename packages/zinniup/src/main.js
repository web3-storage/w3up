import * as UploadAPI from '@web3-storage/upload-api'
import * as ed25519 from '@ucanto/principal/ed25519'

const servicePrincipal = await ed25519.generate()
/**
 * TODO: remove this any type and add the appropriate context variables to make this work!
 * @type {any}
 */
const context = {}

const server = UploadAPI.createServer({
  ...context,
  id: servicePrincipal,
  maxUploadSize: 10000
})

console.log(`started server ${server}`)
