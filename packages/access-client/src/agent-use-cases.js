import { Agent as AccessAgent, agentToData } from './agent.js'
import * as Ucanto from '@ucanto/interface'
import * as Access from '@web3-storage/capabilities/access'
import { bytesToDelegations } from './encoding.js'
import { Provider } from '@web3-storage/capabilities'
import { Delegation } from '@ucanto/core'
import * as w3caps from '@web3-storage/capabilities'

/**
 * Request authorization of a session allowing this agent to issue UCANs
 * signed by the passed email address.
 *
 * @param {AccessAgent} access
 * @param {Ucanto.Principal<Ucanto.DID<'mailto'>>} account
 * @param {Iterable<{ can: Ucanto.Ability }>} capabilities
 */
export async function requestAuthorization(access, account, capabilities) {
  const res = await access.invokeAndExecute(Access.authorize, {
    audience: access.connection.id,
    with: access.issuer.did(),
    nb: {
      iss: account.did(),
      att: [...capabilities],
    },
  })
  if (res?.error) {
    throw new Error('failed to authorize session', { cause: res })
  }
}

/**
 * claim delegations delegated to an audience
 *
 * @param {AccessAgent} access
 * @param {Ucanto.DID} [delegee] - audience of claimed delegations. defaults to access.connection.id.did()
 * @param {object} options
 * @param {boolean} [options.addProofs] - whether to addProof to access agent
 * @returns
 */
export async function claimDelegations(
  access,
  delegee = access.connection.id.did(),
  { addProofs = false } = {}
) {
  const res = await access.invokeAndExecute(Access.claim, {
    audience: access.connection.id,
    with: delegee,
  })
  if (res.error) {
    throw new Error('error claiming delegations')
  }
  const delegations = Object.values(res.delegations).flatMap((bytes) =>
    bytesToDelegations(bytes)
  )
  if (addProofs)
    for (const d of delegations) {
      await access.addProof(d)
    }

  if (addProofs) {
    await addSpacesFromDelegations(access, delegations)
  }

  return delegations
}

/**
 * @private
 * Given a list of delegations, add to agent data spaces list.
 *
 * TODO: DON'T USE - we'd like to move away from storing space information inside the agent, planning on removing this soon!
 *
 * @param {AccessAgent} access
 * @param {Ucanto.Delegation<Ucanto.Capabilities>[]} delegations
 */
export async function addSpacesFromDelegations(access, delegations) {
  const data = agentToData.get(access)
  if (!data) {
    throw Object.assign(new Error(`cannot determine AgentData for Agent`), {
      agent: access,
    })
  }
  if (delegations.length > 0) {
    const allows = Delegation.allows(delegations[0], ...delegations.slice(1))
    for (const [did, value] of Object.entries(allows)) {
      // TODO I don't think this should be `store/*` but this works for today
      if (value['store/*']) {
        data.addSpace(/** @type {Ucanto.DID} */ (did), {
          isRegistered: true,
        })
      }
    }
  }
}

/**
 * @param {AccessAgent} access
 * @param {Ucanto.DID<'key'>} space
 * @param {Ucanto.Principal<Ucanto.DID<'mailto'>>} account
 * @param {Ucanto.DID<'web'>} provider - e.g. 'did:web:staging.web3.storage'
 */
export async function addProvider(access, space, account, provider) {
  const result = await access.invokeAndExecute(Provider.add, {
    audience: access.connection.id,
    with: account.did(),
    nb: {
      provider,
      consumer: space,
    },
  })
  if (result.error) {
    throw new Error(`error adding provider`, { cause: result })
  }
}

/**
 * @param {AccessAgent} access
 * @param {Ucanto.DID} delegee
 * @param {object} [options]
 * @param {number} [options.interval]
 * @param {AbortSignal} [options.abort]
 * @returns {Promise<Iterable<Ucanto.Delegation>>}
 */
export async function expectNewClaimableDelegations(access, delegee, options) {
  const interval = options?.interval || 250
  const claim = () => claimDelegations(access, delegee)
  const initialClaimResult = await claim()
  const claimed = await new Promise((resolve, reject) => {
    options?.abort?.addEventListener('abort', (e) => {
      reject(new Error('expectNewClaimableDelegations aborted', { cause: e }))
    })
    poll(interval)
    /**
     * @param {number} retryAfter
     */
    async function poll(retryAfter) {
      const pollClaimResult = await access.invokeAndExecute(
        w3caps.Access.claim,
        { with: delegee }
      )
      if (pollClaimResult.error) {
        return reject(pollClaimResult)
      }
      // got a response. If it contains same amount of delegations as initialClaimResult,
      // user has not clicked confirm
      const claimedDelegations = Object.values(
        pollClaimResult.delegations
      ).flatMap((d) => bytesToDelegations(d))
      if (claimedDelegations.length > initialClaimResult.length) {
        resolve(claimedDelegations)
      } else {
        setTimeout(() => poll(retryAfter), retryAfter)
      }
    }
  })
  return claimed
}
