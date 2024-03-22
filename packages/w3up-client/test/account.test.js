import * as Test from './test.js'
import * as Account from '../src/account.js'
import * as Space from '../src/space.js'
import * as Result from '../src/result.js'

/**
 * @type {Test.Suite}
 */
export const testAccount = {
  'list accounts': async (assert, { client, mail, grantAccess }) => {
    const email = 'alice@web.mail'

    assert.deepEqual(Account.list(client), {}, 'no accounts yet')

    const login = Account.login(client, email)
    const message = await mail.take()
    assert.deepEqual(message.to, email)
    await grantAccess(message)
    const session = await login
    assert.equal(session.error, undefined)
    assert.equal(session.ok?.did(), Account.fromEmail(email))
    assert.equal(session.ok?.toEmail(), email)
    assert.equal(session.ok?.proofs.length, 2)

    assert.deepEqual(Account.list(client), {}, 'no accounts have been saved')
    await session.ok?.save()
    const accounts = Account.list(client)

    assert.deepEqual(Object.values(accounts).length, 1)
    assert.ok(accounts[Account.fromEmail(email)])

    const account = accounts[Account.fromEmail(email)]
    assert.equal(account.toEmail(), email)
    assert.equal(account.did(), Account.fromEmail(email))
    assert.equal(account.proofs.length, 2)
  },

  'two logins': async (assert, { client, mail, grantAccess }) => {
    const aliceEmail = 'alice@web.mail'
    const bobEmail = 'bob@web.mail'

    assert.deepEqual(Account.list(client), {}, 'no accounts yet')
    const aliceLogin = Account.login(client, aliceEmail)
    await grantAccess(await mail.take())
    const alice = await aliceLogin
    assert.deepEqual(alice.ok?.toEmail(), aliceEmail)

    assert.deepEqual(Account.list(client), {}, 'no accounts have been saved')
    const saveAlice = await alice.ok?.save()
    assert.equal(saveAlice?.error, undefined)

    const one = Account.list(client)
    assert.deepEqual(Object.values(one).length, 1)
    assert.ok(one[Account.fromEmail(aliceEmail)], 'alice in the account list')

    const bobLogin = Account.login(client, bobEmail)
    await grantAccess(await mail.take())
    const bob = await bobLogin
    assert.deepEqual(bob.ok?.toEmail(), bobEmail)
    await bob.ok?.save()

    const two = Account.list(client)

    assert.deepEqual(Object.values(two).length, 2)

    assert.ok(two[Account.fromEmail(aliceEmail)].toEmail(), aliceEmail)
    assert.ok(two[Account.fromEmail(bobEmail)].toEmail(), bobEmail)
  },

  'login idempotence': async (assert, { client, mail, grantAccess }) => {
    const email = 'alice@web.mail'
    const login = client.login(email)
    await grantAccess(await mail.take())
    const alice = await login

    assert.deepEqual(
      Object.keys(client.accounts()),
      [alice.did()],
      'no accounts have been saved'
    )

    const retry = await client.login(email)
    assert.deepEqual(
      alice.toJSON(),
      retry.toJSON(),
      'same account view is returned'
    )

    const loginResult = await Account.login(client, email)
    assert.deepEqual(
      alice.toJSON(),
      loginResult.ok?.toJSON(),
      'same account is returned with low level API'
    )
  },

  'client.login': async (assert, { client, mail, grantAccess }) => {
    const account = client.login('alice@web.mail')

    await grantAccess(await mail.take())

    const alice = await account
    assert.deepEqual(alice.toEmail(), 'alice@web.mail')

    const accounts = client.accounts()
    assert.deepEqual(Object.keys(accounts), [alice.did()])
  },

  'create account and provision space': async (
    assert,
    { client, mail, grantAccess }
  ) => {
    const space = await client.createSpace('test')
    const mnemonic = space.toMnemonic()
    const { signer } = await Space.fromMnemonic(mnemonic, { name: 'import' })
    assert.deepEqual(
      space.signer.encode(),
      signer.encode(),
      'arrived to same signer'
    )

    const email = 'alice@web.mail'
    const login = Account.login(client, email)
    const message = await mail.take()
    assert.deepEqual(message.to, email)
    await grantAccess(message)
    const account = Result.try(await login)

    const result = await account.provision(space.did())
    assert.equal(result.error, undefined)

    // authorize agent to use space
    const proof = await space.createAuthorization(client.agent, {
      access: { 'space/info': {} },
      expiration: Infinity,
    })

    await client.addSpace(proof)

    const info = await client.capability.space.info(space.did())
    assert.deepEqual(info, {
      did: space.did(),
      providers: [client.agent.connection.id.did()],
    })
  },

  'multi device workflow': async (asserts, { connect, mail, grantAccess }) => {
    const laptop = await connect()
    const space = await laptop.createSpace('main')

    // want to provision space ?
    const email = 'alice@web.mail'
    const login = Account.login(laptop, email)
    // confirm by clicking a link
    await grantAccess(await mail.take())
    const account = Result.try(await login)

    // Authorized account can provision space
    Result.try(await account.provision(space.did()))

    // Want to setup a recovery for this space ?
    const recovery = await space.createRecovery(account.did())
    // Authorize laptop to use the space, we need to do it in order
    // to be able to store the recovery delegation in the space.
    await laptop.addSpace(await space.createAuthorization(laptop.agent))

    // Store delegation to the account so it can be used for recovery
    await laptop.capability.access.delegate({
      delegations: [recovery],
    })

    // now connect with a second device
    const phone = await connect()
    const phoneLogin = Account.login(phone, email)
    // confirm by clicking a link
    await grantAccess(await mail.take())
    const session = Result.try(await phoneLogin)
    // save session on the phone
    Result.try(await session.save())

    const result = await phone.capability.space.info(space.did())
    asserts.deepEqual(result.did, space.did())
  },
  'setup recovery': async (assert, { client, mail, grantAccess }) => {
    const space = await client.createSpace('test')

    const email = 'alice@web.mail'
    const login = Account.login(client, email)
    const message = await mail.take()
    assert.deepEqual(message.to, email)
    await grantAccess(message)
    const account = Result.try(await login)

    Result.try(await account.provision(space.did()))

    const recovery = await space.createRecovery(account.did())
    const share = await client.capability.access.delegate({
      space: space.did(),
      delegations: [recovery],
      proofs: [await space.createAuthorization(client)],
    })
    assert.equal(share.error, undefined)
    assert.deepEqual(client.spaces(), [])

    assert.deepEqual(client.spaces().length, 0, 'no spaces had been added')

    // waiting for a sec so that request CID will come out different
    // otherwise we will find previous authorization which does not
    // have the space delegation yet.
    await new Promise((resolve) => setTimeout(resolve, 1000))

    // This is not a great flow but to fix this we need a new to upgrade
    // ucanto and then pull delegations for each account.
    const secondLogin = Account.login(client, email)
    await grantAccess(await mail.take())
    const secondAccount = Result.try(await secondLogin)

    Result.try(await secondAccount.save())

    assert.deepEqual(client.spaces().length, 1, 'spaces had been added')
  },

  'check and set account plan': async (
    assert,
    { client, mail, grantAccess, plansStorage }
  ) => {
    const login = Account.login(client, 'alice@web.mail')
    await grantAccess(await mail.take())
    const account = Result.try(await login)

    const { error } = await account.plan.get()
    assert.ok(error)

    Result.unwrap(
      await plansStorage.initialize(
        account.did(),
        'stripe:123xyz',
        'did:web:free.web3.storage'
      )
    )

    const { ok: plan } = await account.plan.get()

    assert.ok(plan?.product, 'did:web:free.web3.storage')

    Result.unwrap(await account.plan.set('did:web:lite.web3.storage'))

    const { ok: newPlan } = await account.plan.get()

    assert.ok(newPlan?.product, 'did:web:lite.web3.storage')
  },

  'check account subscriptions': async (
    assert,
    { client, mail, grantAccess }
  ) => {
    const space = await client.createSpace('test')

    const email = 'alice@web.mail'
    const login = Account.login(client, email)
    const message = await mail.take()
    assert.deepEqual(message.to, email)
    await grantAccess(message)
    const account = Result.try(await login)

    Result.try(await account.provision(space.did()))

    const subs = Result.unwrap(await account.plan.subscriptions())

    assert.equal(subs.results.length, 1)
    assert.equal(subs.results[0].provider, client.defaultProvider())
    assert.deepEqual(subs.results[0].consumers, [space.did()])
    assert.equal(typeof subs.results[0].subscription, 'string')
  },

  'space.save': async (assert, { client, mail, grantAccess }) => {
    const space = await client.createSpace('test')
    assert.deepEqual(client.spaces(), [])

    const result = await space.save()
    assert.ok(result.ok)

    const spaces = client.spaces()
    assert.deepEqual(spaces.length, 1)
    assert.deepEqual(spaces[0].did(), space.did())

    assert.deepEqual(client.currentSpace()?.did(), space.did())
  },

  'only redundant logins': async (
    assert,
    { client, mail, grantAccess, plansStorage }
  ) => {
    const email = 'alice@web.mail'
    const sessions = []
    const pairs = 2
    for (const _ of Array(pairs)) {
      const login = Account.login(client, email)
      // do not grant access right away so we end up with distinct sessions
      await new Promise((resolve) => setTimeout(resolve, 1000))
      await grantAccess(await mail.take())
      const account = Result.try(await login)
      sessions.push(account)
    }

    // we create temporary account sessions and then save them to avoid
    // login idempotence.
    for (const session of sessions) {
      Result.try(await session.save())
    }

    const account = Object.values(client.accounts())[0]
    assert.equal(
      account.proofs.length,
      pairs * 2,
      `should have ${pairs * 2} proofs`
    )

    // setup a billing for this account
    plansStorage.set(account.did(), client.agent.connection.id.did())

    const space = await client.createSpace('test')
    account.provision(space.did())
  },
}

Test.test({ Account: testAccount })
