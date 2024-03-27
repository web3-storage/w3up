import assert from 'assert'
import { decode, NodeType } from '@ipld/unixfs'
import { exporter } from 'ipfs-unixfs-exporter'
// @ts-expect-error this version of blockstore-core doesn't point to correct types file in package.json, and upgrading to latest version that fixes that leads to api changes
import { MemoryBlockstore } from 'blockstore-core/memory'
import * as raw from 'multiformats/codecs/raw'
import path from 'path'
import { encodeFile, encodeDirectory } from '../src/unixfs.js'
import { File } from './helpers/shims.js'

/** @param {import('ipfs-unixfs-exporter').UnixFSDirectory} dir */
async function collectDir(dir) {
  /** @type {import('ipfs-unixfs-exporter').UnixFSEntry[]} */
  const entries = []
  for await (const entry of dir.content()) {
    if (entry.type === 'directory') {
      entries.push(...(await collectDir(entry)))
    } else {
      entries.push(entry)
    }
  }
  return entries
}

/** @param {Iterable<import('@ipld/unixfs').Block>} blocks */
async function blocksToBlockstore(blocks) {
  const blockstore = new MemoryBlockstore()
  for (const block of blocks) {
    await blockstore.put(block.cid, block.bytes)
  }
  return blockstore
}

describe('UnixFS', () => {
  it('encodes a file', async () => {
    const file = new Blob(['test'])
    const { cid, blocks } = await encodeFile(file)
    const blockstore = await blocksToBlockstore(blocks)
    const entry = await exporter(cid.toString(), blockstore)
    const chunks = []
    for await (const chunk of entry.content()) chunks.push(chunk)
    const out = new Blob(chunks)
    assert.equal(await out.text(), await file.text())
  })

  it('encodes a directory', async () => {
    const files = [
      new File(['top level'], 'aaaaa.txt'),
      new File(['top level dot prefix'], './bbb.txt'),
      new File(['top level slash prefix'], '/c.txt'),
      new File(['in a dir'], 'dir/two.txt'),
      new File(['another in a dir'], 'dir/three.txt'),
      new File(['in deeper in dir'], 'dir/deeper/four.png'),
      new File(['back in the parent'], 'dir/five.pdf'),
      new File(['another in the child'], 'dir/deeper/six.mp4'),
    ]

    const { cid, blocks } = await encodeDirectory(files)
    const blockstore = await blocksToBlockstore(blocks)
    const dirEntry = await exporter(cid.toString(), blockstore)
    assert.equal(dirEntry.type, 'directory')

    const expectedPaths = files.map((f) => path.join(cid.toString(), f.name))
    const entries = await collectDir(dirEntry)
    const actualPaths = entries.map((e) => e.path)

    expectedPaths.forEach((p) => assert(actualPaths.includes(p)))
  })

  it('encodes a sharded directory', async () => {
    const files = []
    for (let i = 0; i < 1001; i++) {
      files.push(new File([`data${i}`], `file${i}.txt`))
    }

    const { cid, blocks } = await encodeDirectory(files)
    const blockstore = await blocksToBlockstore(blocks)
    const dirEntry = await exporter(cid.toString(), blockstore)
    assert.equal(dirEntry.type, 'directory')

    const expectedPaths = files.map((f) => path.join(cid.toString(), f.name))
    const entries = await collectDir(dirEntry)
    const actualPaths = entries.map((e) => e.path)

    expectedPaths.forEach((p) => assert(actualPaths.includes(p)))

    // check root node is a HAMT sharded directory
    const bytes = await blockstore.get(cid)
    const node = decode(bytes)
    assert.equal(node.type, NodeType.HAMTShard)
  })

  it('throws then treating a file as a directory', () =>
    assert.rejects(
      encodeDirectory([
        new File(['a file, not a directory'], 'file.txt'),
        new File(['a file in a file!!!'], 'file.txt/another.txt'),
      ]),
      { message: '"file.txt/another.txt" cannot be a file and a directory' }
    ))

  it('configured to use raw leaves', async () => {
    const file = new Blob(['test'])
    const { cid } = await encodeFile(file)
    assert.equal(cid.code, raw.code)
  })

  it('callback for each directory entry link', async () => {
    const files = [
      new File(['file'], 'file.txt'),
      new File(['another'], '/dir/another.txt'),
    ]
    /** @type {import('../src/types.js').DirectoryEntryLink[]} */
    const links = []
    await encodeDirectory(files, { onDirectoryEntryLink: (l) => links.push(l) })
    assert.equal(links.length, 4)
    assert.equal(links[0].name, 'file.txt')
    assert.equal(links[0].dagByteLength, 4)
    assert.equal(
      links[0].cid.toString(),
      'bafkreib3tq2y6nxqumnwvu7bj4yjy7hrtcwjerxigfxzzzkd2wyzvqblqa'
    )
    assert.equal(links[1].name, 'dir/another.txt')
    assert.equal(links[1].dagByteLength, 7)
    assert.equal(
      links[1].cid.toString(),
      'bafkreifoisfmq3corzg6yzcxffyi55ayooxhtrw77bhp64zwbgeuq7yi4u'
    )
    assert.equal(links[2].name, 'dir')
    assert.equal(links[2].dagByteLength, 66)
    assert.equal(
      links[2].cid.toString(),
      'bafybeigbv3g5frjg66akpd6gwfkryqraom4nyrgtltpyoa4e7h3bhnbmti'
    )
    assert.equal(links[3].name, '')
    assert.equal(links[3].dagByteLength, 173)
    assert.equal(
      links[3].cid.toString(),
      'bafybeie4fxkioskwb4h7xpb5f6tbktm4vjxt7rtsqjit72jrv3ii5h26sy'
    )
  })

  describe('encodeDirectory', async () => {
    it('throws an error when onDirectoryEntryLink throws an error', async () => {
      await assert.rejects(async () => {
        const files = [new File(['file'], 'file.txt')]
        await encodeDirectory(files, {
          onDirectoryEntryLink: () => {
            throw new Error('')
          },
        })
      })
    })
  })
})
