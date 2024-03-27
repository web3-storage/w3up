import * as UnixFS from '@ipld/unixfs'
import * as raw from 'multiformats/codecs/raw'
import { withMaxChunkSize } from '@ipld/unixfs/file/chunker/fixed'
import { withWidth } from '@ipld/unixfs/file/layout/balanced'

const SHARD_THRESHOLD = 1000 // shard directory after > 1,000 items
const queuingStrategy = UnixFS.withCapacity()

const settings = UnixFS.configure({
  fileChunkEncoder: raw,
  smallFileEncoder: raw,
  chunker: withMaxChunkSize(1024 * 1024),
  fileLayout: withWidth(1024),
})

/**
 * @param {import('./types.js').BlobLike} blob
 * @returns {Promise<import('./types.js').UnixFSEncodeResult>}
 */
export async function encodeFile(blob) {
  const readable = createFileEncoderStream(blob)
  const blocks = await collect(readable)
  // @ts-expect-error There is always a root block
  return { cid: blocks.at(-1).cid, blocks }
}

/**
 * @param {import('./types.js').BlobLike} blob
 * @returns {ReadableStream<import('@ipld/unixfs').Block>}
 */
export function createFileEncoderStream(blob) {
  /** @type {TransformStream<import('@ipld/unixfs').Block, import('@ipld/unixfs').Block>} */
  const { readable, writable } = new TransformStream({}, queuingStrategy)
  const unixfsWriter = UnixFS.createWriter({ writable, settings })
  const fileBuilder = new UnixFSFileBuilder('', blob)
  void (async () => {
    try {
      await fileBuilder.finalize(unixfsWriter)
      await unixfsWriter.close()
      /* c8 ignore next 3 */
    } catch (e) {
      await unixfsWriter.writer.abort(/** @type {Error} */ (e))
    }
  })()
  return readable
}

class UnixFSFileBuilder {
  #file

  /**
   * @param {string} name
   * @param {import('./types.js').BlobLike} file
   */
  constructor(name, file) {
    this.name = name
    this.#file = file
  }

  /** @param {import('@ipld/unixfs').View} writer */
  async finalize(writer) {
    const unixfsFileWriter = UnixFS.createFileWriter(writer)
    await this.#file.stream().pipeTo(
      new WritableStream({
        async write(chunk) {
          await unixfsFileWriter.write(chunk)
        },
      })
    )
    return await unixfsFileWriter.close()
  }
}

class UnixFSDirectoryBuilder {
  #options

  /** @type {Map<string, UnixFSFileBuilder | UnixFSDirectoryBuilder>} */
  entries = new Map()

  /**
   * @param {string} name
   * @param {import('./types.js').UnixFSDirectoryEncoderOptions} [options]
   */
  constructor(name, options) {
    this.name = name
    this.#options = options
  }

  /** @param {import('@ipld/unixfs').View} writer */
  async finalize(writer) {
    const dirWriter =
      this.entries.size <= SHARD_THRESHOLD
        ? UnixFS.createDirectoryWriter(writer)
        : UnixFS.createShardedDirectoryWriter(writer)
    for (const [name, entry] of this.entries) {
      const link = await entry.finalize(writer)
      if (this.#options?.onDirectoryEntryLink) {
        // @ts-expect-error
        this.#options.onDirectoryEntryLink({ name: entry.name, ...link })
      }
      dirWriter.set(name, link)
    }
    return await dirWriter.close()
  }
}

/**
 * @param {Iterable<import('./types.js').FileLike>} files
 * @param {import('./types.js').UnixFSDirectoryEncoderOptions} [options]
 * @returns {Promise<import('./types.js').UnixFSEncodeResult>}
 */
export async function encodeDirectory(files, options) {
  const readable = createDirectoryEncoderStream(files, options)
  const blocks = await collect(readable)
  // @ts-expect-error There is always a root block
  return { cid: blocks.at(-1).cid, blocks }
}

/**
 * @param {Iterable<import('./types.js').FileLike>} files
 * @param {import('./types.js').UnixFSDirectoryEncoderOptions} [options]
 * @returns {ReadableStream<import('@ipld/unixfs').Block>}
 */
export function createDirectoryEncoderStream(files, options) {
  const rootDir = new UnixFSDirectoryBuilder('', options)

  for (const file of files) {
    const path = file.name.split('/')
    if (path[0] === '' || path[0] === '.') {
      path.shift()
    }
    let dir = rootDir
    for (const [i, name] of path.entries()) {
      if (i === path.length - 1) {
        dir.entries.set(name, new UnixFSFileBuilder(path.join('/'), file))
        break
      }
      let dirBuilder = dir.entries.get(name)
      if (dirBuilder == null) {
        const dirName = dir === rootDir ? name : `${dir.name}/${name}`
        dirBuilder = new UnixFSDirectoryBuilder(dirName, options)
        dir.entries.set(name, dirBuilder)
      }
      if (!(dirBuilder instanceof UnixFSDirectoryBuilder)) {
        throw new Error(`"${file.name}" cannot be a file and a directory`)
      }
      dir = dirBuilder
    }
  }

  /** @type {TransformStream<import('@ipld/unixfs').Block, import('@ipld/unixfs').Block>} */
  const { readable, writable } = new TransformStream({}, queuingStrategy)
  const unixfsWriter = UnixFS.createWriter({ writable, settings })
  void (async () => {
    try {
      const link = await rootDir.finalize(unixfsWriter)
      if (options?.onDirectoryEntryLink) {
        options.onDirectoryEntryLink({ name: '', ...link })
      }
      await unixfsWriter.close()
    } catch (e) {
      await unixfsWriter.writer.abort(/** @type {Error} */ (e))
    }
  })()

  return readable
}

/**
 * @template T
 * @param {ReadableStream<T>} collectable
 * @returns {Promise<T[]>}
 */
async function collect(collectable) {
  /** @type {T[]} */
  const chunks = []
  await collectable.pipeTo(
    new WritableStream({
      write(chunk) {
        chunks.push(chunk)
      },
    })
  )
  return chunks
}
