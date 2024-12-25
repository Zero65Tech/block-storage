const readline = require('readline');
const zlib = require('zlib');
const bucket = require('../config/storage');
const log = new (require('@zero65tech/log'));

class CollectionService {

  #collectionPath;
  #collectionName;
  #collection;
  #collectionLastUpdated = new Date();
  #collectionLastPersisted = this.#collectionLastUpdated;

  constructor(collectionPath, collectionName) {
    this.#collectionPath = collectionPath;
    this.#collectionName = collectionName;
  }

  async init() {

    while(this.#collection === null) // Waiting while other thread is loading data from GCS
      await new Promise(resolve => setInterval(resolve, 25));

    if(this.#collection)
      return;

    this.#collection = null; // Putting other threads on wait

    await new Promise((resolve, reject) => {

      const map = new Map();

      const rl = readline.createInterface({
        input: bucket.file(this.#collectionPath + this.#collectionName).createReadStream()
      });

      rl.on('error', (e) => {
        if(e.code == 404) {
          this.#collection = map;
          resolve();
        } else {
          this.#collection = undefined;
          reject(e);
        }
      });

      rl.on('close', () => {
        this.#collection = map;
        resolve();
      });

      rl.on('line', (line) => {
        const { key, timestamp, data } = JSON.parse(line);
        map.set(key, { data, timestamp });
      });

    });

    this.init = () => { return; };

  }

  async get(key) {
    await this.init();
    const entry = this.#collection.get(key);
    return entry ? entry.data : null;
  }

  async set(key, data) {
    await this.init();
    this.#collection.set(key, { data, timestamp: new Date() });
    this.#collectionLastUpdated = new Date();
  }

  async persist() {

    await this.init();

    if(this.#collectionLastUpdated == this.#collectionLastPersisted)
      return;

    const dateRef = this.#collectionLastUpdated;

    await new Promise((resolve, reject) => {

      const gzipStream = zlib.createGzip();

      const metadata = { contentType: 'text/plain', contentEncoding: 'gzip' };
      const gcsStream = bucket.file(this.#collectionPath + this.#collectionName).createWriteStream({ metadata });

      gzipStream.on('error', (e) => {
        reject(e);
      });

      gcsStream.on('error', (e) => {
        reject(e);
      });

      gcsStream.on('finish', () => {
        this.#collectionLastPersisted = dateRef;
        log.notice(`${ this.#collectionPath + this.#collectionName } persisted to GCS !`);
        resolve();
      });

      gzipStream.pipe(gcsStream);
      for(const key of Array.from(this.#collection.keys()).sort()) {
        const { data, timestamp } = this.#collection.get(key);
        gzipStream.write(JSON.stringify({ key, timestamp, data }) + '\n');
      }

      gzipStream.end();

    });
  }

}

module.exports = CollectionService;
