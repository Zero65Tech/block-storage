const readline = require('readline');
const bucket = require('../config/storage.js');

const Log = new (require('@zero65tech/log'));

class CollectionService {

  #collectionPath;
  #collectionName;
  #collection;
  #collectionLastUpdated = new Date();
  #collectionLastPersisted = new Date();

  constructor(collectionPath, collectionName) {
    this.#collectionPath = collectionPath;
    this.#collectionName = collectionName;
  }

  async #init() {

    while(this.#collection === null) // Waiting while other thread is loading collection from GCS
      await new Promise(resolve => setInterval(resolve, 100));

    if(this.#collection)
      return;

    this.#collection = null; // Putting other threads on wait

    await new Promise((resolve, reject) => {

      const map = new Map();

      const rl = readline.createInterface({
        input: bucket.file(this.#collectionPath + this.#collectionName).createReadStream()
      });

      rl.on('line', (line) => {
        const { key, timestamp, data } = JSON.parse(line);
        map.set(key, { data, timestamp });
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

    });

  }

  async get(key) {
    await this.#init();
    this.get = (key) => {
      const entry = this.#collection.get(key);
      return entry ? entry.data : null;
    }
    return this.get(key);
  }

  async set(key, data) {
    await this.#init();
    this.set = (key, data) => {
      this.#collection.set(key, { data, timestamp: new Date() });
      this.#collectionLastUpdated = new Date();
    }
    return this.set(key, data);
  }

  async persist() {
    await this.#init();
    this.persist = async () => {

      if(!this.#collectionLastUpdated <= this.#collectionLastPersisted)
        return;

      const dateRef = this.#collectionLastUpdated;

      await new Promise((resolve, reject) => {

        let ws = bucket.file(this.#collectionPath + this.#collectionName).createWriteStream();

        ws.on('error', (e) => {
          reject(e);
        });

        ws.on('finish', () => {
          this.#collectionLastPersisted = dateRef;
          Log.notice(`${ this.#collectionPath + this.#collectionName } persisted to GCS !`);
          resolve();
        });

        const keys = Array.from(this.#collection.keys());
        keys.sort();

        for(const key of keys) {
          const { data, timestamp } = this.#collection.get(key);
          ws.write(JSON.stringify({ key, timestamp, data }) + '\n');
        }

        ws.end();

      });

    }
    await this.persist();
  }

}

module.exports = CollectionService;
