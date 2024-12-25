const readline = require('readline');
const bucket = require('../config/storage.js');

class CollectionService {

  #collectionPath;
  #collectionName;
  #collection;

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
        const { key, value, timestamp } = JSON.parse(line);
        map.set(key, { value, timestamp });
      });

      rl.on('error', (e) => {
        if(e.code == 404) {
          this.#collection = map;
          resolve();
        } else {
          this.#collection = null;
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
      return entry ? entry.value : null;
    }
    return this.get(key);
  }

  async set(key, value) {
    await this.#init();
    this.set = (key, value) => {
      this.#collection.set(key, { value, timestamp: new Date() });
    }
    return this.set(key, value);
  }

  async persist() {
    await this.#init();
    this.persist = async () => {
      // TODO write to GCS
    }
  }

}

module.exports = CollectionService;
