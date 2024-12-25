const readline = require('readline');
const bucket = require('../config/storage');

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

      let ws = bucket.file(this.#collectionPath + this.#collectionName).createWriteStream();

      ws.on('error', (e) => {
        reject(e);
      });

      ws.on('finish', () => {
        this.#collectionLastPersisted = dateRef;
        Log.notice(`${ this.#collectionPath + this.#collectionName } persisted to GCS !`);
        resolve();
      });

      for(const key of Array.from(this.#collection.keys()).sort()) {
        const { data, timestamp } = this.#collection.get(key);
        ws.write(JSON.stringify({ key, timestamp, data }) + '\n');
      }

      ws.end();

    });
  }

}

module.exports = CollectionService;
