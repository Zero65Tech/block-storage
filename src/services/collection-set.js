const CollectionService = require('./collection');

class CollectionSetService {

  #collectionSetPath;
  #collectionSet = new Map();

  constructor(collectionSetPath) {
    this.#collectionSetPath = collectionSetPath;
  }

  get(collectionName) {
    let value = this.#collectionSet.get(collectionName);
    if(!value) {
      value = {
        collection: new CollectionService(this.#collectionSetPath, collectionName),
        timestamp: new Date()
      };
      this.#collectionSet.set(collectionName, value);
    }
    value.timestamp = new Date();
    return value.collection;
  }

  async persistAll() {
    for(const value of this.#collectionSet.values())
      await value.collection.persist();
  }

}

module.exports = CollectionSetService;
