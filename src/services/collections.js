const CollectionService = require('./CollectionService');

class CollectionsService {

  #collectionsPath;
  #collections = new Map();

  constructor(collectionsPath) {
    this.#collectionsPath = collectionsPath;
  }

  get(collectionName) {
    let value = this.#collections.get(collectionName);
    if(!value) {
      value = {
        collection: new CollectionService(this.#collectionsPath, this.collectionName),
        timestamp: new Date()
      };
      this.#collections.set(collectionName, value);
    }
    value.timestamp = new Date();
    return value.collection;
  }

  async persist() {
    for(const value of this.#collections.values())
      await value.collection.persist();
  }

}

module.exports = CollectionsService;
