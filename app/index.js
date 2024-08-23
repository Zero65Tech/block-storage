const express = require('express');
const app     = express();

const readline = require('readline');

const { Storage } = require('@google-cloud/storage');
const storage = new Storage();

const _ = require('lodash');

const BUCKET = 'zero65-invest-portfolio';

const COLLECTIONS = [
  // { name, documents, dirty, locked, lastAccessed }
];



setInterval(async () => {

  COLLECTIONS.sort((a,b) => a.lastAccessed < b.lastAccessed ? 1 : -1);

  for(let collection of COLLECTIONS) {

    if(!collection.dirty)
      continue;

    collection.locked = true;

    let ws = storage.bucket(bucketName).file(fileName).createWriteStream();

    ws.on('finish', () => {
      collection.dirty = false;
    });

    for(let line of content)
      ws.write(JSON.stringify(line) + '\n');

    ws.end();

    collection.locked = false;

  }

}, 10 * 1000);



async function getCollection(name) {

  let collection = COLLECTIONS.find(collection => collection.name == name);

  if(collection) {

    while(collection.locked)
      await new Promise(resolve => setTimeout(resolve, 100));

    collection.lastAccessed = Date.now();

  } else {

    collection = { name, documents:null, locked:true, lastAccessed:Date.now() };
    COLLECTIONS.push(collection);

    const rl = readline.createInterface({
      input: storage.bucket(BUCKET).file(name).createReadStream()
    });

    collection.documents = await new Promise(resolve => {

      let documents = [];
      
      rl.on('line', (line) => documents.push(JSON.parse(line)));

      rl.on('close', () => resolve(documents));

    });

    collection.locked = false;

  }

  return collection;

}



app.get('/', async (req, res) => {

  let collection = await getCollection(`temp/${ req.query.file }.json`);
  res.send(collection.documents);

});

app.put('/', async (req, res) => {

  const { id, ...updates } = req.body;

  await Data.update(id, updates);

  res.sendStatus(204);

});

app.patch('/', async (req, res) => {

  const { id, ...updates } = req.body;

  await Data.update(id, updates);

  res.sendStatus(204);

});

app.delete('/', async (req, res) => {

  const { id } = req.body;

  await Data.purge(id);

  res.sendStatus(204);

});



module.exports = app;
