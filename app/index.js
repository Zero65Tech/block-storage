const express = require('express');
const app     = express();
app.use(express.json());

const readline = require('readline');

const Log = new (require('@zero65tech/log'));

const { Storage } = require('@google-cloud/storage');
const storage = new Storage();

const _ = require('lodash');

const BUCKET = 'zero65-invest-portfolio';

const COLLECTIONS = [
  // { name, records, dirty, locked, lastAccessed }
];



setInterval(async () => {

  COLLECTIONS.sort((a,b) => a.lastAccessed < b.lastAccessed ? 1 : -1);

  for(let collection of COLLECTIONS) {

    if(!collection.dirty)
      continue;

    collection.locked = true;

    let ws = storage.bucket(BUCKET).file(collection.name).createWriteStream();

    ws.on('finish', () => {
      collection.dirty = false;
      collection.locked = false;
      Log.notice(`${ collection.name } saved to GSC !`);
    });

    for(let line of collection.records)
      ws.write(JSON.stringify(line) + '\n');

    ws.end();

  }

}, 10 * 1000);



async function getCollection(name) {

  let collection = COLLECTIONS.find(collection => collection.name == name);

  if(collection) {

    while(collection.locked)
      await new Promise(resolve => setTimeout(resolve, 100));

    collection.lastAccessed = Date.now();

  } else {

    collection = { name, records:null, locked:true, lastAccessed:Date.now() };
    COLLECTIONS.push(collection);

    const rl = readline.createInterface({
      input: storage.bucket(BUCKET).file(name).createReadStream()
    });

    collection.records = await new Promise(resolve => {

      let records = [];
      
      rl.on('line', (line) => records.push(JSON.parse(line)));

      rl.on('close', () => {
        resolve(records);
        Log.notice(`${ name } fetched from GSC !`);
      });

    });

    collection.locked = false;
    collection.lastAccessed = Date.now();

  }

  return collection;

}



app.get('/', async (req, res) => {

  const { name } = req.query;

  let collection = await getCollection(name);
  
  res.send(collection.records);

});

app.put('/', async (req, res) => {

  const { name, select, record } = req.body;

  let collection = await getCollection(name);
  let records = collection.records;

  if(select)
    for(let [ key, value ] in Object.entries(select))
      records = records.filter(record => record[key] == value);

  if(!select || !records.length)
    collection.records.push(record);

  assert.equal(records.length, 1);

  collection.dirty = true;

  res.sendStatus(204);

});

app.patch('/', async (req, res) => {

  const { select, updates } = req.body;

  await Data.update(id, updates);

  res.sendStatus(204);

});

app.delete('/', async (req, res) => {

  const { select } = req.body;

  await Data.purge(id);

  res.sendStatus(204);

});



module.exports = app;
