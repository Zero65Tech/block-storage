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
  // { name, records, timestamp: { accessed, updated, saved } }
];



setInterval(async () => {

  COLLECTIONS.sort((a,b) => a.lastAccessed < b.lastAccessed ? 1 : -1);

  for(let collection of COLLECTIONS) {

    let ts = collection.timestamp.updated;
    if(collection.timestamp.saved == ts)
      continue;

    let ws = storage.bucket(BUCKET).file(collection.name).createWriteStream();

    ws.on('finish', () => {
      collection.timestamp.saved = ts;
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

    // wait while other request is fetching from GSC
    while(collection.records === null)
      await new Promise(resolve => setTimeout(resolve, 100));

  } else {

    // put other requests on wait
    collection = { name, records:null, timestamp: { accessed:Date.now(), updated:0, saved:0 } };
    COLLECTIONS.push(collection);

    const rl = readline.createInterface({
      input: storage.bucket(BUCKET).file(name).createReadStream()
    });

    collection.records = await new Promise(resolve => {

      let records = [];
      
      rl.on('line', (line) => {
        records.push(JSON.parse(line));
      });

      rl.on('close', () => {
        resolve(records);
        Log.notice(`${ name } fetched from GSC !`);
      });

    });

  }

  collection.timestamp.accessed = Date.now();
  return collection;

}



function findIndices(records, select) {

  let indices = [];

  outer: for(let i = 0; i < records.length; i++) {

    let record = records[i];
    
    for(let [key,value] of Object.entries(select))
      if(record[key] !== value)
        continue outer;

    indices.push(i);

  }

  return indices;

}



app.get('/', async (req, res) => {

  const { name } = req.query;

  let collection = await getCollection(name);
  
  res.send(collection.records);

});

app.put('/', async (req, res) => {

  const { name, key, record } = req.body;

  let collection = await getCollection(name);
  let records = collection.records;

  let select = {};
  for(let k of key)
    select[k] = record[k];

  let indices = findIndices(records, select);

  if(indices.length > 1)
    return res.status(400).send('"key" should be unique !');

  if(indices[0])
    records[indices[0]] = record;
  else
    records.push(record);

  collection.timestamp.updated = Date.now();

  res.sendStatus(202);

});

app.patch('/', async (req, res) => {

  const { name, select, updates } = req.body;

  let collection = await getCollection(name);
  let records = collection.records;

  let indices = findIndices(records, select);
  for(let idx of indices)
    Object.assign(records[idx], updates);

  if(indices.length)
    collection.timestamp.updated = Date.now();

  res.sendStatus(202);

});

app.delete('/', async (req, res) => {

  const { name, select } = req.query;

  let collection = await getCollection(name);
  let records = collection.records;

  let indices = findIndices(records, select);
  for(let idx of indices.reverse())
    indices.splice(idx,1);

  if(indices.length)
    collection.timestamp.updated = Date.now();

  res.sendStatus(204);

});



module.exports = app;
