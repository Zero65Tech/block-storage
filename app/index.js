const express = require('express');
const app     = express();
app.use(express.json());

const readline = require('readline');

const Log = new (require('@zero65tech/log'));

const { Storage } = require('@google-cloud/storage');
const storage = new Storage();

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

      rl.on('error', (e) => {
        if(e.code != 404)
          throw e;
        resolve(records);
        Log.notice(`${ name } not found on GSC !`);
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



app.get('/objects', async (req, res) => {

  let { collection } = req.query;

  collection = await getCollection(collection);
  
  res.send(collection.records);

});

app.put('/objects', async (req, res) => {

  let { collection, key, objects } = req.body;

  collection = await getCollection(collection);
  let records = collection.records;

  for(let object of objects) {

    let select = {};
    for(let k of key)
      select[k] = object[k];

    let indices = findIndices(records, select);

    if(indices.length > 1)
      return res.status(400).send('"key" should be unique !');

    if(indices[0])
      records[indices[0]] = object;
    else
      records.push(object);

  }

  collection.timestamp.updated = Date.now();

  res.sendStatus(202);

});

app.patch('/objects', async (req, res) => {

  const { collection, select, updates } = req.body;

  collection = await getCollection(collection);
  let records = collection.records;

  let indices = findIndices(records, select);
  for(let idx of indices)
    Object.assign(records[idx], updates);

  if(indices.length)
    collection.timestamp.updated = Date.now();

  res.sendStatus(202);

});

app.delete('/objects', async (req, res) => {

  const { collection, select } = req.query;

  collection = await getCollection(collection);
  let records = collection.records;

  let indices = findIndices(records, select);
  for(let idx of indices.reverse())
    indices.splice(idx,1);

  if(indices.length)
    collection.timestamp.updated = Date.now();

  res.sendStatus(204);

});



module.exports = app;
