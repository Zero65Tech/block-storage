const express = require('express');

const CollectionSetService = require('./services/collection-set');

const collectionSets = new Map([
  [ 'pnl', new CollectionSetService('ready/pnl/') ]
]);

setInterval(async () => {
  for(const collectionSet of collectionSets.values())
    await collectionSet.persistAll();
}, 60 * 1000);

const app = express();
app.use(express.json());

app.use('/', require('./legacy'));

app.get('/:collectionSet/:collectionName/:key', async (req, res) => {

  const { collectionSet, collectionName, key } = req.params;
  const { processor } = req.query;

  const collectionSetSevice = collectionSets.get(collectionSet);
  const collectionService = collectionSetSevice.get(collectionName);

  const data = {};
  data[key] = await collectionService.get(key);

  if(processor) {
    const processorFn = require(`./services/${collectionSet}/${processor}`);
    data[key] = processorFn(data[key]);
  }

  res.send(data);

});

app.post('/:collectionSet/:collectionName/:key', async (req, res) => {

  const { collectionSet, collectionName, key } = req.params;
  const data = req.body;

  const collectionSetSevice = collectionSets.get(collectionSet);
  const collectionService = collectionSetSevice.get(collectionName);
  await collectionService.set(key, data);

  res.sendStatus(202);

});

module.exports = app;
