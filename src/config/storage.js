const { Storage } = require('@google-cloud/storage');
const storage = new Storage();

module.exports = process.env.STAGE === 'prod' || process.env.STAGE === 'gamma'
  ? storage.bucket('portfolio.zero65.in')
  : storage.bucket('test.zero65.in')
