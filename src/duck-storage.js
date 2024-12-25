const { DuckDBInstance } = require('@duckdb/node-api');

const bucket = require('./config/storage');

class DuckStorage {

  #connection = null;
  #tableLocks = {};

  constructor() {
    DuckDBInstance.create(':memory:')
        .then(instance => instance.connect())
        .then(connection => this.#connection = connection)
        .catch(error => console.error('Error:', error))
  }

  #waitForDBConnection = async () => {
    while(!this.#connection) {
      await new Promise(resolve => setTimeout(resolve, 10));
      console.log('Waiting for the connection to be ready ...')
    }
  }

  #checkTableExists = async (schema, table) => {

    const query = `
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_name = '${table}' AND table_schema = '${schema}';`;

    const result = await this.#connection.run(query);
    const rows = await result.getRows();
    const count = rows[0][0];

    return count > 0;

  }

  #loadTableFromGCS = async (schema, table) => {
    bucket.file(`${schema}/${table}.json`).createReadStream().pipe()
  }

  exec = async (schema, table, query) => {

    await this.#waitForDBConnection();

    while(this.#tableLocks[`${schema}.${table}`])
      await new Promise(resolve => setTimeout(resolve, 10));

    this.#tableLocks[`${schema}.${table}`] = true;

    if(!await this.#checkTableExists(schema, table))
      await this.#loadTableFromGCS(schema, table);

    this.#tableLocks[`${schema}.${table}`] = false;

    return this.#connection.run(query.replace('{table}', `${schema}.${table}`));

  }

}

module.exports = DuckStorage;
