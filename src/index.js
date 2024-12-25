process.env.STAGE = process.env.STAGE || 'alpha';
process.env.PORT  = process.env.PORT  || 8080;

const app = require('./app');
const server = app.listen(
    process.env.PORT,
    console.log(`Server (${process.env.STAGE}) is up and listening at ${process.env.PORT} port.`));

process.on('SIGTERM', () => {

  console.log('SIGTERM received. Closing server...');

  server.close(() => {
    console.log('Server closed.');
    process.exit(0);
  });

});
