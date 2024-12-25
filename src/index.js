process.env.STAGE = process.env.STAGE || 'alpha';
process.env.PORT  = process.env.PORT  || 8080;

require('./legacy').listen(
    process.env.PORT,
    console.log(`Server (${ process.env.STAGE }) is up and listening at ${ process.env.PORT } port.`));
