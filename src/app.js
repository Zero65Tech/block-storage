const express = require('express');

const app = express();
app.use(express.json());

app.use('/', require('./legacy'));

module.exports = app;