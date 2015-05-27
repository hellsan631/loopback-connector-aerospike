module.exports = require('should');

var DataSource = require('loopback-datasource-juggler').DataSource;

var config = require('rc')('loopback', {test: {aerospike: {}}}).test.aerospike;
var suite = require('loopback-datasource-juggler/test/persistence-hooks.suite.js');

if (process.env.CI) {
  config = {
    host: 'localhost'
  };
}

global.getDataSource = global.getSchema = function(customConfig) {
  var db = new DataSource(require('../'), customConfig || config);
  db.log = function(a) {
    console.log(a);
  };

  return db;
};

suite(global.getDataSource(), module.exports);
