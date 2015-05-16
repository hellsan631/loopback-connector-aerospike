module.exports = require('should');

var DataSource = require('loopback-datasource-juggler').DataSource;

var config = require('rc')('loopback', {test: {aerospike: {}}}).test.aerospike;

if (process.env.CI) {
  config = {
    host: 'localhost'
  };
}
