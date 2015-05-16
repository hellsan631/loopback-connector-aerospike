/*!
 * Module dependencies
 */
var aerospike = require('aerospike');
var util = require('util');
var async = require('async');
var Connector = require('loopback-connector').Connector;
var debug = require('debug')('loopback:connector:aerospike');

/**
 * Initialize the Aerospike connector for the given data source
 * @param {DataSource} dataSource The data source instance
 * @param {Function} [callback] The callback function
 */
exports.initialize = function initializeDataSource(dataSource, callback) {
  if (!aerospike) {
    debug("aerospike connector initializeDataSource(): Error happened, No aerospike module avaialbe");
    return;
  }

  var s = dataSource.settings;

  var options = {
      hosts: [
        {
          addr: s.host || localhost,
          port: s.port || 3000
        }
      ],
      username: s.username || '',
      password: s.password || '',
      log: {
        level: aerospike.log.INFO
      },
      policies: {
          timeout: s.connectionTimeout || 20000
      }
  };

  dataSource.connector = new Aerospike(options, dataSource);

  if (callback) {
    dataSource.connector.connect(callback);
  }
};

/**
 * The constructor for Aerospike connector
 * @param {Object} settings The settings object
 * @param {DataSource} dataSource The data source instance
 * @constructor
 */
function Aerospike(settings, dataSource) {

  this.name = 'aerospike';
  this.settings = settings;
  this.dataSource = dataSource;
  this.debug = settings.debug || debug.enabled;

  if (this.debug) {
    debug('Settings: %j', settings);
  }

  Connector.call(this, 'aerospike', settings);

}

exports.Aerospike = Aerospike;

util.inherits(Aerospike, Connector);

/**
 * Connect to Aerospike
 * @param {Function} [callback] The callback function
 *
 * @callback callback
 * @param {Db} db The aerospike client object
 */
Aerospike.prototype.connect = function (callback) {
  var self = this;

  if (self.db) {

    process.nextTick(function () {
      if(callback)
        callback(self.db);
    });

  } else {

    self.db = aerospike.client(self.settings);

    self.db.connect(function (response) {
      if (response.code !== 0) {
        if (self.debug) {
          debug('Aerospike connection established: ' + self.settings.host);
        }
      } else {
        if (self.debug || !callback) {
          debug('Aerospike connection failed: ' + response);
        }
      }

      if(callback)
        callback(self.db);
    });
  }
};

/**
 * Disconnect from Aerospike
 */
 Aerospike.prototype.disconnect = function () {
  if (this.debug) {
    debug('disconnect');
  }
  if (this.db) {
    this.db.close();
  }
};
