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
                addr: s.host || 'localhost',
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

Aerospike.prototype.getTypes = function () {
  return ['db', 'nosql', 'aerospike'];
};

Aerospike.prototype.getDefaultIdType = function () {
  return String;
};

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
            var err;
            if (response.code === 0) {
                if (self.debug) {
                    debug('Aerospike connection established: ' + self.settings.host);
                }
            } else {
                self.db.connectionError = response;
                err = response;
                if (self.debug || !callback) {
                    console.error('Aerospike connection failed: ' + response);
                }
            }

            if(callback) {
                callback(err, self.db);
            }
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

/**
* Create a new model instance for the given data
* @param {String} model The model name
* @param {Object} data The model data
* @param {Function} [callback] The callback function
*/
Aerospike.prototype.create = function (model, data, callback) {
    callback();
};

/**
* Save the model instance for the given data
* @param {String} model The model name
* @param {Object} data The model data
* @param {Function} [callback] The callback function
*/
Aerospike.prototype.save = function (model, data, callback) {
    callback();
};

/**
* Check if a model instance exists by id
* @param {String} model The model name
* @param {*} id The id value
* @param {Function} [callback] The callback function
*
*/
Aerospike.prototype.exists = function (model, id, callback) {
    callback();
};

/**
* Find a model instance by id
* @param {String} model The model name
* @param {*} id The id value
* @param {Function} [callback] The callback function
*/
Aerospike.prototype.find = function find(model, id, callback) {
    callback();
};

/**
* Parses the data input for update operations and returns the
* sanitised version of the object.
*
* @param data
* @returns {*}
*/
Aerospike.prototype.parseUpdateData = function(model, data) {
    callback();
};

/**
* Update if the model instance exists with the same id or create a new instance
*
* @param {String} model The model name
* @param {Object} data The model instance data
* @param {Function} [callback] The callback function
*/
Aerospike.prototype.updateOrCreate = function updateOrCreate(model, data, callback) {
    callback();
};

/**
* Delete a model instance by id
* @param {String} model The model name
* @param {*} id The id value
* @param [callback] The callback function
*/
Aerospike.prototype.destroy = function destroy(model, id, callback) {
    callback();
};

Aerospike.prototype.buildWhere = function (model, where) {
    callback();
};

/**
* Find matching model instances by the filter
*
* @param {String} model The model name
* @param {Object} filter The filter
* @param {Function} [callback] The callback function
*/
Aerospike.prototype.all = function all(model, filter, callback) {
    callback();
};

/**
* Delete all instances for the given model
* @param {String} model The model name
* @param {Object} [where] The filter for where
* @param {Function} [callback] The callback function
*/
Aerospike.prototype.destroyAll = function destroyAll(model, where, callback) {
    callback();
};

/**
* Count the number of instances for the given model
*
* @param {String} model The model name
* @param {Function} [callback] The callback function
* @param {Object} filter The filter for where
*
*/
Aerospike.prototype.count = function count(model, callback, where) {
    callback();
};

/**
* Update properties for the model instance data
* @param {String} model The model name
* @param {Object} data The model data
* @param {Function} [callback] The callback function
*/
Aerospike.prototype.updateAttributes = function updateAttrs(model, id, data, callback) {
    callback();
};

/**
* Update all matching instances
* @param {String} model The model name
* @param {Object} where The search criteria
* @param {Object} data The property/value pairs to be updated
* @callback {Function} cb Callback function
*/
Aerospike.prototype.update =
Aerospike.prototype.updateAll = function updateAll(model, where, data, callback) {
    callback();

};


/**
* Perform autoupdate for the given models. It basically calls ensureIndex
* @param {String[]} [models] A model name or an array of model names. If not
* present, apply to all models
* @param {Function} [cb] The callback function
*/
Aerospike.prototype.autoupdate = function (models, callback) {
    callback();

};

/**
* Perform automigrate for the given models. It drops the corresponding collections
* and calls ensureIndex
* @param {String[]} [models] A model name or an array of model names. If not present, apply to all models
* @param {Function} [cb] The callback function
*/
Aerospike.prototype.automigrate = function (models, callback) {
    callback();

};

Aerospike.prototype.ping = function (callback) {
    if (this.db && !this.db.connectionError) {

        this.db.info("statistics", function(err, response, host) {
            callback(err.code === 0);
        });

    } else {
        callback(false);
    }
};
