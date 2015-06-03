/*!
* Module dependencies
*/
var aerospike = require('aerospike');
var util = require('util');
var async = require('async');
var _ = require('lodash');
var Connector = require('loopback-connector').Connector;
var debug = require('debug')('loopback:connector:aerospike');

/*!
* Generate an aerospike key object
*/
function AKey(model, data) {
    var ns = this.settings.namespace;
    var idValue = this.getIdValue(model, data);
    var metadata = {};

    var modelClass = this._models[model];
    if (modelClass.settings.aerospike) {
        model = modelClass.settings.aerospike.set || model;

        metadata = {
            ttl: modelClass.settings.aerospike.ttl
        };

        ns = modelClass.settings.aerospike.ns || ns;


        if(modelClass.settings.aerospike.idField) {
            idValue = data[modelClass.settings.aerospike.idField];
            delete data[modelClass.settings.aerospike.idField];
        }
    }



    if(!idValue) {
        idValue = Math.floor(Date.now() / 1000).toString() + (data[Object.keys(data)[0]] || '' );
    }

    // The key of the record we are reading.
    return aerospike.key(ns, model, idValue);
}

/*!
* Generate an aerospike key object
*/
function AResultFromKey(model, data, key) {
    var idValue = this.getIdValue(model, data);

    data[this.idName(model)] = key.key;
    return data;
}

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
        namespace: s.namespace || 'test',
        username: s.username || '',
        password: s.password || '',
        log: {
            level: aerospike.log.OFF
        },
        policies: {
            timeout: s.connectionTimeout || 20000,
            key:1
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
    var self = this;

    if(!data) data = {};

    if (self.debug) {
        debug('create', model, data);
    }

    var idValue = self.getIdValue(model, data);
    var idName = self.idName(model);
    var metadata = {};

    var modelClass = this._models[model];
    if (modelClass.settings.aerospike) {
        metadata = {
            ttl: modelClass.settings.aerospike.ttl
        };
    }

    // The key of the record we are reading.
    var key = AKey.call(this, model, data);

    delete data[idName];

    if(typeof key.key !== "string") {
        throw "Key must be a string, we need advanced delete query support";
    }

    // write a record to database.
    this.db.put(key, data, metadata, function(err, key) {
        if(err.code === 0) err = undefined;

        return callback(err, err ? null : key.key);
    });

};

/**
* Save the model instance for the given data
* @param {String} model The model name
* @param {Object} data The model data
* @param {Function} [callback] The callback function
*/
Aerospike.prototype.save = function (model, data, callback) {
    this.create(model, data, callback);
};

/**
* Check if a model instance exists by id
* @param {String} model The model name
* @param {*} id The id value
* @param {Function} [callback] The callback function
*
*/
Aerospike.prototype.exists = function (model, id, callback) {
  var self = this,
      ns = this.settings.namespace,
      key = AKey.call(this, model, {id: id}),
      statement,
      filter = aerospike.filter;

  statement.filters = [filter.equal("id", id)];

  var scan = self.db.query(key.ns, key.set, statement);

  var stream = scan.execute();
  var recordCount = 0;

  stream.on('data', function(record) {
    recordCount++;
  });
  stream.on('error', function(error) {
    callback(error);
  });
  stream.on('end', function(end){
    callback(null,  (recordCount > 0));
  });

};

/**
* Find a model instance by id
* @param {String} model The model name
* @param {*} id The id value
* @param {Function} [callback] The callback function
*/
Aerospike.prototype.find = function find(model, id, callback) {
    console.log('find');
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
    this.create(model, data, callback);
};

/**
* Delete a model instance by id
* @param {String} model The model name
* @param {*} id The id value
* @param [callback] The callback function
*/
Aerospike.prototype.destroy = function destroy(model, id, callback) {
    var self = this;
    var key;

    if(typeof id === 'string') {
        key = AKey.call(self, model, {id: id});
    } else {
        key = id;
    }

    self.db.remove(key, function(error, key) {
        if(error.code !== 0) {
            return callback(error);
        }
        return callback(null, {count: 1});

    });
};

/**
* Find matching model instances by the filter
*
* @param {String} model The model name
* @param {Object} filter The filter
* @param {Function} [callback] The callback function
*/
Aerospike.prototype.all = function all(model, filter, callback) {
    var self = this,
        key;

    if(filter.where && filter.where.id !== undefined) {
        // The key of the record we are reading.
        key = AKey.call(this, model, filter.where);

        // Read the same record from database
        this.db.get(key, function(err, rec, meta) {
            if(err.code < 100) err = undefined;

            var result = [];

            if(!err && rec) {
                result.push(AResultFromKey.call(self, model, rec, key));
            }

            return callback(err, result);
        });
    } else {
        // The key of the record we are reading.
        key = AKey.call(this, model, filter.where || {});

        var statement = {
            concurrent: true,
            nobins: false,
            filters: []
        };
        var scan = self.db.query(key.ns, key.set, statement);

        var stream = scan.execute();
        var records = [];

        stream.on('data', function(rec) {
            records.push(AResultFromKey.call(self, model, rec.bins, key));
        });
        stream.on('error', function(error) {
            return callback(error);
        });
        stream.on('end', function(end) {
            if(callback) return callback(null, records);
        });

        // https://github.com/aerospike/aerospike-client-nodejs/blob/769b2051e2359ab4f37f00eef23af61227589150/docs/filters.md
        // var filter = aerospike.filters
        //
        // var queryArgs = { filters = [
        //                     filter.equal('a', 'hello')
        //                   ]
        // }


    }

};

/**
* Delete all instances for the given model
* @param {String} model The model name
* @param {Object} [where] The filter for where
* @param {Function} [callback] The callback function
*/
Aerospike.prototype.destroyAll = function destroyAll(model, where, callback) {
    var self = this;

    var key = AKey.call(this, model, where);

    if(!callback && 'function' === typeof where) {
        callback = where;
        where = {};
    }
    if(where && Object.keys(where).length > 0) {
        this.all(model, {where: where}, function(err, results) {
            if(err || !results) {
                callback && callback(err, results);
                return;
            }

            var tasks = [];
            results.forEach(function(result) {
                tasks.push(function(done) {
                  self.destroy(model, result.id, done);
                });
            });

            async.parallel(tasks, callback);
        });
    } else {
        //callback(null,  {count: undefined});
        var statement = {
            concurrent: false,
            nobins: true,
        };
        var scan = self.db.query(key.ns, key.set, statement);

        var stream = scan.execute();
        var recordCount = 0;
        var tasks = [];

        stream.on('data', function(rec) {
            recordCount++;

            tasks.push(function(done) {
                self.destroy(model, rec.key, done);
            });
        });
        stream.on('error', function(error) {
            return callback(error);
        });
        stream.on('end', function(end) {
            async.parallel(tasks, function(err) {
                if(err) return callback(err);

                callback(null,  {count: recordCount});
            });

        });
    }
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
    var self = this;

    var key = AKey.call(this, model, where);

    var statement = {
      concurrent: true,
      nobins: false,
    };

    var scan = self.db.query(key.ns, key.set, statement);

    var stream = scan.execute();
    var recordCount = 0;

    stream.on('data', function(record) {
      recordCount++;
    });
    stream.on('error', function(error) {
      callback(error);
    });
    stream.on('end', function(end){
      callback(null,  {count: recordCount});
    });

};

/**
* Update properties for the model instance data
* @param {String} model The model name
* @param {Object} data The model data
* @param {Function} [callback] The callback function
*/
Aerospike.prototype.updateAttributes = function updateAttrs(model, id, data, callback) {
    var self = this;
    this.all(model, {where: {id: id}}, function(err, docs) {
        var doc = _.merge({}, docs[0], data);
        self.update(model, {where: {id: id}}, doc, callback);
    });
};

/**
* Update all matching instances
* @param {String} model The model name
* @param {Object} where The search criteria
* @param {Object} data The property/value pairs to be updated
* @callback {Function} callback Callback function
*/
Aerospike.prototype.update = Aerospike.prototype.updateAll = function updateAll(model, where, data, callback) {

    if(where && where.id && Object.keys(where).length === 1) {
        data.id = where.id;
        this.create(model, data, callback);
    } else {
        callback({message: "update without providing the id is not supported"})
    }
};

/**
* Perform autoupdate for the given models. It basically calls ensureIndex
* @param {String[]} [models] A model name or an array of model names. If not
* present, apply to all models
* @param {Function} [callback] The callback function
*/
Aerospike.prototype.autoupdate = function (models, callback) {
    var self = this;
    if (self.db) {
        if (self.debug) {
            debug('automigrate');
        }
        if ((!callback) && ('function' === typeof models)) {
            callback = models;
            models = undefined;
        }
        // First argument is a model name
        if ('string' === typeof models) {
            models = [models];
        }

        models = models || Object.keys(self._models);

        async.each(models, function (model, modelCallback) {
            var args = { ns: "test", set: "demo", bin: "bin1", index: "index_name"};
            self.db.createStringIndex(args, function (error) {
                modelCallback(error);
            });
        }, function (err) {
            if (err.code !== 0) {
                return callback && callback(err);
            }
            return callback && callback();
        });

    } else {
        self.dataSource.once('connected', function () {
            self.automigrate(models, callback);
        });
    }
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
