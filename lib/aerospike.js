/*!
* Module dependencies
*/
var aerospike = require('aerospike');
var util = require('util');
var async = require('async');
var _ = require('lodash');
var Connector = require('loopback-connector').Connector;
var debug = require('debug')('loopback:connector:aerospike');
var crypto = require("crypto");
var filter = aerospike.filter;

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
        // use crypto for truly unique results
        idValue = crypto.randomBytes(20).toString('hex');
    }

    // The key of the record we are reading.
    return aerospike.key(ns, model, idValue);
}

/*!
* take DB results and make them conform to loopback expectations
*/
function AResultFromDB(model, data, key) {
    if(!data) data = {};

    var idValue = this.getIdValue(model, data);

    data[this.idName(model)] = key.key;
    delete data.lbte;

    // TODO: Convert int values to bool where the model param is a bool
    return data;
}

/*!
* take DB results and make them conform to loopback expectations
*/
function LBDataToA(model, data, dontDeleteID) {
    if(!data) data = {};

    if(!dontDeleteID){
        delete data[this.idName(model)];
    }

    // loopback allows the creation of DB entries without any content
    // Many loopback tests create these empty entries (ugh)...
    // Aerospike does not support this.
    // AResultFromDB will remove this tag when we query data back out...
    if(Object.keys(data).length === 0) {
        data.lbte = true;
    }

    // convert bool values to int because loopback does not understand bool
    data = _.mapValues(data, function(n) {
        if(n === true) {
            return 1;
        }
        if(n === false) {
            return 0;
        }
        return n;
    });

    return data;
}

/*!
* Convert a LB where query to Aerospike filters
*/
function LBWhereToFilters(model, where) {
    if(!where) where = {};

    var whereNormalized = [],
        filters = [];

    if(where.and) {
        whereNormalized = where.and.map(function(arg) {
            Object.keys(arg).forEach(function(key) {
                if(_.isObject(arg[key])) {
                    throw "Complex queries not yet supported";
                }
            });
            return LBDataToA(model, arg, true);
        });
    } else {
        whereNormalized.push(LBDataToA(model, where, true));
    }

    whereNormalized.forEach(function(query) {
        if(!query.lbte) {
            Object.keys(query).forEach(function(key) {
                if(key === 'or' || _.isObject(query[key])) {
                    throw "Complex queries not yet supported";
                }
                filters.push(filter.equal(key, query[key]));
            });
        }
    });

    // https://github.com/aerospike/aerospike-client-nodejs/blob/769b2051e2359ab4f37f00eef23af61227589150/docs/filters.md
    // var filter = aerospike.filters
    //
    // var queryArgs = { filters = [
    //                     filter.equal('a', 'hello')
    //                   ]
    // }
    function whereToAEquals() {

    }

    return filters;
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
    var metadata = {};

    var modelClass = this._models[model];
    if (modelClass.settings.aerospike) {
        metadata = {
            ttl: modelClass.settings.aerospike.ttl
        };
    }

    // The key of the record we are reading.
    var key = AKey.call(this, model, data);

    if(typeof key.key !== "string") {
        throw "Key must be a string, we need advanced delete query support";
    }

    data = LBDataToA.call(this, model, data);

    // write a record to database.
    this.db.put(key, data, metadata, function(err, key) {
        if(err.code === 0) err = undefined;

        if(err) {
            return callback(err);
        }
        return callback(null, key.key);

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
    key = AKey.call(this, model, {id: id});

    this.db.exists(key, function(err, metadata, key) {
        if(err.code === 0) err = undefined;

        callback(err, !err);

    });
};

/**
* Find a model instance by id
* @param {String} model The model name
* @param {*} id The id value
* @param {Function} [callback] The callback function
*/
Aerospike.prototype.find = function find(model, id, callback) {
    var self = this;

    if(id && id.inq) {
        var results = [];
        id.inq.forEach(function(ind_id) {
            self.find(model, ind_id, function(err, result) {
                if(err) callback(err);

                results.push(result);

                if(results.length === id.inq.length) {
                    callback(null, results);
                }
            })
        });
        return;
    }
    // The key of the record we are reading.
    var key = AKey.call(this, model, {id: id});

    // Read the same record from database
    this.db.get(key, function(err, rec, meta) {
        // We do this notFound crap because notFound is returned as
        // an error but rec is still SOMETIMES an empty object
        var notFound = true;
        if(err.code < 100) {
            if(err.code !== 2) {
                notFound = false;
            }
            err = undefined;
        }

        if(err) {
            return callback(err);
        }

        if(notFound) {
            callback(null, null);
        }

        return callback(err, AResultFromDB.call(self, model, rec, key));
    });
};

/**
* Update if the model instance exists with the same id or create a new instance
*
* @param {String} model The model name
* @param {Object} data The model instance data
* @param {Function} [callback] The callback function
*/
Aerospike.prototype.updateOrCreate = function updateOrCreate(model, data, callback) {
    this.create(model, data, function(err, key) {
        data.id = key;
        callback(err, data);
    });
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

    if(filter.order) {
        throw new Error("Using `order` in a query is not supported by aerospike");
    }

    if(filter.offset || filter.skip) {
        throw new Error("Using `offset` or `skip` in a query is not supported by aerospike");
    }

    if(filter.where && filter.where.id !== undefined) {
        this.find(model, filter.where.id, function(err, rec) {
            if(err) return callback(err);

            if(!rec) {
                return callback(null, null);
            }

            if(!_.isArray(rec)) {
                rec = [rec];
            }

            return callback(undefined, rec);
        });
    } else {
        // The key of the record we are reading.
        key = AKey.call(this, model, filter.where || {});

        var statement = {
            concurrent: true,
            nobins: false,
            filters: LBWhereToFilters.call(this, model, filter.where)
        };

        var scan = self.db.query(key.ns, key.set, statement);

        var stream = scan.execute();
        var records = [];
        stream.on('data', function(rec) {
            records.push(AResultFromDB.call(self, model, rec.bins, key));
        });
        stream.on('error', function(error) {
            return callback(error);
        });
        stream.on('end', function(end) {
            if(filter.limit) {
                records = _.take(records, filter.limit);
            }
            if(callback) return callback(null, records);
        });

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
                return callback && callback(err, results);
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
        callback(null,  recordCount);
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

        if(err){
            return callback && callback(err);
        }

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
        callback({message: "update without providing the id is not supported"});
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
            callback(err.code === 0 ? null : false);
        });

    } else {
        callback(false);
    }
};
