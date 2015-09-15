'use strict';

module.exports = DSMongoDBDriver;

var async = require( 'async' );
var extend = require( 'extend' );
var inflect = require( 'i' )();
var isISODate = require( 'is-iso-date' );
var mongodb = require( 'mongodb' );
var uniqlParse = require( 'uniql' );
var mongoCompile = require( 'uniql-mongodb' );
var traverse = require( 'traverse' );

var _defaults = {
    idField: 'id'
};

function DSMongoDBDriver( options ) {
    var self = this;

    self.options = extend( true, _defaults, options );
    self.db = null;

    return self;
}

DSMongoDBDriver.prototype._connect = function( next ) {
    var self = this;

    if ( self.db ) {
        next();
        return;
    }

    if ( !self.options.uri ) {
        next( new Error( 'You must specify a valid mongodb uri.' ) );
        return;
    }

    mongodb.MongoClient.connect( self.options.uri, function( error, db ) {
        if ( error ) {
            next( error );
            return;
        }

        self.db = db;
        next();
    } );
};

function _getCollectionName( type ) {
    return inflect.pluralize( inflect.underscore( type ) );
}

function _isValidId( id ) {
    return ( id && /^[0-9a-fA-F]{24}$/.test( id ) );
}

DSMongoDBDriver.prototype.get = function( type, id, options, callback ) {
    var self = this;

    var object = null;
    var collection = null;
    async.series( [
        self._connect.bind( self ),

        // get collection
        function( next ) {
            var collectionName = _getCollectionName( type );
            collection = self.db.collection( collectionName );
            next();
        },

        function( next ) {
            if ( !collection ) {
                next();
                return;
            }

            var criteria = {};
            criteria[ self.options.idField ] = _isValidId( id ) ? new mongodb.ObjectID( id ) : id;

            collection.findOne( criteria, options, function( error, _object ) {
                object = _object;
                next( error );
            } );
        }
    ], function( error ) {
        callback( error, object );
    } );
};

DSMongoDBDriver.prototype.put = function( type, object, callback ) {
    var self = this;

    if ( !self.options.wait ) {
        callback();
    }

    var collection = null;
    async.series( [
        // connect
        self._connect.bind( self ),

        // ensure mongodb id
        function( next ) {
            object._id = object._id || ( new mongodb.ObjectId() ).toHexString();
            next();
        },

        // validate mongo id
        function( next ) {
            if ( !_isValidId( object._id ) ) {
                next( new Error( 'Invalid mongodb id.' ) );
                return;
            }

            next();
        },

        // get collection
        function( next ) {
            var collectionName = _getCollectionName( type );
            collection = self.db.collection( collectionName );

            if ( !collection ) {
                self.db.createCollection( collectionName, function( error, _collection ) {
                    collection = _collection;
                    next( error );
                } );
                return;
            }

            next();
        },

        // walk object and convert any relevant fields to mongodb types
        function( next ) {
            // use map and re-assign so top level object doesn't get everything switched
            // from strings to internal representations
            object = traverse( object ).map( function( value ) {
                if ( typeof value === 'string' ) {
                    if ( _isValidId( value ) ) { // convert to mongo ids
                        this.update( new mongodb.ObjectID( value ) );
                    }
                    else if ( isISODate( value ) ) { // convert ISO date strings to date objects
                        this.update( new Date( value ) );
                    }
                }
            } );
            next();
        },

        // store to database using update/upsert
        function( next ) {
            var criteria = {};
            criteria[ self.options.idField ] = object[ self.options.idField ];

            collection.update( criteria, object, {
                upsert: true
            }, next );
        }

    ], function( error ) {
        if ( self.options.wait ) {
            callback( error );
        }
    } );
};

DSMongoDBDriver.prototype.delete = function( type, id, options, callback ) {
    var self = this;

    var numRemoved = 0;
    var collection = null;
    async.series( [
        self._connect.bind( self ),

        // get collection
        function( next ) {
            var collectionName = _getCollectionName( type );
            collection = self.db.collection( collectionName );
            next();
        },

        function( next ) {
            if ( !collection ) {
                next();
                return;
            }

            var criteria = {};
            criteria[ self.options.idField ] = _isValidId( id ) ? new mongodb.ObjectID( id ) : id;

            collection.remove( criteria, options, function( error, _numRemoved ) {
                numRemoved = _numRemoved;
                next( error );
            } );
        }
    ], function( error ) {
        callback( error, numRemoved );
    } );
};

DSMongoDBDriver.prototype.query = function( type, query, options, callback ) {
    var self = this;

    var result = null;
    var collection = null;
    var queryObject = null;
    async.series( [
        self._connect.bind( self ),

        // parse query if necessary
        function( next ) {
            if ( typeof query === 'object' ) {
                queryObject = query;
                next();
                return;
            }

            if ( typeof query !== 'string' ) {
                next( new Error( 'Invalid query' ) );
                return;
            }

            try {
                var ast = uniqlParse( query );
                queryObject = mongoCompile( ast );
                next();
            }
            catch ( ex ) {
                next( ex );
            }
        },

        // get collection
        function( next ) {
            var collectionName = _getCollectionName( type );
            collection = self.db.collection( collectionName );
            next();
        },

        function( next ) {
            if ( !collection ) {
                next();
                return;
            }

            // TODO: streaming?
            collection.find( queryObject, options.view || {} ).toArray( function( error, _result ) {
                result = _result;
                next( error );
            } );
        }
    ], function( error ) {
        callback( error, result );
    } );
};

DSMongoDBDriver.prototype.search = function( type, query, callback ) {
    // no full text search right now
    callback( null, [] );
};
