'use strict';

module.exports = Rest;

var async = require( 'async' );
var EventEmitter = require( 'events' ).EventEmitter;
var extend = require( 'extend' );
var util = require( 'util' );

var Objecticon = require( './index.js' );

// TODO: websocket support for pushing diffs out?

// examples binds for express:
// app.post( '/store/:type', userCheckMiddleware, objecticonRest.create.bind( objecticonRest ) );
// app.get( '/store/:type/:id', userCheckMiddleware, objecticonRest.get.bind( objecticonRest ) );
// app.put( '/store/:type/:id', userCheckMiddleware, objecticonRest.update.bind( objecticonRest ) );
// app.del( '/store/:type/:id', userCheckMiddleware, objecticonRest.delete.bind( objecticonRest ) );
// app.get( '/store/:type', userCheckMiddleware, objecticonRest.query.bind( objecticonRest ) );
// app.get( '/store/:type/:id/log', userCheckMiddleware, objecticonRest.getLog.bind( objecticonRest ) );

var _defaults = {
    idField: 'id',
    strict: true
};

// example options for mongodb:
// {
//     idField: '_id',
//     drivers: [
//         new DSMongoDBDriver( {
//             uri: base.mongoInfo.uri,
//             authoritative: 'get,query,search',
//             idField: '_id'
//         } )
//     ]
// }

function Rest( options ) {
    var self = this;
    EventEmitter.call( self );

    self.options = extend( {}, _defaults, options );
    self.objecticon = new Objecticon( {
        idField: options.idField,
        create: self._createObject.bind( self ),
        drivers: options.drivers
    } );

    return self;
}

util.inherits( Rest, EventEmitter );

Rest.prototype._createObject = function( type ) {
    var self = this;

    var object = null;
    var Model = self.options.models[ type ];
    if ( Model ) {
        object = ( new Model() ).toObject();
        object.createdAt = object.updatedAt = new Date();
    }

    return object;
};

Rest.prototype.create = function( request, response, final ) {
    var self = this;

    self.objecticon.create( {
        type: request.params.type,
        overlay: request.body,
        meta: {
            user: request.user
        }
    }, function( error, object ) {
        if ( error ) {
            final( error );
            return;
        }

        response.json( object );

        var event = {
            type: request.params.type,
            obj: object
        };

        self.emit( 'created', event );
        self.emit( 'created.' + event.type, event );
    } );
};

Rest.prototype.get = function( request, response, final ) {
    var self = this;

    self.objecticon.get( {
        type: request.params.type,
        id: request.params.id,
        meta: {
            user: request.user
        }
    }, function( error, object ) {
        if ( error ) {
            final( error );
            return;
        }

        response.send( object );
    } );
};

Rest.prototype.delete = function( request, response, final ) {
    var self = this;

    self.objecticon.delete( {
        type: request.params.type,
        id: request.params.id,
        meta: {
            user: request.user
        }
    }, function( error ) {
        if ( error ) {
            final( error );
            return;
        }

        response.send( true );
    } );
};

Rest.prototype.query = function( request, response, final ) {
    var self = this;

    var query = null;
    var view = null;
    var results = null;

    async.series( [
        // parse the query
        function( next ) {
            try {
                query = JSON.parse( request.query.query, function( key, value ) {
                    if ( typeof value !== 'string' || value.indexOf( '!!RE:' ) !== 0 ) {
                        return value;
                    }

                    var regexpString = value.slice( 5 );
                    var match = regexpString.match( /^\/(.*)\/(.*)/ );
                    if ( !match ) {
                        throw new Error( 'Could not parse RegExp: ' + regexpString );
                    }

                    return new RegExp( match[ 1 ], match[ 2 ] );
                } );
            }
            catch ( ex ) {
                next( {
                    error: 'invalid query',
                    message: ex,
                    code: 400
                } );
                return;
            }

            if ( !query ) {
                next( {
                    error: 'invalid query',
                    message: 'You must specify a valid query for searching.',
                    code: 400
                } );
                return;
            }

            next();
        },

        // parse the view if there is one
        function( next ) {
            if ( !request.query.view ) {
                next();
                return;
            }

            try {
                view = JSON.parse( request.query.view );
            }
            catch ( ex ) {
                next( {
                    error: 'invalid view',
                    message: ex,
                    code: 400
                } );
                return;
            }

            next();
        },

        function( next ) {
            self.objecticon.query( {
                type: request.params.type,
                query: query,
                view: view,
                meta: {
                    user: request.user
                }
            }, function( error, _results ) {
                if ( error ) {
                    next( error );
                    return;
                }

                results = _results;
                next();
            } );
        }

    ], function( error ) {
        if ( error ) {
            final( error );
            return;
        }

        response.json( results );
    } );
};

Rest.prototype.update = function( request, response, final ) {
    var self = this;

    var changes = request.body;
    if ( !changes || !Array.isArray( changes ) ) {
        final( {
            error: 'no diff provided',
            message: 'You must provide a diff to apply to the object.',
            code: 400
        } );
        return;
    }

    self.objecticon.update( {
        type: request.params.type,
        id: request.params.id,
        changes: changes,
        meta: {
            user: request.user
        }
    }, function( error, object ) {
        if ( error ) {
            final( error );
            return;
        }

        response.json( object );

        var event = {
            type: request.params.type,
            obj: object,
            id: object.id
        };

        self.emit( 'updated', event );
        self.emit( 'updated.' + event.type, event );
        self.emit( 'updated.' + event.type + '.' + event.id, event );
    } );
};

Rest.prototype.getLog = function( request, response, final ) {
    var self = this;

    self.objecticon.getLog( {
        type: request.params.type,
        id: request.params.id,
        limit: request.query.limit,
        meta: {
            user: request.user
        }
    }, function( error, log ) {
        if ( error ) {
            final( error );
            return;
        }

        response.json( log );
    } );
};

Rest.prototype.Interface = {
    Rest: {}
};
