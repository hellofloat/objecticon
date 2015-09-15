'use strict';

module.exports = Objecticon;

var async = require( 'async' );
var diff = require( 'deep-diff' );
var EventEmitter = require( 'events' ).EventEmitter;
var extend = require( 'extend' );
var util = require( 'util' );
var uuid = require( 'node-uuid' );

var DataStore = require( './datastore/index.js' );

// TODO: websocket support for pushing diffs out?

var _defaults = {
    idField: 'id',
    strict: true
};

function Objecticon( options ) {
    var self = this;
    EventEmitter.call( self );

    self.options = extend( {}, _defaults, options );
    self.rules = {};
    self.ds = new DataStore( {
        idField: self.options.idField,
        create: self.options.create,
        drivers: self.options.drivers
    } );

    return self;
}

util.inherits( Objecticon, EventEmitter );

Objecticon.prototype._createObject = function( opts, callback ) {
    var self = this;
    opts.results = self.ds.create( opts.type );
    opts.results.id = uuid.v4();
    opts.creating = true;
    callback();
};

Objecticon.prototype._getObject = function( opts, callback ) {
    var self = this;

    var results = null;

    async.series( [
        // attempt to get the object
        function( next ) {
            self.ds.get( opts.type, opts.id, function( error, _object ) {
                if ( error ) {
                    callback( error );
                    return;
                }

                results = _object;
                next();
            } );
        },

        // check if the object exists or if
        function( next ) {
            if ( results || opts.allowMissing ) {
                next();
                return;
            }

            next( {
                error: 'invalid id',
                message: 'There is no ' + opts.type + ' with id: ' + opts.id,
                code: 404
            } );
        },

        // create the object if necessary (for instance, when allowMissing is enabled)
        function( next ) {
            if ( results ) {
                next();
                return;
            }

            self._createObject( opts, next );
        }

    ], function( error ) {
        if ( error ) {
            callback( error );
            return;
        }

        opts.results = opts.results || results;
        callback();
    } );
};

Objecticon.prototype._query = function( opts, callback ) {
    var self = this;
    self.ds.query( opts.type, opts.query, {
        view: opts.view
    }, function( error, _results ) {
        if ( error ) {
            callback( error );
            return;
        }

        opts.results = _results;
        callback();
    } );
};

Objecticon.prototype._update = function( opts, callback ) {
    var self = this;

    async.series( [
        self._applyChanges.bind( self, opts ),
        self._checkTypeRules.bind( self, opts, 'write' ),
        self._checkDiffRules.bind( self, opts, 'write' ),
        self._write.bind( self, opts )
    ], callback );
};

Objecticon.prototype._remove = function( opts, callback ) {
    var self = this;

    async.series( [
        self._checkTypeRules.bind( self, opts, 'delete' ),
        self._delete.bind( self, opts )
    ], callback );
};

Objecticon.prototype._getLog = function( opts, callback ) {
    var self = this;
    var limit = Math.min( opts.limit || 10, 100 );

    self.ds.getLog( opts.type, opts.id, {
        sort: {
            createdAt: -1
        },
        limit: limit
    }, function( error, _results ) {
        if ( error ) {
            callback( error );
            return;
        }

        opts.results = _results;
        callback();
    } );
};

Objecticon.prototype._getChanges = function( opts, callback ) {
    // we accept a diff array on creation as well
    if ( Array.isArray( opts.overlay ) ) {
        opts.changes = opts.overlay;
    }
    else {
        var incomingSettings = extend( true, {}, opts.results, opts.overlay );
        opts.changes = diff( opts.results, incomingSettings ) || [];
    }
    callback();
};

Objecticon.prototype._applyChanges = function( opts, callback ) {
    opts.updated = extend( true, {}, opts.results );
    opts.changes.forEach( function( change ) {
        diff.applyChange( opts.updated, true, change );
    } );
    if ( typeof opts.updated.updateAt !== 'undefined' ) {
        opts.updated.updatedAt = new Date();
    }
    callback();
};

Objecticon.prototype._checkRules = function( opts, callback ) {
    var self = this;

    var type = opts.type.toLowerCase();
    var action = opts.action.toLowerCase();
    var field = opts.field || null;

    self.rules[ type ] = self.rules[ type ] || {};
    self.rules[ type ][ action ] = self.rules[ type ][ action ] || {};
    var rules = self.rules[ type ][ action ][ field ] || [];

    if ( !field && self.options.strict && rules.length === 0 ) {
        callback( {
            error: 'permission denied',
            message: 'You do not have the required permissions.',
            code: 400
        } );
        return;
    }

    async.each( rules, function( rule, next ) {
        rule( opts, next );
    }, callback );
};

Objecticon.prototype._checkTypeRules = function( opts, action, callback ) {
    var self = this;
    self._checkRules( extend( {}, opts, {
        action: action
    } ), callback );
};

Objecticon.prototype._checkDiffRules = function( opts, action, callback ) {
    var self = this;
    async.each( opts.changes, function( change, next ) {
        var field = change.path.join( '.' );
        self._checkRules( extend( {}, opts, {
            action: action,
            field: field,
            change: change
        } ), next );
    }, callback );
};

Objecticon.prototype._write = function( opts, callback ) {
    var self = this;
    self.ds.put( opts.type, opts.updated, extend( {}, opts.meta, {
        diff: JSON.stringify( opts.changes )
    } ), function( error ) {
        if ( error ) {
            callback( error );
            return;
        }

        opts.results = opts.updated;
        callback();
    } );
};

Objecticon.prototype._delete = function( opts, callback ) {
    var self = this;
    self.ds.delete( opts.type, opts.id, extend( {}, opts.meta ), callback );
};

Objecticon.prototype.addRule = function( type, action, field, rule ) {
    var self = this;

    if ( typeof field === 'function' ) {
        rule = field;
        field = null;
    }

    type = type.toLowerCase();
    action = action.toLowerCase();

    self.rules[ type ] = self.rules[ type ] || {};
    self.rules[ type ][ action ] = self.rules[ type ][ action ] || {};
    self.rules[ type ][ action ][ field ] = self.rules[ type ][ action ][ field ] || [];
    self.rules[ type ][ action ][ field ].push( rule );
};

Objecticon.prototype.removeRule = function( type, action, field, rule ) {
    var self = this;

    if ( typeof field === 'function' ) {
        rule = field;
        field = null;
    }

    type = type.toLowerCase();
    action = action.toLowerCase();

    self.rules[ type ] = self.rules[ type ] || {};
    self.rules[ type ][ action ] = self.rules[ type ][ action ] || {};
    var rules = self.rules[ type ][ action ][ field ] || [];

    for ( var i = rules.length - 1; i >= 0; --i ) {
        var existingRule = rules[ i ];
        if ( rule === existingRule ) {
            rules.splice( i, 1 );
        }
    }
};

Objecticon.prototype.create = function( opts, callback ) {
    var self = this;

    async.series( [
        self._createObject.bind( self, opts ),
        self._getChanges.bind( self, opts ),
        self._update.bind( self, opts )
    ], function( error ) {
        if ( error ) {
            callback( error );
            return;
        }

        callback( null, opts.results );

        var event = {
            type: opts.type,
            obj: opts.results
        };

        self.emit( 'created', event );
        self.emit( 'created.' + event.type, event );
    } );
};

Objecticon.prototype.get = function( opts, callback ) {
    var self = this;

    opts.results = null;

    async.series( [
        self._getObject.bind( self, opts ),
        self._checkTypeRules.bind( self, opts, 'read' )
    ], function( error ) {
        callback( error, opts.results );
    } );
};

Objecticon.prototype.delete = function( opts, callback ) {
    var self = this;

    async.series( [
        self._getObject.bind( self, opts ),
        self._checkTypeRules.bind( self, opts, 'delete' ),
        self._delete.bind( self, opts )
    ], function( error ) {
        callback( error );
    } );
};

Objecticon.prototype.query = function( opts, callback ) {
    var self = this;

    opts.results = null;

    async.series( [
        self._query.bind( self, opts ),
        self._checkTypeRules.bind( self, opts, 'query' )
    ], function( error ) {
        if ( error ) {
            callback( error );
            return;
        }

        callback( null, opts.results );
    } );
};

Objecticon.prototype.update = function( opts, callback ) {
    var self = this;

    opts = extend( opts, {
        allowMissing: true
    } );

    async.series( [
        self._getObject.bind( self, opts ),
        self._update.bind( self, opts )
    ], function( error ) {
        if ( error ) {
            callback( error );
            return;
        }

        callback( null, opts.results );

        var event = {
            type: opts.type,
            obj: opts.results,
            id: opts.results.id
        };

        self.emit( 'updated', event );
        self.emit( 'updated.' + event.type, event );
        self.emit( 'updated.' + event.type + '.' + event.id, event );
    } );
};

Objecticon.prototype.getLog = function( opts, callback ) {
    var self = this;

    async.series( [
        self._checkTypeRules.bind( self, opts, 'log' ),
        self._getLog.bind( self, opts )
    ], function( error ) {
        if ( error ) {
            callback( error );
            return;
        }

        callback( null, opts.results );
    } );
};

Objecticon.prototype.Interface = {
    datastore: {}
};
