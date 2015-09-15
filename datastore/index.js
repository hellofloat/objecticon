'use strict';

var async = require( 'async' );
var extend = require( 'extend' );

module.exports = DataStore;

var _defaults = {
    idField: 'id'
};

function DataStore( _options ) {
    var self = this;

    self.options = extend( {}, _defaults, _options );

    self.drivers = _options.drivers || [];

    if ( !_options.create ) {
        throw new Error( 'You must specify an object factory method.' );
    }

    self.loggers = _options.loggers || [];

    self.create = _options.create;
    self.get = self._handleReadOperation.bind( self, 'get', self.drivers );
    self.query = self._handleReadOperation.bind( self, 'query', self.drivers );
    self.search = self._handleReadOperation.bind( self, 'search', self.drivers );

    self.addDriver = _addArrayItem.bind( null, self.drivers );
    self.removeDriver = _removeArrayItem.bind( null, self.drivers );

    self.addLogger = _addArrayItem.bind( null, self.loggers );
    self.removeLogger = _removeArrayItem.bind( null, self.loggers );
}

function _addArrayItem( array, item ) {
    return array.push( item );
}

function _removeArrayItem( array, item ) {
    var index = -1;
    if ( typeof item === 'number' ) {
        index = item;
    }
    else {
        index = array.indexOf( item );
    }

    if ( index > -1 ) {
        array.splice( index, 1 );
        return true;
    }

    return false;
}

function _getAuthoritative( array, operation ) {
    var authoritative = null;
    array.some( function( obj ) {
        var authorities = ( obj.options && obj.options.authoritative ) ? obj.options.authoritative.split( ',' ) : [];
        if ( authorities.indexOf( operation ) !== -1 ) {
            authoritative = obj;
            return true;
        }

        return false;
    } );

    return authoritative;
}

DataStore.prototype._handleReadOperation = function( operation, handlers, type, criteria, options, callback ) {
    callback = ( typeof options === 'function' && !callback ) ? options : callback;
    options = typeof options === 'function' ? {} : options;

    var authoritative = _getAuthoritative( handlers, operation );
    if ( !authoritative ) {
        callback( new Error( 'No drivers available.' ) );
        return;
    }

    authoritative[ operation ]( type, criteria, options, callback );
};

DataStore.prototype.put = function( type, object, meta, callback ) {
    var self = this;

    callback = ( typeof meta === 'function' && !callback ) ? meta : callback;

    async.each( self.drivers, function( driver, next ) {
        driver.put( type, object, next );
    }, function( error ) {
        callback( error );

        self.log( {
            action: 'put',
            type: type,
            objectId: object[ self.options.idField ],
            meta: meta
        }, function() {
            // if error, emit?
        } );
    } );
};

DataStore.prototype.delete = function( type, id, meta, callback ) {
    var self = this;

    callback = ( typeof meta === 'function' && !callback ) ? meta : callback;

    async.each( self.drivers, function( driver, next ) {
        driver.delete( type, id, next );
    }, function( error ) {
        callback( error );

        self.log( {
            action: 'delete',
            type: type,
            objectId: id,
            meta: meta
        }, function() {
            // if error, emit?
        } );
    } );
};

var _defaultLogEntry = {
    action: null,
    type: null,
    objectId: null,
    meta: null
};

DataStore.prototype.log = function( options, callback ) {
    var self = this;

    var auditLogEntry = extend( {
        createdAt: new Date()
    }, _defaultLogEntry, options );

    async.each( self.loggers, function( logger, next ) {
        logger.put( 'auditlogentry', auditLogEntry, next );
    }, callback );
};

DataStore.prototype.getLog = function( type, id, options, callback ) {
    var self = this;
    self._handleReadOperation( 'query', self.loggers, 'auditlogentry', {
        type: type,
        objectId: id
    }, options, callback );
};
