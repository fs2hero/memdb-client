// Copyright 2015 rain1017.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

'use strict';

var util = require('util');
var EventEmitter = require('events').EventEmitter;
var P = require('bluebird');
var protocol = require('pomelo-protocol');
var Package = protocol.Package;
var logger = require('memdb-logger').getLogger('memdb-client', __filename);

var DEFAULT_MAX_MSG_LENGTH = 1024 * 1024;

var ST_HEAD = 1;      // wait for head
var ST_BODY = 2;      // wait for body
var ST_CLOSED = 3;    // closed

var HEAD_SIZE = 4;
var headHandler = function(headBuffer) {
    var len = 0;
    for(var i=1; i<4; i++) {
      if(i > 1) {
        len <<= 8;
      }
      len += headBuffer.readUInt8(i);
    }
    return len;
  };

var Protocol = function (opts) {
    EventEmitter.call(this);

    opts = opts || {};

    this.socket = opts.socket;
    this.maxMsgLength = opts.maxMsgLength || DEFAULT_MAX_MSG_LENGTH;

    this.remainLine = '';

    var self = this;


    this.headSize = HEAD_SIZE;
    this.headBuffer = Buffer.alloc(this.headSize);
    this.headOffset = 0;
    this.headHandler = headHandler;

    this.packageOffset = 0;
    this.packageSize = 0;
    this.packageBuffer = null;

    this.state = ST_HEAD;

    this.socket.on('data',this.ondata.bind(this));

    this.socket.on('close', function (hadError) {
        self.emit('close', hadError);
    });

    this.socket.on('connect', function () {
        self.emit('connect');
    });

    this.socket.on('error', function (err) {
        self.emit('error', err);
    });

    this.socket.on('timeout', function () {
        self.emit('timeout');
    });
};

util.inherits(Protocol, EventEmitter);

Protocol.prototype.sendRaw = function(msg) {
    var self = this;
  
    this.socket.write(msg, {binary: true}, function(err) {
      if(!!err) {
        logger.error('websocket send binary data failed: %j', err.stack);
        return;
      }
    });
  };
  
  /**
   * Send byte data package to client.
   *
   * @param  {Buffer} msg byte data
   */
  Protocol.prototype.send = function(msg) {
    if(!(msg instanceof String)) {
      msg = JSON.stringify(msg);
    }
    var data = protocol.strencode(msg);
    this.sendRaw(Package.encode(Package.TYPE_DATA, data));

    logger.debug('send msg ',msg,' data ',data);
  };

Protocol.prototype.disconnect = function () {
    this.socket.end();
    this.state = ST_CLOSED;
};

Protocol.prototype.ondata = function (chunk) {
    if (this.state === ST_CLOSED) {
        throw new Error('socket has closed');
    }

    if (typeof chunk !== 'string' && !Buffer.isBuffer(chunk)) {
        throw new Error('invalid data');
    }

    if (typeof chunk === 'string') {
        chunk = Buffer.from(chunk, 'utf8');
    }

    var offset = 0, end = chunk.length;

    while (offset < end && this.state !== ST_CLOSED) {
        if (this.state === ST_HEAD) {
            offset = this.readHead( chunk, offset);
        }

        if (this.state === ST_BODY) {
            offset = this.readBody( chunk, offset);
        }
    }

    return true;
};

/**
* Read head segment from data to this.headBuffer.
*
* @param  {Object} socket Socket instance
* @param  {Object} data   Buffer instance
* @param  {Number} offset offset read star from data
* @return {Number}        new offset of data after read
*/
Protocol.prototype.readHead = function ( data, offset) {
    var hlen = this.headSize - this.headOffset;
    var dlen = data.length - offset;
    var len = Math.min(hlen, dlen);
    var dend = offset + len;

    data.copy(this.headBuffer, this.headOffset, offset, dend);
    this.headOffset += len;

    if (this.headOffset === this.headSize) {
        // if head segment finished
        var size = this.headHandler(this.headBuffer);
        if (size < 0) {
            throw new Error('invalid body size: ' + size);
        }
        // check if header contains a valid type
        if (this.checkTypeData(this.headBuffer[0])) {
            this.packageSize = size + this.headSize;
            this.packageBuffer = Buffer.alloc(this.packageSize);
            this.headBuffer.copy(this.packageBuffer, 0, 0, this.headSize);
            this.packageOffset = this.headSize;
            this.state = ST_BODY;
        } else {
            dend = data.length;
            logger.error('close the connection with invalid head message, the remote ip is %s && port is %s && message is %j', this.socket.remoteAddress, this.socket.remotePort, data);
            this.disconnect();
        }

    }

    return dend;
};

/**
 * Read body segment from data buffer to this.packageBuffer;
 *
 * @param  {Object} socket Socket instance
 * @param  {Object} data   Buffer instance
 * @param  {Number} offset offset read star from data
 * @return {Number}        new offset of data after read
 */
Protocol.prototype.readBody = function (data, offset) {
    var blen = this.packageSize - this.packageOffset;
    var dlen = data.length - offset;
    var len = Math.min(blen, dlen);
    var dend = offset + len;

    data.copy(this.packageBuffer, this.packageOffset, offset, dend);

    this.packageOffset += len;

    if (this.packageOffset === this.packageSize) {
        // if all the package finished
        var buffer = this.packageBuffer;
        var msg = Package.decode(buffer);
        if(msg.type == Package.TYPE_DATA){
            msg = protocol.strdecode(msg.body);
            logger.debug('recv Package ',msg,' data ',buffer);
            
            msg = JSON.parse(msg);
            this.emit('msg', msg);
        }
        
        this.reset();
    }

    return dend;
};

Protocol.prototype.reset = function() {
    this.headOffset = 0;
    this.packageOffset = 0;
    this.packageSize = 0;
    this.packageBuffer = null;
    this.state = ST_HEAD;
  };

Protocol.prototype.checkTypeData = function (data) {
    return data === Package.TYPE_HANDSHAKE || data === Package.TYPE_HANDSHAKE_ACK || data === Package.TYPE_HEARTBEAT || data === Package.TYPE_DATA || data === Package.TYPE_KICK;
};

module.exports = Protocol;
