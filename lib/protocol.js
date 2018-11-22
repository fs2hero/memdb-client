// Copyright 2015 The MemDB Authors.
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
var logger = require('memdb-logger').getLogger('memdb-client', __filename);

var Protocol = function(opts){
    EventEmitter.call(this);

    opts = opts || {};

    this.socket = opts.socket;
    this.socket.setEncoding('utf8');

    this.remainLine = '';

    var self = this;
    this.socket.on('data', function(data){
        // message is json encoded and splited by '\n'
        var fullData = self.remainLine + data;
        self.remainLine = '';
        var lines = fullData.split('\n');

        if(fullData.lastIndexOf('\n') < fullData.length -1){
            self.remainLine = lines.pop();
        }

        for(var i=0; i<lines.length; i++){
            try{
                if(lines[i] == ''){
                    continue;
                }

                var msg = '';
                msg = JSON.parse(lines[i]);
                self.emit('msg', msg);
            }
            catch(err){
                logger.error(err.stack,' remainLine ',self.remainLine,' data ',data,' index ',i);
            }
        }
    });

    this.socket.on('close', function(hadError){
        self.emit('close', hadError);
    });

    this.socket.on('connect', function(){
        self.emit('connect');
    });

    this.socket.on('error', function(err){
        self.emit('error', err);
    });

    this.socket.on('timeout', function(){
        self.emit('timeout');
    });
};

util.inherits(Protocol, EventEmitter);

Protocol.prototype.send = function(msg){
    var data = JSON.stringify(msg) + '\n';

    var ret = this.socket.write(data);
    if(!ret){
        logger.warn('socket.write return false');
    }
};

Protocol.prototype.disconnect = function(){
    this.socket.end();
};

module.exports = Protocol;
