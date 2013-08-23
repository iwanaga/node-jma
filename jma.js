var util      = require('util');
var zlib      = require('zlib');
var TCPClient = require('./lib/tcpclient.js');

var SIZE_JMAHEAD   = 10;
var SIZE_HC        = 3;
var SIZE_BCH       = 20;
var SIZE_HEADING   = 18;
var SIZE_CP_REPLY  = 30;
var SIZE_MAX_TOTAL = 720010;
var SIZE_CATEGORY  = 6;
var OFFSET_HEADING_START = SIZE_JMAHEAD + SIZE_BCH + 1; // 31
var OFFSET_CATEGORY_END  = OFFSET_HEADING_START + SIZE_CATEGORY;
var OFFSET_BODY_START    = OFFSET_HEADING_START + SIZE_HEADING;
var TYPE_CONTROL = 'EN';
var TYPE_MESSAGE = 'bI';
var TYPE_NOTIFICATION = 'BI';
var BODY_HC = 'chk';
var HC_REPLY = '00000003ENCHK';
var CP_REPLY = new Buffer(SIZE_JMAHEAD + SIZE_CP_REPLY);
CP_REPLY.write('00000033ENACK', 0, 13, 'ascii');

/**
 * @param input, output: instance of TCPClient Class.
 */
function JMA(dst, timeout, parser) {
    var self = this;
    var CATEGORY = 'VTSE41';
    this.parser = function(buffer){};
    /* buffer size > 8KB. therefore SlowBuffer Class will be used. */
    this.buff = new Buffer(SIZE_MAX_TOTAL);
    this.size = 0;

    this.tcp = new TCPClient(dst, timeout);
    this.start = function(){
        this.tcp.start();
    };

    this.set = function(name, target){
        if (name === 'category' && getClassName(target) === 'String') {
            CATEGORY = target;
        } else if (name === 'parser' && getClassName(target) === 'Function') {
            this.parser = target;
        } else {
            util.log('JMA.set(): invalid argument');
        }
    };
    this.dataHandler = function(data, dst) {
        util.log(dst.Label +' - data length: ' + data.length);
        data.copy(this.buff, this.size, 0, data.length);
        this.size += data.length;
        this.handleMessage(dst);
    };
    this.connectHandler = function(instance){
        util.log(instance.dst.Label + ' - connected');
        this.tcp = instance;
    };

    this.shiftBuffer = function() {
        util.log('Buff - total: ' + this.size +
                 ', front: ' + (SIZE_JMAHEAD + getMessageSize(this.buff)));
        var shiftSize = SIZE_JMAHEAD + getMessageSize(this.buff);
        var tmpBuff = new Buffer(this.buff.slice(SIZE_JMAHEAD + getMessageSize(this.buff)));
        this.buff.fill(0);
        tmpBuff.copy(this.buff);
        this.size = this.size - shiftSize;
        tmpBuff = null;
    };

    this.handleMessage = function(dst){
        if ( headerReceived(this.size) && allReceived(this.size, this.buff) ) {
            if ( isHealthCheckRequest(this.buff) ) {
                util.log(dst.Label + ' - health check request.');
                sendHealthCheckReply(this.tcp);
            } else if ( isMessage(this.buff) ) {
                util.log(dst.Label + ' - bI message');
                sendCheckPointReply(this.buff);
                handleBody(this.buff);
            } else if ( isNotification(this.buff) ) {
                util.log(dst.Label + ' - BI message');
                handleBody(this.buff);
            } else {
                util.log(dst.Label + ' - no-op');
            }
            this.shiftBuffer();
            // in case buff has more than 2 message
            process.nextTick(function(){
                self.handleMessage(dst);
            });
        } else if ( this.size > 0 ) {
            util.log(dst.Label + ' - partial messge');
        }
    };


    function getClassName(t) {
        return Object.prototype.toString.call(t).slice(8, -1);
    }
    function generateMessageSize(size) {
        var out = '';
        var digits = size.toString().length;
        for (var i = 0; i + digits < 8; i++) {
            out += '0';
        }
        return (out + size.toString());
    }
    function headerReceived(size) {
        return (size >= SIZE_JMAHEAD);
    }
    function allReceived(size, buff){
        return (size >= SIZE_JMAHEAD + getMessageSize(buff));
    }
    function isHealthCheckRequest(buff) {
        return (buff.toString('ascii', 8, 13) === (TYPE_CONTROL + BODY_HC));
    }
    function isMessage(buff) {
        return (buff.toString('ascii', 8, 10) === TYPE_MESSAGE);
    }
    function isNotification(buff) {
        return (buff.toString('ascii', 8, 10) === TYPE_NOTIFICATION);
    }
    function getMessageSize(buff) {
        return parseInt(buff.toString('ascii', 0, 8), 10);
    }
    function sendHealthCheckReply() {
        self.tcp.sock.write(HC_REPLY, 'ascii', function(){
            util.log(self.tcp.dst.Label + ' - sent HC Reply');
        });
    }
    function sendCheckPointReply(buff) {
        setReply(buff);
        self.tcp.sock.write(CP_REPLY, false, function(){
            util.log(self.tcp.dst.Label + ' - sent CP Reply');
        });
    }
    function setReply(buff) {
        return buff.copy(CP_REPLY, 13, 0, 30);
    }
    function isTsunamiAlert(buff) {
        return (buff.toString('ascii', OFFSET_HEADING_START, OFFSET_CATEGORY_END) === CATEGORY);
    }
    function handleBody(buff) {
        if (! isTsunamiAlert(buff) ) {
            util.log(dst.Label + ' - not ' + CATEGORY);
            return;
        }
        util.log(dst.Label + ' - ' + CATEGORY);
        zlib.gunzip(new Buffer(buff.slice(
            OFFSET_BODY_START,
            SIZE_JMAHEAD + getMessageSize(buff))),
            function(err, buffer){
                if (err) {
                    util.log(err);
                } else {
                    self.parser(buffer);
                }
        });
    }
    this.tcp.addEventHandler('data', function(data, dst){ self.dataHandler(data, dst); });
    this.tcp.addEventHandler('connect', function(upstream){ self.connectHandler(upstream); });

    return this;
}

module.exports = JMA;
