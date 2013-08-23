var net      = require('net');
var util     = require('util');

function TCPClient(dst, timeout) {
    this.dst = dst;
    this.sock = null;
    this.timer = {
        error: null,
        close: null
    };
    function dataHandler(data, dst){};
    function connectHandler(dst){};
    function reconnect (self, eventName) {
        self.sock.destroy();
        if (! (self.timer[eventName]) ) {
            self.timer[eventName] = setTimeout(function(){
                self.timer[eventName] = null;
                self.start();
            }, timeout);
        }
    }
    this.addEventHandler = function(eventName, func) {
        if ( !(func instanceof Function) ) {
            util.log('TCPClient - addEventHandler: not a function');
            return;
        }
        if (eventName === 'data') {
            dataHandler = func;
        } else if (eventName === 'connect') {
            connectHandler = func;
        }
    }
    this.start = function(){
        var self = this;
        this.sock = new net.Socket({type: 'tcp4', allowHalfOpen: false});
        this.sock.on('close', function(hadError){
            util.log(dst.Label + ' - close');
            if ( !hadError ) {
                reconnect(self, 'close');
            }
        });
        this.sock.on('error', function(err){
            util.log(dst.Label + ' - ' + err);
            reconnect(self, 'error');
        });
        this.sock.on('data', function(data){
            // main operation
            dataHandler(data, dst);
        });
        this.sock.connect(dst.Port, dst.Addr, function(){
            connectHandler(self);
        });
    };
    return this;
}

module.exports = TCPClient;