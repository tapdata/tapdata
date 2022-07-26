/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/3/30 下午12:30
 */

const WS = (function(){

    function WS(){
        this.ws = null;
        this.connected = false;
        this.__listeners = {};
    }

    WS.prototype.connect = function(url, headers = {}){
        this.ws = new WebSocket(url);
        this.__bindEvent();
    }

    WS.prototype.__bindEvent = function(){
        const self = this;
        self.ws.addEventListener('open', function(){
            console.log('ws is opened.')
            self.connected = true;
        });
        self.ws.addEventListener('message', function(msg){
            console.log('received message: ' + msg.data);
            self.emit('message', msg.data);
        });
        self.ws.addEventListener('error', function(e){console.log('error: ' + e)});
        self.ws.addEventListener('close', function(e){console.log('ws is closed')});
    }

    WS.prototype.send = function(msg) {
        if (!this.connected) {
            console.log('Call connect() method to connect server before send message.');
            return;
        }
        this.ws.send(typeof msg === 'object' ? JSON.stringify(msg) : msg + '');
    }

    WS.prototype.disconnect = function() {
        this.ws.removeEventListener('open');
        this.ws.removeEventListener('message');
        this.ws.removeEventListener('error');
        this.ws.removeEventListener('close');
        this.ws.close(0, null);
    }

    WS.prototype.on = function(event, listener) {
        this.__listeners[event] = this.__listeners[event] || [];
        this.__listeners[event].push(listener);
    }

    WS.prototype.emit = function(event, data) {
        let listeners = this.__listeners[event] || [];
        listeners.forEach(l => {
            if (typeof (l === 'function')) {
                try {
                    l(data);
                } catch (e) {
                    console.log('callback error', e);
                }
            }
        });
    }

    return WS;
})();
