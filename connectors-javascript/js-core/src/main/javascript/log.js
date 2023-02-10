const log = {
    warn: function (msg, ...params) {
        this.printf('warn', msg, params);
    },
    error: function (msg, ...params) {
        this.printf('error', msg, params);
    },
    info: function (msg, ...params) {
        this.printf('info', msg, params);
    },
    debug: function (msg, ...params) {
        this.printf('debug', msg, params);
    },
    printf: function (type, msg, ...params) {
        if ('undefined' === type || null == type) type = 'debug';
        switch (type) {
            case 'debug' :
                tapLog.debug(msg, params);
                break;
            case 'warn' :
                tapLog.warn(msg, params);
                break;
            case 'error' :
                tapLog.error(msg, params);
                break;
            case 'info' :
                tapLog.info(msg, params);
                break;
            default:
                tapLog.debug(msg, params);
                break;
        }
    }
}

