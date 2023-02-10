const log = {
    warn: function (msg, params) {
        this.printf('warn', arguments);
    },
    error: function (msg, params) {
        this.printf('error', arguments);
    },
    info: function (msg, params) {
        this.printf('info', arguments);
    },
    debug: function (msg, params) {
        this.printf('debug', arguments);
    },
    printf: function (type, args) {
        if ('undefined' === type || null == type) type = 'debug';
        let arg = [];
        for (let argIndex = 1; argIndex < arguments.length; argIndex++) {
            arg.push(arguments[argIndex]);
        }
        switch (type) {
            case 'debug' :
                tapLog.debug(arg);
                break;
            case 'warn' :
                tapLog.warn(arg);
                break;
            case 'error' :
                tapLog.error(arg);
                break;
            case 'info' :
                tapLog.info(arg);
                break;
            default:
                tapLog.debug(arg);
                break;
        }
    }
};

