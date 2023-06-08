/**
 * @type global variable
 * @author Gavin
 * @description 操作日志打印相关工具方法
 * */
var log = {
    /**
     * @type function
     * @author Gavin
     * @description 日志打印，打印 WARN 级别日志
     * @param msg 需要输出的内容，String
     * @param params 格式化参数 String[] ,
     *  - 使用案例： log.warn('This is a warn log,param1: {}, param2: {}',param1,param2);
     * @date 2023/2/13
     * */
    warn: function (msg, ...params) {
        let arg = [];
        if (arguments.length > 0) {
            for (let argIndex = 1; argIndex < arguments.length; argIndex++) {
                arg.push(arguments[argIndex]);
            }
        }
        tapLog.warn(stringUtils.format(msg, arg));
    },
    /**
     * @type function
     * @author Gavin
     * @description 日志打印，打印 ERROR 级别日志
     * @param msg 需要输出的内容，String
     * @param params 格式化参数 String[] ,
     *  - 使用案例： log.error('This is a error log,param1: {}, param2: {}',param1,param2);
     * @date 2023/2/13
     * */
    error: function (msg, ...params) {
        let arg = [];
        for (let argIndex = 1; argIndex < arguments.length; argIndex++) {
            arg.push(arguments[argIndex]);
        }
        tapLog.error(stringUtils.format(msg, arg));
    },
    /**
     * @type function
     * @author Gavin
     * @description 日志打印，打印 INFO 级别日志
     * @param msg 需要输出的内容，String
     * @param params 格式化参数 String[] ,
     *  - 使用案例： log.info('This is a info log,param1: {}, param2: {}',param1,param2);
     * @date 2023/2/13
     * */
    info: function (msg, ...params) {
        let arg = [];
        for (let argIndex = 1; argIndex < arguments.length; argIndex++) {
            arg.push(arguments[argIndex]);
        }
        tapLog.info(stringUtils.format(msg, arg));
    },
    /**
     * @type function
     * @author Gavin
     * @description 日志打印，打印 DEBUG 级别日志
     * @param msg 需要输出的内容，String
     * @param params 格式化参数 String[] ,
     *  - 使用案例： log.debug('This is a debug log,param1: {}, param2: {}',param1,param2);
     * @date 2023/2/13
     * */
    debug: function (msg, ...params) {
        let arg = [];
        for (let argIndex = 1; argIndex < arguments.length; argIndex++) {
            arg.push(arguments[argIndex]);
        }
        tapLog.debug(stringUtils.format(msg, arg));
    },
    /**
     * @type function
     * @author Gavin
     * @description 日志打印
     * @param type 需要输出的日志级别，默认debug，范围：debug,info,warn,error.
     * @param args 打印参数，args[0]:需要输出的内容，String; args[1~n]:格式化参数 String[] ,
     *  - 使用案例： log.printf('warn','This is a warn log,param1: {}, param2: {}',param1,param2);
     * @date 2023/2/13
     * */
    printf: function (type, msg, args) {
        if ('undefined' === type || null == type) type = 'debug';
        let arg = [];
        for (let argIndex = 1; argIndex < arguments.length; argIndex++) {
            arg.push(arguments[argIndex]);
        }
        msg = stringUtils.format(msg, arg);
        switch (type) {
            case 'debug' :
                tapLog.debug();
                break;
            case 'warn' :
                tapLog.warn(msg);
                break;
            case 'error' :
                tapLog.error(msg);
                break;
            case 'info' :
                tapLog.info(msg);
                break;
            default:
                tapLog.debug(msg);
                break;
        }
    }
};

