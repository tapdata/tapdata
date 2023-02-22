/**
 * @type global variable
 * @author Gavin
 * @description 操作字符串相关工具方法
 * */
var stringUtils = {
    format(msg, args) {
        if ('undefined' === msg || null == msg || "" === msg) return "";
        for (let index = 1; index < arguments.length; index++) {
            //let arg = arguments[index];
            //let typeArg = typeof arg;
            //let outputArg = '';
            // switch (typeArg) {
            //     case "bigint":
            //         outputArg = arg;
            //         break;
            //     case "boolean":
            //         outputArg = arg;
            //         break;
            //     case "number":
            //         outputArg = arg;
            //         break;
            //     case "string":
            //         outputArg = arg;
            //         break;
            //     case "undefined":
            //         outputArg = '';
            //         break;
            //     case "symbol":
            //         outputArg = arg;
            //         break;
            //     case "function":
            //         outputArg = arg;
            //         break;
            //     case "object":
            //         outputArg = JSON.stringify(arg);
            //         break;
            //     default:
            //         outputArg = arg;
            // }
            msg = msg.replace(new RegExp("(\{\})|(%\s)"), arguments[index]);
        }
        return msg;
    }
}