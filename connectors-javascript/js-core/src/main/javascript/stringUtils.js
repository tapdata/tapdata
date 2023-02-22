/**
 * @type global variable
 * @author Gavin
 * @description 操作字符串相关工具方法
 * */
var stringUtils = {
    /**
     * 字符串格式化，根据占位符（ {} 或 %s ）替换成对应字符串
     * @param msg 需要替换的字符串文本 String
     * @param args 待替换变量的参数列表 Array
     * @return String
     * 例如：format('This is string will replace a {}。', ["number"])
     * 则输出结果为： This is string will replace a number。
     * */
    format(msg, args) {
        if ('undefined' === msg || null == msg || "" === msg) return "";
        for (let index = 0; index < args.length; index++) {
            msg = msg.replace(new RegExp("(\{\})|(%\s)"), args[index]);
        }
        return msg;
    }
}