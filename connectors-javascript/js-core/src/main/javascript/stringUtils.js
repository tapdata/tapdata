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
        if ('undefined' === msg || null == msg || "" === msg) {
            if (args.length>0){
                msg = "{}";
                for (let i = 1; i < args.length; i++) {
                    msg = msg + ", {}"
                }
            }else {
                return "";
            }
        }
        if (typeof msg != 'string'){
            args = [msg];
            msg = "{}";
        }
        for (let index = 0; index < args.length; index++) {
            msg = msg.replace(new RegExp("(\{\})|(%\s)"), args[index]);
        }
        return msg;
    },

    /**
     * 生成指定长度的随机字符串
     * @param len 需要生成字符串的长度 Integer
     * @return String
     * 例如：randomString(5)
     * 则输出结果为，形如： sfdk8 五位字符串。
     * */
    randomString(len){
        let chars = 'ABCDEFGHJKMNPQRSTWXYZabcdefhijkmnprstwxyz2345678';
        let tempLen = chars.length, tempStr='';
        for(let i=0 ; i<len; ++i){
            tempStr += chars.charAt(Math.floor(Math.random() * tempLen ));
        }
        return tempStr;
    },
    /**
     * 将指定的 value 和 key 进行 hmacSha256 求值，生成加密串
     * @param value 需要加密的value ,String
     * @param key 需要加密的key ,String
     * @return String
     * 例如：sha256_HMAC('I'm Gavin in Tapdata', 'gavin')
     * 则输出结果为加密后的加密串：d5d1f12eafe24a8190665fce376eaeb82858319028e34a8010637bc600e9d0e0。
     * */
    sha256_HMAC(value, key){
        return tapUtil.sha256_HMAC(value, key);
    },
    /**
     * MD5算法加密，生成加密串
     * @param str 需要加密的str ,String
     * @return String
     * 例如：stringUtils.encodeMD5('abc')
     * 则输出结果为加密后的加密串：900150983cd24fb0d6963f7d28e17f72
     * */
    encodeMD5(str){
        return MD5.hex_md5(str);
    },
    /**
     * MD5算法加密，生成大写加密串
     * @param str 需要加密的str ,String
     * @return String
     * 例如：stringUtils.encodeMD5Upper('abc')
     * 则输出结果为加密后的加密串：900150983CD24FB0D6963F7D28E17F72
     * */
    encodeMD5Upper(str){
        return MD5.hex_md5(str).toUpperCase();
    }
}