/**
 *  This is the toolkit encapsulated by Tap Data.
 * */
var invoker = loadAPI();
var OptionalUtil = {
    isEmpty: function (obj) {
        return typeof (obj) == 'undefined' || null == obj;
    },
    notEmpty: function (obj) {
        return !this.isEmpty(obj);
    }
}

function isValue(value){
    return 'undefined' !== value && null != value;
}

/**
 * 目前支持的签名算法为：MD5(sign_method=md5)，签名过程如下：
 本次请求中所有请求参数（包含公共参数与业务参数）进行首字母以ASCII方式升序排列（ASCII ASC），对于相同字母则使用下个字母做二次排序，字母序为从左到右，以此类推
 排序后的结果按照参数名（key）参数值（value）的次序进行字符串拼接，拼接处不包含任何字符
 拼接完成的字符串做进一步拼接成1个字符串（包含所有kv字符串的长串），并在该长串的头部及尾部分别拼接client_secret，完成签名字符串的组装
 最后对签名字符串，使用MD5算法加密后，得到的MD5加密密文后转为大写，即为sign值
 * */
function signURL(params, secretKey) {
    let openKeyIdCodeOfValue = mapUtils.getParamsWithKeyValueAsString(params, "");
    openKeyIdCodeOfValue = secretKey + openKeyIdCodeOfValue + secretKey;
    return stringUtils.encodeMD5Upper(openKeyIdCodeOfValue);
}