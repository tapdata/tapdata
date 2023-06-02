/**
 *  This is the toolkit encapsulated by Tap Data.
 * */
var invoker = loadAPI();

function isValue(value){
    return 'undefined' !== value && null != value;
}

function getSignatureRules(appSecret, urlPath, params){
    let sort = getParamsWithKeyValue(params);
    let openKeyIdCodeOfValue = urlPath + sort;
    let hmacSha256 = stringUtils.sha256_HMAC(openKeyIdCodeOfValue, appSecret);//hex(sign(secretKeyCodeOfKey, openKeyIdCodeOfValue));
    return hmacSha256.toUpperCase();
}

function getParamsWithKeyValue(params){
    let keys = Object.keys(params);
    let paramArr = [];
    for (let i = 0; i < keys.length; i++) {
        let key = keys[i];
        let param = params[key];
        paramArr.push(key + "" + param);
    }
    paramArr.sort();
    return paramArr;
}



var globalTableConfig = {
    "ShippingOrder": {
        "name": "ShippingOrder",
        "supportRead": true,
        "supportWrite": false,
        "fields":{
            "baseInfo":{
                'type': 'Object',
                'comment': '订单基本信息，包括订单号、订单创建时间......',
                'nullable': true
            },
            "baseInfo.id":{
                'type': 'Number',
                'comment': '订单ID',
                'nullable': true,
                "isPrimaryKey": true,
                "primaryKeyPos": 1
            },
            "nativeLogistics":{
                'type': 'Object',
                'comment': '',
                'nullable': true
            },
            "orderRateInfo":{
                "type": "Object",
                "comment": "",
                "nullable": false
            },
            "productItems": {
                "type": "Array",
                "comment": "商品信息列表",
                "nullable": true
            },
            "tradeTerms": {
                "type": "Array",
                "comment": "",
                "nullable": false
            }
        }
    }
}


/**
 * @deprecated
 * */
function strToHexCharCode(str) {
    if(str === "")
        return "";
    let hexCharCode = [];
    hexCharCode.push("0x");
    for(let i = 0; i < str.length; i++) {
        hexCharCode.push((str.charCodeAt(i)).toString(16));
    }
    return hexCharCode.join("");
}

/**
 * @deprecated
 * */
function toHesWithUpperCase(str){
    let item = strToHexCharCode(str);
    return item.toUpperCase();
}