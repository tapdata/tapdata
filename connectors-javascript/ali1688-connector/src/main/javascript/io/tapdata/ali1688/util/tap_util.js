/**
 *  This is the toolkit encapsulated by Tap Data.
 * */
var invoker = loadAPI();

function isValue(value){
    return 'undefined' !== value && null != value;
}

function getSignatureRules(appSecret, urlPath, params){
    let openKeyIdCodeOfValue = mapUtils.getParamsWithKeyValueAsString(params, "");
    //log.warn(openKeyIdCodeOfValue);
    //hex(sign(secretKeyCodeOfKey, openKeyIdCodeOfValue));
    return tapUtil.ali1688HmacSha1ToHexStr(openKeyIdCodeOfValue, appSecret);
}

function handleRecord(order){
    if (!isValue(order)) return order;
    let newRecord = {};
    for (let key of Object.keys(order)) {
        let element = order[key];
        if (!isValue(element)) continue;
        if ("baseInfo" === key){
            for (let subKey of Object.keys(element)) {
                newRecord[subKey] = "id" === subKey ? BigInt(element[subKey]) : element[subKey];
            }
        } else {
            newRecord["info_" + key] = order[key];
        }
    }
    //log.warn("newRecord: {}", JSON.stringify(newRecord));
    return newRecord;
}

var globalTableConfig = {
    "ShippingOrder": {
        "name": "ShippingOrder",
        "supportRead": true,
        "supportWrite": false,
        "fields":{
            // "baseInfo":{
            //     'type': 'Object',
            //     'comment': '订单基本信息，包括订单号、订单创建时间......',
            //     'nullable': true
            // },
            "info_nativeLogistics":{
                'type': 'Object',
                'comment': '',
                'nullable': true
            },
            "info_orderRateInfo":{
                "type": "Object",
                "comment": "",
                "nullable": false
            },
            "info_productItems": {
                "type": "Array",
                "comment": "商品信息列表",
                "nullable": true
            },
            "info_tradeTerms": {
                "type": "Array",
                "comment": "",
                "nullable": false
            },
            "idOfStr":{
                'type': 'String',
                'comment': '-'
            },
            "id":{
                'type': 'Number',
                'comment': '订单ID',
                'nullable': true,
                "isPrimaryKey": true,
                "primaryKeyPos": 1
            },
            "allDeliveredTime":{
                'type': 'String',
                'comment': '-'
            },
            "payTime":{
                'type': 'String',
                'comment': '-'
            },
            "receiverInfo":{
                'type': 'Object',
                'comment': '-'
            },
            "discount":{
                'type': 'Number',
                'comment': '-'
            },
            "alipayTradeId":{
                'type': 'String',
                'comment': '-'
            },
            "remark":{
                'type': 'String',
                'comment': '-'
            },
            "sumProductPayment":{
                'type': 'Number',
                'comment': '-'
            },
            "buyerFeedback":{
                'type': 'Number',
                'comment': '-'
            },
            "buyerLoginId":{
                'type': 'Number',
                'comment': '-'
            },
            "modifyTime":{
                'type': 'Number',
                'comment': '-'
            },
            "tradeType":{
                'type': 'Number',
                'comment': '-'
            },
            "buyerContact":{
                'type': 'Object',
                'comment': '-'
            },
            "sellerAlipayId":{
                'type': 'String',
                'comment': '-'
            },
            "stepPayAll":{
                'type': 'Boolean',
                'comment': '-'
            },
            "sellerLoginId":{
                'type': 'String',
                'comment': '-'
            },
            "sellerUserId":{
                'type': 'Number',
                'comment': '-'
            },
            "buyerID":{
                'type': 'String',
                'comment': '-'
            },
            "buyerAlipayId":{
                'type': 'String',
                'comment': '-'
            },
            "totalAmount":{
                'type': 'Number',
                'comment': '-'
            },
            "sellerID":{
                'type': 'String',
                'comment': '-'
            },
            "shippingFee":{
                'type': 'Number',
                'comment': '-'
            },
            "createTime":{
                'type': 'String',
                'comment': '-'
            },
            "buyerUserId":{
                'type': 'Number',
                'comment': '-'
            },
            "businessType":{
                'type': 'String',
                'comment': '-'
            },
            "overSeaOrder":{
                'type': 'Boolean',
                'comment': '-'
            },
            "refund":{
                'type': 'Number',
                'comment': '-'
            },
            "status":{
                'type': 'String',
                'comment': '-'
            },
            "sellerContact":{
                'type': 'Object',
                'comment': '-'
            },
            "refundPayment":{
                'type': 'Number',
                'comment': '-'
            },
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