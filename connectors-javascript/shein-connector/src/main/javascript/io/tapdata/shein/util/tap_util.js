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

var globalTableConfig = {
    "ShippingOrder": {
        "name": "ShippingOrder",
        "supportRead": true,
        "supportWrite": false,
        "fields":{
            "orderNo":{
                'type': 'String',
                'comment': '订单编号',
                'nullable': true,
                "isPrimaryKey": true,
                "primaryKeyPos": 1
            },
            "typeName":{
                'type': 'String',
                'comment': '类型名称, 急采、备货，急采type=1,备货type=2',
                'nullable': true
            },
            "orderExtends":{
                "type": "Array",
                "comment": "详情信息",
                "nullable": false
            },
            "currencyName": {
                "type": "String",
                "comment": "币种名称",
                "nullable": true
            },
            "isPriorProductionName": {
                "type": "String",
                "comment": "是否优先生产（枚举值：是、否）",
                "nullable": true
            },
            "isJitMotherName": {
                "type": "String",
                "comment": "是否JIT母单（枚举值：是、否）【场景示例：JIT母单是预下单，比如下单500，不需要发货。需要发货时，比如要发货100，会产生一个JIT子单（即普通订单）数量100。】",
                "nullable": true
            },
            "updateTime": {
                "type": "Number",
                "comment": "更新时间"
            },
            "supplierName": {
                "type": "String",
                "comment": "供应商名称"
            },
            "orderSupervisor": {
                "type": "String",
                "comment": "跟单员"
            },
            "addUid": {
                "type": "String",
                "comment": "添加人"
            },
            "requestDeliveryTime": {
                "type": "String",
                "comment": "要求发货时间"
            },
            "addTime": {
                "type": "Number",
                "comment": "添加时间"
            },
            "allocateTime": {
                "type": "Number",
                "comment": "分单时间"
            },
            "reserveTime": {
                "type": "Number",
                "comment": "预约送货时间"
            },
            "receiptTime": {
                "type": "Number",
                "comment": "收货时间"
            },
            "checkTime": {
                "type": "Number",
                "comment": "查验时间"
            },
            "storageTime": {
                "type": "Number",
                "comment": "入仓时间"
            },
            "returnTime": {
                "type": "Number",
                "comment": "退货时间"
            },
            "firstMarkName": {
                "type": "String",
                "comment": "首单标识"
            },
            "prepareTypeName": {
                "type": "String",
                "comment": "备货类型名称"
            },
            "category":{
                "type":"Number",
                "comment":"订单类型名称"
            },
            "categoryName": {
                "type": "String",
                "comment": "订单类型名称"
            },
            "orderMarkName": {
                "type": "String",
                "comment": "订单标识名称"
            },
            "warehouseName": {
                "type": "String",
                "comment": "仓库id对应的仓库名称"
            },
            "urgentTypeName": {
                "type": "String",
                "comment": "紧急类型"
            },
            "storageId": {
                "type": "String",
                "comment": "仓库id"
            },
            "requestCompleteTime": {
                "type": "String",
                "comment": "要求完工时间(是否送货为“否”时才有值）"
            },
            "isProductionCompletionName": {
                "type": "String",
                "comment": "是否生产完成(是否送货为“否”时才有值）"
            },
            "isAllDeliveryName": {
                "type": "String",
                "comment": "是否完全要求发货(是否送货为“否”时才有值）"
            },
            "isDeliveryName": {
                "type": "String",
                "comment": "是否送货"
            },
            "status": {
                "type": "Number",
                "comment": "状态(订单状态：1待下单，2已下单，3发货中，4已送货，5已收货，6已查验，7已退货，8已完成，9无货下架，10已作废，11待审核，12分单中,13待退货, 14-全部)"
            },
            "statusName": {
                "type": "String",
                "comment": "状态名称"
            }
        }
    }
}

function getSignatureRules(openKeyId, secretKey, urlPath, time){
    let openKeyIdCodeOfValue = openKeyId + "&" + time + "&" + urlPath;
    //log.warn("openKeyIdCodeOfValue: {}", openKeyIdCodeOfValue)

    let randomStr = randomString(5);
    //log.warn("randomStr: {}", randomStr)
    let secretKeyCodeOfKey = secretKey + randomStr;
    //log.warn("secretKeyCodeOfKey: {}", secretKeyCodeOfKey)

    let hmacSha256 = sha256_HMAC(openKeyIdCodeOfValue, secretKeyCodeOfKey);//hex(sign(secretKeyCodeOfKey, openKeyIdCodeOfValue));

    //log.warn("hmacSha256: {}", hmacSha256)
    let base64 = new Base64();
    let result = base64.encode(hmacSha256);
    //log.warn("result: {}", result)
    //log.info("SignatureRule: {}, time: {}", randomStr + result, time)
    return randomStr + result;
}


function randomString(len){
    let chars = 'ABCDEFGHJKMNPQRSTWXYZabcdefhijkmnprstwxyz2345678';
    let tempLen = chars.length, tempStr='';
    for(let i=0 ; i<len; ++i){
        tempStr += chars.charAt(Math.floor(Math.random() * tempLen ));
    }
    return tempStr;
}


