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
                "type": "Long",
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
                "type": "DateTime0",
                "comment": "要求发货时间"
            },
            "addTime": {
                "type": "DateTime0",
                "comment": "添加时间"
            },
            "allocateTime": {
                "type": "DateTime0",
                "comment": "分单时间"
            },
            "reserveTime": {
                "type": "DateTime0",
                "comment": "预约送货时间"
            },
            "receiptTime": {
                "type": "DateTime0",
                "comment": "收货时间"
            },
            "checkTime": {
                "type": "DateTime0",
                "comment": "查验时间"
            },
            "storageTime": {
                "type": "DateTime0",
                "comment": "入仓时间"
            },
            "returnTime": {
                "type": "DateTime0",
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
                "type":"String",
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
                "type": "Integer",
                "comment": "状态(订单状态：1待下单，2已下单，3发货中，4已送货，5已收货，6已查验，7已退货，8已完成，9无货下架，10已作废，11待审核，12分单中,13待退货, 14-全部)"
            },
            "statusName": {
                "type": "String",
                "comment": "状态名称"
            }
        }
    }
}
var globalCSVTableConfig = {
    "ShippingOrder": {
        "name": "ShippingOrder",
        "supportRead": true,
        "supportWrite": false,
        "fields":{
            "订单编号":{
                'type': 'String',
                'comment': '订单编号-orderNo',
                'nullable': true,
                "isPrimaryKey": true,
                "primaryKeyPos": 1
            },
            "订单类型":{
                'type': 'String',
                'comment': '类型名称, 急采、备货，急采type=1,备货type=2-typeName',
                'nullable': true
            },
            "备货类型": {
                "type": "String",
                "comment": "备货类型名称-prepareTypeName"
            },
            "SKC":{
                "type": "String",
                "comment": "SKC-orderExtends.skc"
            },
            "skuCode":{
                "type": "String",
                "comment": "skuCode-orderExtends.skuCode"
            },
            "供应商货号":{
                "type": "String",
                "comment": "供应商货号-orderExtends.supplierCode"
            },
            "图片":{
                "type": "String",
                "comment": "图片-orderExtends.imgPath"
            },
            "订单类型": {
                "type": "String",
                "comment": "订单类型名称-categoryName"
            },
            "订单标识": {
                "type": "String",
                "comment": "订单标识名称-orderMarkName"
            },
            "紧急类型": {
                "type": "String",
                "comment": "紧急类型-urgentTypeName"
            },
            "是否JIT母单": {
                "type": "String",
                "comment": "是否JIT母单（枚举值：是、否）【场景示例：JIT母单是预下单，比如下单500，不需要发货。需要发货时，比如要发货100，会产生一个JIT子单（即普通订单）数量100。】-isJitMotherName",
                "nullable": true
            },
            "供应商": {
                "type": "String",
                "comment": "供应商名称-supplierName"
            },
            "跟单员": {
                "type": "String",
                "comment": "跟单员-orderSupervisor"
            },
            "添加人": {
                "type": "String",
                "comment": "添加人-addUid"
            },
            "首单标识": {
                "type": "String",
                "comment": "首单标识-firstMarkName"
            },
            "价格":{
                "type": "Money",
                "comment": "价格-orderExtends.price"
            },
            "币种": {
                "type": "String",
                "comment": "币种名称-currencyName",
                "nullable": true
            },

            "发货时间": {
                "type": "DateTime0",
                "comment": "要求发货时间-deliveryTime"
            },
            "要求发货时间": {
                "type": "DateTime0",
                "comment": "要求发货时间-requestDeliveryTime"
            },
            "分单时间": {
                "type": "DateTime0",
                "comment": "分单时间-allocateTime"
            },
            "预约送货时间": {
                "type": "DateTime0",
                "comment": "预约送货时间-reserveTime"
            },
            "收货时间": {
                "type": "DateTime0",
                "comment": "收货时间-receiptTime"
            },
            "查验时间": {
                "type": "DateTime0",
                "comment": "查验时间-checkTime"
            },
            "入仓时间": {
                "type": "DateTime0",
                "comment": "入仓时间-storageTime"
            },
            "退货时间": {
                "type": "DateTime0",
                "comment": "退货时间-returnTime"
            },
            "要求完工时间": {
                "type": "DateTime0",
                "comment": "要求完工时间(是否送货为“否”时才有值）-requestCompleteTime"
            },
            "添加时间": {
                "type": "DateTime0",
                "comment": "添加时间-addTime"
            },
            "更新时间": {
                "type": "DateTime0",
                "comment": "更新时间-updateTime"
            },

            "是否优先生产": {
                "type": "String",
                "comment": "是否优先生产（枚举值：是、否）-isPriorProductionName",
                "nullable": true
            },
            "仓库名称": {
                "type": "String",
                "comment": "仓库id对应的仓库名称-warehouseName"
            },
            "仓库ID": {
                "type": "String",
                "comment": "仓库id-storageId"
            },
            "是否生产完成": {
                "type": "String",
                "comment": "是否生产完成(是否送货为“否”时才有值）-isProductionCompletionName"
            },
            "是否完全要求发货": {
                "type": "String",
                "comment": "是否完全要求发货(是否送货为“否”时才有值）-isAllDeliveryName"
            },
            "是否送货": {
                "type": "String",
                "comment": "是否送货-isDeliveryName"
            },
            "订单状态": {
                "type": "String",
                "comment": "状态名称-statusName"
            },

            "商品后缀":{
                "type": "String",
                "comment": "商品后缀-orderExtends.suffixZh"
            },
            "下单数量":{
                "type": "Integer",
                "comment": "下单数量-orderExtends.orderQuantity"
            },
            "入仓数量":{
                "type": "Integer",
                "comment": "入仓数量-orderExtends.storageQuantity"
            },
            "收货数量":{
                "type": "Integer",
                "comment": "收货数量-orderExtends.receiptQuantity"
            },
            "送货数量":{
                "type": "Integer",
                "comment": "送货数量-orderExtends.deliveryQuantity"
            },
            "次品数量":{
                "type": "Integer",
                "comment": "次品数量-orderExtends.defectiveQuantity"
            },
            // "详情":{
            //     "type": "Array",
            //     "comment": "详情信息-orderExtends",
            //     "nullable": false
            // },
            "已要求发货数量":{
                "type": "Integer",
                "comment": "已要求发货数量-requestDeliveryQuantity"
            },
            "未要求发货数":{
                "type": "Integer",
                "comment": "未要求发货数-noRequestDeliveryQuantity"
            },
            "已发货数量":{
                "type": "Integer",
                "comment": "已发货数量-alreadyDeliveryQuantity"
            },
            "供应商SKU":{
                "type": "String",
                "comment": "供应商SKU-orderExtends.supplierSku"
            },
            // "订单类型Code":{
            //     "type":"String",
            //     "comment":"订单类型名称-category"
            // },
            // "状态Code": {
            //     "type": "String",
            //     "comment": "状态(订单状态：1待下单，2已下单，3发货中，4已送货，5已收货，6已查验，7已退货，8已完成，9无货下架，10已作废，11待审核，12分单中,13待退货, 14-全部)-status"
            // },
            "详情备注":{
                "type": "String",
                "comment": "备注-orderExtends.skuCode"
            }
        }
    }
}

// var keyMap = {
//     "orderNo": "订单编号",
//     "typeName": "类型",
//     "orderExtends": "详情",
//     "currencyName": "币种",
//     "isPriorProductionName":"是否优先生产",
//     "isJitMotherName": "是否JIT母单",
//     "updateTime": "更新时间",
//     "supplierName": "供应商",
//     "orderSupervisor": "跟单员",
//     "addUid": "添加人",
//     "requestDeliveryTime": ,
//     "addTime": ,
//     "allocateTime": ,
//     "reserveTime": ,
//     "receiptTime": ,
//     "checkTime": ,
//     "storageTime": ,
//     "returnTime":,
//     "firstMarkName": ,
//     "prepareTypeName": ,
//     "category":,
//     "categoryName": ,
//     "orderMarkName": ,
//     "warehouseName": ,
//     "urgentTypeName": ,
//     "storageId": ,
//     "requestCompleteTime": ,
//     "isProductionCompletionName": ,
//     "isAllDeliveryName": ,
//     "isDeliveryName": ,
//     "status": ,
//     "statusName":
// }

function getSignatureRules(openKeyId, secretKey, urlPath, time){
    let openKeyIdCodeOfValue = openKeyId + "&" + time + "&" + urlPath;
    //log.warn("openKeyIdCodeOfValue: {}", openKeyIdCodeOfValue)

    let randomStr = stringUtils.randomString(5);
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



