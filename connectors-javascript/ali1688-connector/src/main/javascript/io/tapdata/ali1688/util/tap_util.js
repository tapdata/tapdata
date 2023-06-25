

function isValue(value){
    return 'undefined' !== value && null != value;
}

function getSignatureRules(appSecret, urlPath, params){
    let openKeyIdCodeOfValue = urlPath + mapUtils.getParamsWithKeyValueAsString(params, "");
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
        if ("baseInfo" === key) {
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
                'type': 'Float',
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

var csvTableConfig = {
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
            // "info_nativeLogistics":{
            //     'type': 'Object',
            //     'comment': '',
            //     'nullable': true
            // },
            // "info_orderRateInfo":{
            //     "type": "Object",
            //     "comment": "",
            //     "nullable": false
            // },
            // "info_productItems": {
            //     "type": "Array",
            //     "comment": "商品信息列表",
            //     "nullable": true
            // },
            // "info_tradeTerms": {
            //     "type": "Array",
            //     "comment": "",
            //     "nullable": false
            // },
            "订单编号":{
                'type': 'String',
                'comment': '订单编号-idOfStr'
            },
            "商品明细条目ID":{
                "type":"String",
                "comment":"商品明细条目ID",
                "isPrimaryKey": true,
                "primaryKeyPos": 1
            },
            "交易ID":{
                'type': 'Long',
                'comment': '交易ID-id',
                // 'nullable': true,
            },
            "skuID":{
                "type":"Long",
                "comment":"skuID",
                // "isPrimaryKey": true,
                // "primaryKeyPos": 2
            },
            "发货时间":{
                'type': 'String',
                'comment': '发货时间-allDeliveredTime'
            },
            "付款时间":{
                'type': 'String',
                'comment': '付款时间-payTime'
            },
            // "receiverInfo":{
            //     'type': 'Object',
            //     'comment': '-'
            // },
            "折扣信息（元）":{
                'type': 'Long',
                'comment': '折扣信息（元）-discount'
            },
            // "alipayTradeId":{
            //     'type': 'String',
            //     'comment': '-'
            // },
            "订单备注":{
                'type': 'String',
                'comment': '订单备注-remark'
            },
            "货品金额总计（不包含运费）":{
                'type': 'Number',
                'comment': '货品金额总计（不包含运费）-sumProductPayment'
            },
            // "buyerFeedback":{
            //     'type': 'Number',
            //     'comment': '-'
            // },
            "采购账号":{
                'type': 'Number',
                'comment': '采购账号-buyerLoginId'
            },
            "修改时间":{
                'type': 'Number',
                'comment': '修改时间-modifyTime'
            },
            "订单最后修改时间":{
                'type': 'Number',
                'comment': '修改时间-modifyTime'
            },
            // "tradeType":{
            //     'type': 'Number',
            //     'comment': '-'
            // },
            // "buyerContact":{
            //     'type': 'Object',
            //     'comment': '-'
            // },
            // "sellerAlipayId":{
            //     'type': 'String',
            //     'comment': '-'
            // },
            // "stepPayAll":{
            //     'type': 'Boolean',
            //     'comment': '-'
            // },
            // "sellerLoginId":{
            //     'type': 'String',
            //     'comment': '-'
            // },
            // "sellerUserId":{
            //     'type': 'Number',
            //     'comment': '-'
            // },
            "买家主账号ID":{
                'type': 'String',
                'comment': '买家主账号ID-buyerID'
            },
            // "buyerAlipayId":{
            //     'type': 'String',
            //     'comment': '-'
            // },
            "应付款总金额（元）":{
                'type': 'Number',
                'comment': '应付款总金额（元）-totalAmount'
            },
            "卖家主账号ID":{
                'type': 'Long',
                'comment': '卖家主账号ID-sellerID'
            },
            // "shippingFee":{
            //     'type': 'Number',
            //     'comment': '-'
            // },
            "创建时间":{
                'type': 'String',
                'comment': '创建时间-createTime'
            },
            // "buyerUserId":{
            //     'type': 'Number',
            //     'comment': '-'
            // },
            "业务类型":{
                'type': 'String',
                'comment': '业务类型-businessType'
            },
            // "overSeaOrder":{
            //     'type': 'Boolean',
            //     'comment': '-'
            // },
            "退款金额（元）":{
                'type': 'Float',
                'comment': '退款金额（元）-refund'
            },
            "交易状态":{
                'type': 'String',
                'comment': '交易状态-status'
            },
            // "sellerContact":{
            //     'type': 'Object',
            //     'comment': '-'
            // },
            // "refundPayment":{
            //     'type': 'Number',
            //     'comment': '-'
            // },
            "买家备忘信息":{
                'type': 'String',
                'comment': '买家备忘信息-buyerMemo'
            },
            "完成时间":{
                'type': 'String',
                'comment': '完成时间-completeTime'
            },
            "收货时间":{
                "type":"String",
                "comment":"收货时间-receivingTime"
            },
            "运费（元）":{
                "type":"Float",
                "comment":"运费（元）-shippingFee"
            },
            "买家备忘标志":{
                "type":"String",
                "comment":"买家备忘标志-buyerRemarkIcon"
            },
            "币种":{
                "type":"String",
                "comment":"币种-currency"
            },
            "阶段":{
                "type":"String",
                "comment":"阶段-tradeTerms.phase"
            },
            "跨境地址扩展信息":{
                "type":"String",
                "comment":"跨境地址扩展信息-overseasExtraAddress"
            },
            "跨境报关信息":{
                "type":"String",
                "comment":"跨境报关信息-customs"
            },
            "发票公司":{
                "type":"String",
                "comment":"发票公司-orderInvoiceInfo.invoiceCompanyName"
            },
            "发票类型":{
                "type":"String",
                "comment":"发票类型-orderInvoiceInfo.invoiceType"
            },
            "本地发票号":{
                "type":"String",
                "comment":"本地发票号-orderInvoiceInfo.localInvoiceId"
            },
            "（收件人）址区域编码":{
                "type":"String",
                "comment":"(收件人)址区域编码-orderInvoiceInfo.receiveCode"
            },
            "（收件人）址区域编码":{
                "type":"String",
                "comment":"(收件人)址区域编码-orderInvoiceInfo.receiveCode"
            },
            "发票收货人手机":{
                "type":"String",
                "comment":"发票收货人手机-orderInvoiceInfo.receiveMobile"
            },
            "发票收货人":{
                "type":"String",
                "comment":"发票收货人-orderInvoiceInfo.receiveName"
            },
            "发票收货人电话":{
                "type":"String",
                "comment":"发票收货人电话-orderInvoiceInfo.receivePhone"
            },
            "发票收货地址邮编":{
                "type":"String",
                "comment":"发票收货地址邮编-orderInvoiceInfo.receivePost"
            },
            "街道地址（增值税发票信息）":{
                "type":"String",
                "comment":"发票收货地址邮编-orderInvoiceInfo.receivePost"
            },
            "银行账号（增值税发票信息）":{
                "type":"String",
                "comment":"银行账号（增值税发票信息）-orderInvoiceInfo.registerAccountId"
            },
            "开户银行（增值税发票信息）":{
                "type":"String",
                "comment":"开户银行（增值税发票信息）-orderInvoiceInfo.registerBank"
            },
            "省市区编码（增值税发票信息）":{
                "type":"String",
                "comment":"省市区编码（增值税发票信息）-orderInvoiceInfo.registerCode"
            },
            "省市区文本（增值税发票信息）":{
                "type":"String",
                "comment":"省市区文本（增值税发票信息）-orderInvoiceInfo.registerCodeText"
            },
            "注册电话（增值税发票信息）":{
                "type":"String",
                "comment":"注册电话（增值税发票信息）-orderInvoiceInfo.registerPhone"
            },
            "街道地址（增值税发票信息）":{
                "type":"String",
                "comment":"街道地址（增值税发票信息）-orderInvoiceInfo.registerStreet"
            },
            "纳税人识别号（增值税发票信息）":{
                "type":"String",
                "comment":"纳税人识别号（增值税发票信息）-orderInvoiceInfo.taxpayerIdentify"
            },
            "供应商":{
                "type":"String",
                "comment":"供应商-sellerContact.companyName"
            },
            "供应商手机号码":{
                "type":"String",
                "comment":"供应商手机号码-sellerContact.mobile"
            },
            "供应商旺旺号":{
                "type":"String",
                "comment":"供应商旺旺号-sellerContact.imInPlatform"
            },
            "供应商联系人":{
                "type":"String",
                "comment":"供应商联系人-sellerContact.name"
            },
            "单品货号":{
                "type":"String",
                "comment":"单品货号"
            },
            "描述":{
                "type":"String",
                "comment":"描述"
            },
            "数量":{
                "type":"Number",
                "comment":"数量"
            },
            "实付金额":{
                "type":"Float",
                "comment":"实付金额"
            },
            "实际单价":{
                "type":"Float",
                "comment":"实际单价"
            },
            "运费（元）":{
                "type":"Number",
                "comment":"运费（元）"
            },
            "单价（元）":{
                "type":"Number",
                "comment":"单价（元）"
            },
            "商品名称":{
                "type":"String",
                "comment":"商品名称"
            },
            "原始单价（元）":{
                "type":"Float",
                "comment":"原始单价（元）"
            },
            "产品ID（非在线产品为空）":{
                "type":"Long",
                "comment":"产品ID（非在线产品为空）"
            },
            "商品图片url":{
                "type":"String",
                "comment":"商品图片url"
            },
            "产品快照url":{
                "type":"String",
                "comment":"产品快照url"
            },
            "退款金额（元）":{
                "type":"Float",
                "comment":"退款金额（元）"
            },
            "排序字段":{
                "type":"String",
                "comment":"排序字段"
            },
            "售卖单位":{
                "type":"String",
                "comment":"售卖单位"
            },
            "重量":{
                "type":"Float",
                "comment":"重量"
            },
            "重量单位":{
                "type":"String",
                "comment":"重量单位"
            },
            "商品货号":{
                "type":"String",
                "comment":"商品货号"
            },
            "订单明细涨价或降价的金额":{
                "type":"Float",
                "comment":"订单明细涨价或降价的金额"
            },
            "订单销售属性ID":{
                "type":"String",
                "comment":"订单销售属性ID"
            },
            "精度系数":{
                "type":"Float",
                "comment":"精度系数"
            },
            "子订单状态描述":{
                "type":"String",
                "comment":"子订单状态描述"
            },
            "退货状态":{
                "type":"String",
                "comment":"退货状态"
            },
            "关闭原因":{
                "type":"String",
                "comment":"关闭原因"
            },
            "售中退款单号":{
                "type":"String",
                "comment":"售中退款单号"
            },
            "售后退款单号":{
                "type":"String",
                "comment":"售后退款单号"
            },
            "子订单关联码":{
                "type":"String",
                "comment":"子订单关联码"
            },
            "SKU属性描述":{
                "type":"String",
                "comment":"SKU属性描述"
            },
            "货物单位":{
                "type":"String",
                "comment":"货物单位"
            },
            "货物数量":{
                "type":"Number",
                "comment":"货物数量"
            },
            "货物名称":{
                "type":"String",
                "comment":"货物名称"
            },
            "收货人":{
                "type":"String",
                "comment":"收货人"
            },
            "收货地址":{
                "type":"String",
                "comment":"收货地址"
            },
            "收货区号":{
                "type":"String",
                "comment":"收货区号"
            },
            "收货手机号码":{
                "type":"String",
                "comment":"收货手机号码"
            },
            "发货人":{
                "type":"String",
                "comment":"发货人"
            },
            "发货地址":{
                "type":"String",
                "comment":"发货地址"
            },
            "发货电话":{
                "type":"String",
                "comment":"发货电话"
            },
            "发货区号":{
                "type":"String",
                "comment":"发货区号"
            },
            "物流公司":{
                "type":"String",
                "comment":"物流公司"
            },
            "物流单号":{
                "type":"String",
                "comment":"物流单号"
            },
            "物流公司ID":{
                "type":"String",
                "comment":"物流公司ID"
            },
            "物流账单号":{
                "type":"String",
                "comment":"物流账单号"
            },
            "物流状态":{
                "type":"String",
                "comment":"物流状态"
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