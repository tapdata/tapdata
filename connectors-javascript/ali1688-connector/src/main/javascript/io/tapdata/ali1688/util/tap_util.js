

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
                newRecord[subKey] = ("id" === subKey ? BigInt(element[subKey]) : element[subKey]);
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
            "创建时间":{
                'type': 'DateTime0',
                'comment': '创建时间-createTime'
            },
            "订单编号":{
                'type': 'String',
                'comment': '订单编号-idOfStr'
            },
            "商品名称":{
                "type":"String",
                "comment":"商品名称"
            },
            "SKU属性描述":{
                "type":"String",
                "comment":"SKU属性描述"
            },
            "子单原始单价（元）":{
                "type":"Money",
                "comment":"子单原始单价（元）"
            },
            "子单实际单价（元）":{
                "type":"Money",
                "comment":"实际单价"
            },
            "子单数量":{
                "type":"Number",
                "comment":"数量"
            },
            "折扣信息（元）":{
                'type': 'Money',
                'comment': '折扣信息（元）-discount'
            },
            "子单涨价或折扣":{
                "type":"Money",
                "comment":"子单涨价或降价的金额"
            },
            "子单优惠金额（元）":{
                "type":"Money",
                "comment":"子单优惠金额（元）"
            },
            "子单实付金额（不包含运费）":{
                'type': 'Money',
                'comment': '货品金额总计（不包含运费）-sumProductPayment'
            },
            "运费（元）":{
                "type":"Money",
                "comment":"运费（元）-shippingFee"
            },
            "应付款总金额（元）":{
                'type': 'Money',
                'comment': '应付款总金额（元）-totalAmount'
            },
            "买家备忘信息":{
                'type': 'String',
                'comment': '买家备忘信息-buyerMemo'
            },
            "付款时间":{
                'type': 'DateTime0',
                'comment': '付款时间-payTime'
            },
            "发货时间":{
                'type': 'DateTime0',
                'comment': '发货时间-allDeliveredTime'
            },
            "物流公司":{
                "type":"String",
                "comment":"物流公司"
            },
            "物流备注":{
                "type":"String",
                "comment":"物流状态"
            },
            "物流编号":{
                "type":"String",
                "comment":"物流编号"
            },
            "运单号码":{
                "type":"String",
                "comment":"运单号码"
            },
            "收货时间":{
                "type":"DateTime0",
                "comment":"收货时间-receivingTime"
            },
            "物流状态":{
                "type":"String",
                "comment":"物流状态"
            },
            "售中退款单号":{
                "type":"String",
                "comment":"售中退款单号"
            },
            "退款金额（元）":{
                "type":"Money",
                "comment":"退款金额（元）"
            },
            "完成时间":{
                'type': 'DateTime0',
                'comment': '完成时间-completeTime'
            },
            "子订单状态描述":{
                "type":"String",
                "comment":"子订单状态描述"
            },
            "商品图片url":{
                "type":"String",
                "comment":"商品图片url"
            },
            "采购账号":{
                'type': 'String',
                'comment': '采购账号-buyerLoginId'
            },
            "产品ID（非在线产品为空）":{
                "type":"String",
                "comment":"产品ID（非在线产品为空）"
            },
            "skuID":{
                "type":"String",
                "comment":"skuID",
                // "isPrimaryKey": true,
                // "primaryKeyPos": 2
            },
            "关闭原因":{
                "type":"String",
                "comment":"关闭原因"
            },
            "退货状态":{
                "type":"String",
                "comment":"退货状态"
            },
            "售后退款单号":{
                "type":"String",
                "comment":"售后退款单号"
            },
            "子订单关联码":{
                "type":"String",
                "comment":"子订单关联码"
            },
            "描述":{
                "type":"String",
                "comment":"描述"
            },
            "收货人":{
                "type":"String",
                "comment":"收货人"
            },
            "收货地址":{
                "type":"String",
                "comment":"收货地址"
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
            "供应商":{
                "type":"String",
                "comment":"供应商-sellerContact.companyName"
            },
            "供应商手机号码":{
                "type":"String",
                "comment":"供应商手机号码-sellerContact.mobile"
            },
            "供应商座机":{
                "type":"String",
                "comment":"供应商手机号码-sellerContact.phone"
            },
            "供应商旺旺号":{
                "type":"String",
                "comment":"供应商旺旺号-sellerLoginId"
            },
            "供应商联系人":{
                "type":"String",
                "comment":"供应商联系人-sellerContact.name"
            },
            "商品明细条目ID":{
                "type":"String",
                "comment":"商品明细条目ID",
                "isPrimaryKey": true,
                "primaryKeyPos": 1
            },





            "交易ID":{
                'type': 'String',
                'comment': '交易ID-id',
                // 'nullable': true,
            },
            "子单实付金额（元）":{
                "type":"Money",
                "comment":"子单实付金额（元）"
            },
            "售卖单位":{
                "type":"String",
                "comment":"售卖单位"
            },
            "单品货号":{
                "type":"String",
                "comment":"单品货号"
            },
            "产品快照url":{
                "type":"String",
                "comment":"产品快照url"
            },
            "修改时间":{
                'type': 'DateTime0',
                'comment': '修改时间-modifyTime'
            },
            "订单最后修改时间":{
                'type': 'DateTime0',
                'comment': '修改时间-modifyTime'
            },
            "买家主账号ID":{
                'type': 'String',
                'comment': '买家主账号ID-buyerID'
            },
            "卖家主账号ID":{
                'type': 'String',
                'comment': '卖家主账号ID-sellerID'
            },
            "业务类型":{
                'type': 'String',
                'comment': '业务类型-businessType'
            },
            // "overSeaOrder":{
            //     'type': 'Boolean',
            //     'comment': '-'
            // },
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
            "买家备忘标志":{
                "type":"String",
                "comment":"买家备忘标志-buyerRemarkIcon"
            },
            "阶段":{
                "type":"String",
                "comment":"阶段-tradeTerms.phase"
            },
            "商品货号":{
                "type":"String",
                "comment":"商品货号"
            },
            "订单销售属性ID":{
                "type":"String",
                "comment":"订单销售属性ID"
            },
            "精度系数":{
                "type":"Float",
                "comment":"精度系数"
            },
            "货物单位":{
                "type":"String",
                "comment":"货物单位"
            },
            "货物数量":{
                "type":"String",
                "comment":"货物数量"
            },
            "货物名称":{
                "type":"String",
                "comment":"货物名称"
            },
            "收货区号":{
                "type":"String",
                "comment":"收货区号"
            },
            "发货区号":{
                "type":"String",
                "comment":"发货区号"
            },
            "物流公司ID":{
                "type":"String",
                "comment":"物流公司ID"
            },

            "币种":{
                "type":"String",
                "comment":"币种-currency"
            },
            "买家留言":{
                'type': 'String',
                'comment': '买家留言-remark'
            },
            "重量":{
                "type":"Money",
                "comment":"重量"
            },
            "重量单位":{
                "type":"String",
                "comment":"重量单位"
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
            "排序字段":{
                "type":"String",
                "comment":"排序字段"
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