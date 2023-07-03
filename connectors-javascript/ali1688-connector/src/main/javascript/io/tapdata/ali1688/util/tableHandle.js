var tableHandle = {
    handle: function (tableName) {
        if ('undefined' === tableName || tableName == null || '' === tableName) return null;
        switch (tableName) {
            case 'ShippingOrder': return new ShippingOrder();
        }
        return null;
    }
}

class DefaultTable {
    batchReadV(connectionConfig, nodeConfig, offset, pageSize, batchReadSender) {

    }

    streamReadV(connectionConfig, nodeConfig, offset, pageSize, streamReadSender) {

    }

    defaultBatchReadOffset(offset) {

    }

    defaultStreamReadOffset(offset) {

    }
}

class ShippingOrder extends DefaultTable {
    tableName = 'ShippingOrder';
    //同一秒内的数据缓存，存在是不需要再次处理，（orderNo + "_C" + addTime + "_U" + updateTime）
    currentTableNoHistory = [];
    time = "0";

    batchReadV(connectionConfig, nodeConfig, offset, pageSize, batchReadSender) {
        offset = this.defaultBatchReadOffset(offset);
        this.read(false, connectionConfig, nodeConfig, offset, pageSize, batchReadSender, (baseInfo, offset1, submitInfo, isOne) => {
            //let baseInfo = orderInfo.baseInfo;
            let orderId = submitInfo.get('商品明细条目ID') + baseInfo.idOfStr;
            //let addTime = orderInfo.get('创建时间');
            //let updateTime = orderInfo.get('修改时间');
            let addTime = baseInfo.createTime;//订单创建时间
            let updateTime = baseInfo.modifyTime;//订单修改时间
            //if (!isOne) {
            let cacheKey = orderId + "_C" + addTime + "_U" + updateTime;
            if (addTime === this.time){
                if (this.currentTableNoHistory.includes(cacheKey)){
                    log.info("The current data has been processed and is about to be ignored. Please be informed: OrderNo{}", cacheKey);
                    return;
                }
                this.currentTableNoHistory.push(cacheKey);
            } else {
                this.time = addTime;
                this.currentTableNoHistory = [];
                this.currentTableNoHistory.push(cacheKey);
            }
            //}

            batchReadSender.send({
                "afterData": submitInfo,
                "eventType": "i",
                "tableName": this.tableName
            }, this.tableName, offset1);
        })
    }

    streamReadV(connectionConfig, nodeConfig, offset, pageSize, streamReadSender) {
        offset = this.defaultStreamReadOffset(offset);
        this.read(true, connectionConfig, nodeConfig, offset, pageSize, streamReadSender, (baseInfo, offset1, submitInfo, isOne) => {
            //let baseInfo = orderInfo.baseInfo;//订单基础信息
            let orderNo = submitInfo.get('商品明细条目ID') + baseInfo.idOfStr;//订单ID
            //let addTime = orderInfo.get('创建时间');//订单创建时间
            //let updateTime = orderInfo.get('修改时间');//订单修改时间
            let addTime = baseInfo.createTime;
            let updateTime = baseInfo.modifyTime;
            //if (!isOne) {
            let cacheKey = orderNo + "_C" + addTime + "_U" + updateTime;
            if (updateTime === this.time){
                if (this.currentTableNoHistory.includes(cacheKey)){
                    log.info("The current data has been processed and is about to be ignored. Please be informed: OrderNo{}", cacheKey);
                    return;
                }
                this.currentTableNoHistory.push(cacheKey);
            } else {
                this.time = updateTime;
                this.currentTableNoHistory = [];
                this.currentTableNoHistory.push(cacheKey);
            }


            offset1[this.tableName].updateTimeStart = updateTime;
            //}
            streamReadSender.send({
                "afterData": submitInfo,
                "eventType": !isValue(updateTime) || updateTime === addTime ? "i" : "u",
                "tableName": this.tableName,
            }, this.tableName, offset1);
        })
    }

    defaultBatchReadOffset(offset) {
        try{
            if (isValue(offset)) {
                if (isValue(offset.get(this.tableName))) {
                    let off = offset.get(this.tableName);
                    if (isValue(off.get("hasNext")) && !off.get("hasNext")) {
                        offset = {};
                        offset[this.tableName] = {
                            "hasNext": true,
                            "page": 1,
                            "pageSize": 50,
                            "createEndTime": dateUtils.timeStamp2Date(BigInt(new Date().getTime()), "yyyyMMddHHmmsssss") + "+0800",
                            "createStartTime": "19700101000000000+0800"
                        };
                        this.currentTableNoHistory = [];
                        this.time = "0";
                        return offset;
                    } else {
                        let newOffset = {};
                        newOffset[this.tableName] = {
                            "hasNext": off.get("hasNext"),
                            "page": off.get("page"),
                            "pageSize": off.get("pageSize"),
                            "createEndTime": off.get("createEndTime"),
                            "createStartTime": off.get("createStartTime")
                        };
                        return newOffset;
                    }
                } else {
                    //@TODO 不是ShippingOrder表就重置
                    throw ("empty offset");
                }
            } else {
                throw ("empty offset");
            }
        } catch (e) {
            offset = {};
            if (!isValue(offset[this.tableName])) {
                offset[this.tableName] = {
                    "hasNext": true,
                    "page": 1,
                    "pageSize": 50,
                    "createEndTime": dateUtils.timeStamp2Date(BigInt(new Date().getTime()), "yyyyMMddHHmmsssss") + "+0800",
                    "createStartTime": "19700101000000000+0800"
                };
                this.currentTableNoHistory = [];
                this.time = "0";
            }
            if (!isValue(offset[this.tableName].hasNext) || !(typeof (offset[this.tableName].hasNext) === 'boolean')) offset[this.tableName].hasNext = true;
            if (!isValue(offset[this.tableName].page) || !(typeof (offset[this.tableName].page) === 'number')) offset[this.tableName].page = 1;
            if (!isValue(offset[this.tableName].pageSize) || !(typeof (offset[this.tableName].pageSize) === 'number')) offset[this.tableName].pageSize = 200;
            if (!isValue(offset[this.tableName].createEndTime)) offset[this.tableName].createEndTime = dateUtils.timeStamp2Date(BigInt(new Date().getTime()), "yyyyMMddHHmmsssss") + "+0800";
            if (!isValue(offset[this.tableName].createStartTime)) offset[this.tableName].createStartTime = '19700101000000000+0800';
            return offset;
        }
    }

    defaultStreamReadOffset(offset){
        let timeStream = !isNaN(offset) ? offset : 0;
        if (!isValue(offset) || !(offset instanceof Map) || typeof (offset) !== 'object'){
            offset = {};
        }
        if (!isValue(offset[this.tableName])){
            offset[this.tableName] = {
                "hasNext": true,
                "page": 1,
                "pageSize": 50,
                "modifyEndTime": "29991231235959000+0800",
                "modifyStartTime": dateUtils.timeStamp2Date(BigInt(0 !== timeStream ? timeStream : new Date().getTime()), "yyyyMMddHHmmsssss") + '+0800',
            }
        }
        if (!isValue(offset[this.tableName].hasNext) || !(typeof (offset[this.tableName].hasNext) === 'boolean')) offset[this.tableName].hasNext = true;
        if (!isValue(offset[this.tableName].page) || !(typeof (offset[this.tableName].page) === 'number')) offset[this.tableName].page = 1;
        if (!isValue(offset[this.tableName].pageSize) || !(typeof (offset[this.tableName].pageSize) === 'number')) offset[this.tableName].pageSize = 200;
        if (!isValue(offset[this.tableName].modifyStartTime)) offset[this.tableName].modifyStartTime = dateUtils.timeStamp2Date(BigInt(0 !== timeStream ? timeStream : new Date().getTime()), "yyyyMMddHHmmsssss") + '+0800';
        if (!isValue(offset[this.tableName].modifyEndTime)) offset[this.tableName].modifyEndTime = '29991231235959000+0800';
        return offset;
    }

    read(isStreamRead, connectionConfig, nodeConfig, offset, pageSize, streamReadSender, eventHandle){
        let apiKey = connectionConfig.appKey;
        if (!isParam(apiKey) || null == apiKey || "" === apiKey.trim()){
            log.error("The App Key has expired. Please contact technical support personnel");
            return null;
        }
        let secretKey = connectionConfig.secretKey;
        if (!isParam(secretKey) || null == secretKey || "" === secretKey.trim()){
            log.error("The App Secret has expired. Please contact technical support personnel");
            return null;
        }
        //let apiFactoryImpl = new APIFactoryImpl(connectionConfig, nodeConfig);

        while(isAlive() && offset[this.tableName].hasNext){
            let timeStamp = new Date().getTime();
            let accessToken = getConfig("access_token");
            let apiConfig = {
                "page": offset[this.tableName].page,
                "pageSize": 200,
                "_aop_signature": "",
                "_aop_timestamp": BigInt(timeStamp),
                "needBuyerAddressAndPhone": true,
                "needMemoInfo": true
            };
            //{"createEndTime": {{createEndTime}},"createStartTime": {{createStartTime}},"isHis":true,"modifyEndTime":{{modifyEndTime}},"modifyStartTime":{{modifyStartTime}}, "page": {{page}},"pageSize": {{pageSize}},"needMemoInfo": true,"needBuyerAddressAndPhone": true, "bizTypes": ["yp","cn","ws","yf","fs","cz","ag","hp","gc","supply","nyg","factory","quick","xiangpin","nest","f2f","cyfw","sp","wg","factorysamp","factorybig"]}
            //page={{page}}&pageSize={{pageSize}}&needBuyerAddressAndPhone=true&needMemoInfo=true&isHis=true
            let singMap = {
                "access_token": accessToken,
                "_aop_timestamp": BigInt(timeStamp),

                "page": offset[this.tableName].page,
                "pageSize": 200,
                "needBuyerAddressAndPhone": true,
                "needMemoInfo": true
                //"bizTypes": "[\"yp\",\"cn\",\"ws\",\"yf\",\"fs\",\"cz\",\"ag\",\"hp\",\"gc\",\"supply\",\"nyg\",\"factory\",\"quick\",\"xiangpin\",\"nest\",\"f2f\",\"cyfw\",\"sp\",\"wg\",\"factorysamp\",\"factorybig\"]"
            }
            if (isStreamRead){
                apiConfig.modifyStartTime = offset[this.tableName].modifyStartTime;
                apiConfig.modifyEndTime = offset[this.tableName].modifyEndTime;
                if (apiConfig.createStartTime)
                    delete apiConfig.createStartTime;
                if (apiConfig.createEndTime)
                    delete apiConfig.createEndTime;

                singMap.modifyStartTime = apiConfig.modifyStartTime;
                singMap.modifyEndTime = apiConfig.modifyEndTime;
                if (singMap.createStartTime)
                    delete singMap.createStartTime;
                if (singMap.createEndTime)
                    delete singMap.createEndTime;
                //modifyStartTime={{modifyStartTime}}&modifyEndTime={{modifyEndTime}}
            }else {
                apiConfig.createStartTime = offset[this.tableName].createStartTime;
                apiConfig.createEndTime = offset[this.tableName].createEndTime;
                if (apiConfig.modifyStartTime)
                    delete apiConfig.modifyStartTime;
                if (apiConfig.modifyEndTime)
                    delete apiConfig.modifyEndTime;

                singMap.createStartTime = apiConfig.createStartTime
                singMap.createEndTime = apiConfig.createEndTime
                if (singMap.modifyStartTime)
                    delete singMap.modifyStartTime;
                if (singMap.modifyEndTime)
                    delete singMap.modifyEndTime;
                //&createStartTime={{createStartTime}}&createEndTime={{createEndTime}}
            }

            apiConfig._aop_signature = getSignatureRules(secretKey, "param2/1/com.alibaba.trade/alibaba.trade.getBuyerOrderList/" + apiKey, singMap);

            if (isStreamRead){
                apiConfig.modifyStartTime = offset[this.tableName].modifyStartTime.replace("+","%2B");
                apiConfig.modifyEndTime = offset[this.tableName].modifyEndTime.replace("+","%2B");
            }else {
                apiConfig.createStartTime = offset[this.tableName].createStartTime.replace("+","%2B");
                apiConfig.createEndTime = offset[this.tableName].createEndTime.replace("+","%2B");
            }
            let orders = invoker.invoke("OrderListOfBuyer-" + (!isStreamRead ?  "batch" : "stream"), apiConfig);

            if (!isParam(orders) || null == orders){
                log.warn("Can not get any order with http response.");
                return null;
            }
            let httpRes = orders.result;
            if (!isParam(httpRes) || null == httpRes){
                log.warn("Can not get any order in response body.");
                return null;
            }
            let pageList = httpRes.result;
            //log.warn("{}", JSON.stringify(orders.result));
            if (!isValue(pageList)){
                log.warn("Can not get order list, http code {}{}{}{}",
                    orders.httpCode,
                    isValue(httpRes.error_code)?(", error_code:" + httpRes.error_code) : "",
                    isValue(httpRes.error_message)? (", error_message: " + httpRes.error_message) : "",
                    isValue(httpRes.exception) ? (", exception: " + httpRes.exception) : ""
                );
                return null;
            }
            let pageNo = offset[this.tableName].page;
            let count = 0;
            try {
                count = !isNaN(httpRes.totalRecord) ? httpRes.totalRecord : parseInt(httpRes.totalRecord);
            }catch (e){
                log.warn(exceptionUtil.eMessage(e));
                return null;
            }
            let pageSize = offset[this.tableName].pageSize;

            try{
                offset[this.tableName].page = !isNaN(pageNo) ? (pageNo + 1) : (parseInt(pageNo) + 1);
                //log.warn("index: {}, size: {}" , offset[this.tableName].pageIndex, pageList.length)
                offset[this.tableName].hasNext = (((pageNo - 1) * pageSize + pageList.length) < count);
                //log.warn("Has next: {}" , offset[this.tableName].hasNext)
            }catch (e){
                log.warn(exceptionUtil.eMessage(e));
                return null;
            }
            if(!isValue(pageList)){
                log.warn("Can not get order list in http result, list data is empty.");
                continue;
            }
            for (let index = 0; index < pageList.length; index++) {
                let record = handleRecord(pageList[index]);

                let idOfStr = record.idOfStr;

                //获取物流信息
                let needLogistics = connectionConfig.getLogistics;
                if (!(isValue(needLogistics) && !needLogistics)){
                    let commandInfo = {
                        "command": "executeQuery",
                        "params": {
                            "params":{
                                "orderId": record.idOfStr
                            },
                            "funcName": "getLogisticsInfos"
                        },
                    }
                    let callCommand = execCommand.command(connectionConfig, nodeConfig, commandInfo);
                    if (isValue(callCommand)) {
                        let logistics = callCommand.command();
                        if (isValue(logistics)) {
                            let logisticsRecords = new Array();
                            logistics.forEach(it => {
                                if (!it.orderEntryIds) {
                                    it.orderEntryIds = it.order;
                                    it.logisticsId = it.order;
                                    delete it.order;

                                    it.status = "NONE";
                                    //物流状态。WAITACCEPT:未受理;CANCEL:已撤销;ACCEPT:已受理;TRANSPORT:运输中;NOGET:揽件失败;SIGN:已签收;UNSIGN:签收异常
                                }
                                logisticsRecords.push(it);
                            })
                            if (logisticsRecords.length > 0) {
                                pageList[index].logistics = logisticsRecords;
                                record.logistics = logisticsRecords;
                            }
                        }
                    }
                }

                //获取订单详情
                //let detail = apiFactoryImpl.getOrderDetail(idOfStr);
                //let orderMixedDetail = apiFactoryImpl.setOrderDetailToOrderInfo(record, detail);

                let orderInfo = this.csv(record);
                let afterEvent = [];
                let isOne = false;
                for (let indexX = 0; indexX < orderInfo.length; indexX++) {
                    if (!isOne) isOne = true;
                    let finalHandle1 = this.finalHandle(orderInfo[indexX]);
                    afterEvent.push(finalHandle1);
                    eventHandle(record, offset, finalHandle1, isOne);
                }

                if (isValue(nodeConfig.logCompile) && nodeConfig.logCompile) {
                    log.warn("Original data of Simple about 订单编号={}: {}", idOfStr, JSON.stringify(pageList[index]))
                    //log.warn("Original data of Detail about 订单编号={}: {}", idOfStr, JSON.stringify(orderMixedDetail))
                    log.warn("After data about 订单编号={}: {}", idOfStr, tapUtil.fromJson(afterEvent.length === 1? afterEvent[0] : afterEvent))
                } else {
                    let filterOrders = nodeConfig.filter.split(",");
                    if (filterOrders.includes(idOfStr)) {
                        log.warn("Original data of Simple about 订单编号={}: {}", idOfStr, JSON.stringify(pageList[index]))
                        //log.warn("Original data of Detail about 订单编号={}: {}", idOfStr, JSON.stringify(orderMixedDetail))
                        log.warn("After data about 订单编号={}: {}", idOfStr, tapUtil.fromJson(afterEvent.length === 1 ? afterEvent[0] : afterEvent))
                    }
                }
            }
        }
    }

    csv(record) {
        let newRecord = new LinkedHashMap();
        newRecord.put('订单编号', bigintConvertAsString(record.idOfStr));
        newRecord.put('买家主账号ID', bigintConvertAsString(record.buyerID));
        newRecord.put('交易ID',  bigintConvertAsString(record.id));
        newRecord.put('卖家主账号ID', bigintConvertAsString(record.sellerID));
        newRecord.put('采购账号', record.buyerLoginId);
        newRecord.put('发货时间', convertDateStr(record.allDeliveredTime));
        newRecord.put('买家备忘信息', record.buyerMemo);
        newRecord.put('完成时间', convertDateStr(record.completeTime));
        newRecord.put('创建时间', convertDateStr(record.createTime));
        newRecord.put('修改时间', convertDateStr(record.modifyTime));
        newRecord.put('付款时间', convertDateStr(record.payTime));
        newRecord.put('收货时间', convertDateStr(record.receivingTime));
        newRecord.put('退款金额（元）', coverFloat(record.refund));
        newRecord.put('买家留言', record.buyerFeedback);//remark
        newRecord.put('运费（元）', coverFloat(record.shippingFee));
        newRecord.put('应付款总金额（元）', coverFloat(record.totalAmount));
        newRecord.put('买家备忘标志', record.buyerRemarkIcon);
        newRecord.put('折扣信息（元）',coverFloat(record.discount / 100));
        //newRecord.put('子单实付金额（不包含运费）', record.sumProductPayment);
        newRecord.put('币种', record.currency);
        newRecord.put('订单最后修改时间', convertDateStr(record.modifyTime));
        newRecord.put('供应商旺旺号', bigintConvertAsString(record.sellerLoginId));

        //业务类型。
        //国际站：ta(信保),wholesale(在线批发)。
        //中文站：普通订单类型 = "cn"; 大额批发订单类型 = "ws"; 普通拿样订单类型 = "yp"; 一分钱拿样订单类型 = "yf";
        //        倒批(限时折扣)订单类型 = "fs"; 加工定制订单类型 = "cz"; 协议采购订单类型 = "ag"; 伙拼订单类型 = "hp";
        //        供销订单类型 = "supply"; 淘工厂订单 = "factory"; 快订下单 = "quick"; 享拼订单 = "xiangpin"; 当面付 = "f2f";
        //        存样服务 = "cyfw"; 代销订单 = "sp"; 微供订单 = "wg";零售通 = "lst";
        let bzType = record.businessType;
        let typeName = '';
        switch (bzType) {
            case 'ta': typeName = '信保';break;
            case 'wholesale':  typeName = '在线批发';break;
            case 'cn':  typeName = '普通订单';break;
            case 'ws':  typeName = '大额批发订单';break;
            case 'yp':  typeName = '普通拿样订单';break;
            case 'yf':  typeName = '一分钱拿样订单';break;
            case 'fs':  typeName = '倒批(限时折扣)订单';break;
            case 'cz':  typeName = '加工定制订单';break;
            case 'ag':  typeName = '协议采购订单';break;
            case 'hp':  typeName = '伙拼订单';break;
            case 'supply':  typeName = '供销订单';break;
            case 'factory':  typeName = '淘工厂订单';break;
            case 'quick':  typeName = '快订下单';break;
            case 'xiangpin':  typeName = '享拼订单';break;
            case 'f2f':  typeName = '当面付';break;
            case 'cyfw':  typeName = '存样服务';break;
            case 'sp':  typeName = '代销订单';break;
            case 'wg':  typeName = '微供订单';break;
            case 'lst':  typeName = '零售通';break;
            default: typeName = '-';
        }
        newRecord.put('业务类型', typeName);

        //交易状态，waitbuyerpay:等待买家付款;waitsellersend:等待卖家发货;waitlogisticstakein:等待物流公司揽件;
        //waitbuyerreceive:等待买家收货;waitbuyersign:等待买家签收;signinsuccess:买家已签收;confirm_goods:已收货;
        //success:交易成功;cancel:交易取消;terminated:交易终止;未枚举:其他状态
        let status = record.status;
        switch(status){
            case 'waitbuyerpay': typeName = '等待买家付款';break;
            case 'waitsellersend': typeName = '等待卖家发货';break;
            case 'waitlogisticstakein': typeName = '等待物流公司揽件';break;
            case 'waitbuyerreceive': typeName = '等待买家收货';break;
            case 'waitbuyersign': typeName = '等待买家签收';break;
            case 'signinsuccess': typeName = '买家已签收';break;
            case 'confirm_goods': typeName = '已收货';break;
            case 'success': typeName = '交易成功';break;
            case 'cancel': typeName = '交易取消';break;
            case 'terminated': typeName = '交易终止';break;
            default: typeName = '其他状态';
        }
        newRecord.put('交易状态', typeName);

        if(record.info_tradeTerms && record.info_tradeTerms.length > 0) {
            let tradeTerm = record.info_tradeTerms[0];
            newRecord.put('阶段', '' + bigintConvert(tradeTerm.phase));
        }
        if(record.info_overseasExtraAddress){
            newRecord.put('跨境地址扩展信息', JSON.stringify(record.info_overseasExtraAddress));
        }
        if(record.info_customs){
            newRecord.put('跨境报关信息', JSON.stringify(record.info_customs));
        }

        // if(record.info_orderRateInfo){
        //   let rate = record.info_orderRateInfo.buyerRateStatus;
        //   //4:已评论,5:未评论,6;不需要评论
        //   switch(rate){
        //     case 4:newRecord['买家评价状态', '已评论';break;
        //     case 5:newRecord['买家评价状态','未评论'; break;
        //     case 6:newRecord['买家评价状态','不需要评论'; break;
        //     default:newRecord['买家评价状态','-'; break;
        //   }

        //   rate = record.info_orderRateInfo.sellerRateStatus;
        //   switch(rate){
        //     case 4:newRecord['卖家评价状态', '已评论';break;
        //     case 5:newRecord['卖家评价状态','未评论'; break;
        //     case 6:newRecord['卖家评价状态','不需要评论'; break;
        //     default:newRecord['卖家评价状态','-'; break;
        //   }
        // }

        if(record.info_orderInvoiceInfo){
            newRecord.put('发票公司', record.info_orderInvoiceInfo.invoiceCompanyName);
            let invoiceType = record.info_orderInvoiceInfo.invoiceType;
            let invoiceTypeName = '-';
            //0：普通发票，1:增值税发票，9未知类型
            switch(invoiceType){
                case '0':invoiceTypeName = '普通发票';break;
                case '1':invoiceTypeName = '增值税发票';break;
                default:invoiceTypeName = '未知类型';break;
            }
            newRecord.put('发票类型', invoiceTypeName);
            newRecord.put('本地发票号', record.info_orderInvoiceInfo.localInvoiceId);
            newRecord.put('订单ID', bigintConvertAsString(record.info_orderInvoiceInfo.orderId));

            newRecord.put('（收件人）址区域编码', record.info_orderInvoiceInfo.receiveCode);
            newRecord.put('（收件人）省市区编码对应的文案', record.info_orderInvoiceInfo.receiveCodeText);
            newRecord.put('发票收货人手机', record.info_orderInvoiceInfo.receiveMobile);
            newRecord.put('发票收货人', record.info_orderInvoiceInfo.receiveName);
            newRecord.put('发票收货人电话', record.info_orderInvoiceInfo.receivePhone);
            newRecord.put('发票收货地址邮编', record.info_orderInvoiceInfo.receivePost);
            newRecord.put('街道地址（增值税发票信息）', record.info_orderInvoiceInfo.receiveStreet);
            newRecord.put('银行账号（增值税发票信息）', record.info_orderInvoiceInfo.registerAccountId);
            newRecord.put('开户银行（增值税发票信息）', record.info_orderInvoiceInfo.registerBank);
            newRecord.put('省市区编码（增值税发票信息）', record.info_orderInvoiceInfo.registerCode);
            newRecord.put('省市区文本（增值税发票信息）', record.info_orderInvoiceInfo.registerCodeText);
            newRecord.put('注册电话（增值税发票信息）', record.info_orderInvoiceInfo.registerPhone);
            newRecord.put('街道地址（增值税发票信息）', record.info_orderInvoiceInfo.registerStreet);
            newRecord.put('纳税人识别号（增值税发票信息）', record.info_orderInvoiceInfo.taxpayerIdentify);
        }

        // if(record.info_nativeLogistics){
        //   newRecord['物流地址', record.info_nativeLogistics.address;
        //   newRecord['县/区（物流）', record.info_nativeLogistics.area;
        //   newRecord['省/市/区编码（物流）', record.info_nativeLogistics.areaCode;
        //   newRecord['城市（物流）', record.info_nativeLogistics.city;
        //   newRecord['联系人（物流）', record.info_nativeLogistics.contactPerson;
        //   newRecord['传真（物流）', record.info_nativeLogistics.fax;
        //   newRecord['手机（物流）', record.info_nativeLogistics.mobile;
        //   newRecord['省份（物流）', record.info_nativeLogistics.province;
        //   newRecord['电话（物流）', record.info_nativeLogistics.telephone;
        //   newRecord['邮编（物流）', record.info_nativeLogistics.zip;
        //   newRecord['镇/街道地址码（物流）', record.info_nativeLogistics.townCode;
        //   newRecord['镇/街道地址码（物流）', record.info_nativeLogistics.town;
        // }

        // if(record.info_guaranteesTerms){
        //   newRecord['保障条款', record.info_guaranteesTerms.assuranceInfo;
        //   let assuranceType = record.info_guaranteesTerms.assuranceType;
        //   let assuranceName = '';
        //   //国际站：TA(信保)
        //   switch(assuranceName){
        //     case 'TA': assuranceName = '信保';break;
        //     default: assuranceName = '-'
        //   }
        //   newRecord['保障方式（保障条款）', record.info_guaranteesTerms.assuranceType;
        //   assuranceType = record.info_guaranteesTerms.qualityAssuranceType;
        //   assuranceName = '';
        //   //质量保证类型。国际站：pre_shipment(发货前),post_delivery(发货后)
        //   switch(assuranceName){
        //     case 'pre_shipment': assuranceName = '发货前';break;
        //     case 'post_delivery': assuranceName = '发货后';break;
        //     default: assuranceName = '-'
        //   }
        //   newRecord['质量保证类型（保障条款）', assuranceName;
        // }

        // if(record.info_orderBizInfo){
        //   newRecord['是否采源宝订单(诚e赊)', record.info_nativeLogistics.odsCyd ? "是" : "否";
        //   newRecord['账期交易到账时间(诚e赊)', convertDateStr(record.info_nativeLogistics.accountPeriodTime);
        //   newRecord['诚e赊交易方式(诚e赊)', record.info_nativeLogistics.creditOrder ? "采用" : "未采用";
        //   if(record.info_orderBizInfo.creditOrderDetail){
        //     let creditOrderDetail = record.info_orderBizInfo.creditOrderDetail;
        //     newRecord['订单金额(诚e赊)', creditOrderDetail.payAmount;
        //     newRecord['支付时间(诚e赊)', convertDateStr(creditOrderDetail.createTime);
        //     newRecord['状态(诚e赊)', creditOrderDetail['status'];
        //     newRecord['最晚还款时间(诚e赊)', creditOrderDetail.gracePeriodEndTime;
        //     newRecord['状态描述(诚e赊)', creditOrderDetail.statusStr;
        //     newRecord['应还金额(诚e赊)', creditOrderDetail.restRepayAmount;
        //   }
        //   if(record.info_orderBizInfo.preOrderInfo){
        //     let preOrderInfo = record.info_orderBizInfo.preOrderInfo;
        //     newRecord['创建预订单的appkey', preOrderInfo.appkey;
        //     newRecord['传入市场名', preOrderInfo.marketName;
        //     newRecord['当前查询的ERP创建', preOrderInfo.createPreOrderApp ?
        //     '预订单为当前查询的通过当前查询的ERP创建' :
        //     '预订单不为当前查询的通过当前查询的ERP创建';
        //   }
        // }
        if(record.sellerContact) {
            let sellerContact = record.sellerContact;
            newRecord.put("供应商", sellerContact.companyName);
            newRecord.put("供应商座机",  sellerContact.phone);
            newRecord.put("供应商手机号码", sellerContact.mobile);
            newRecord.put("供应商旺旺号", sellerContact.imInPlatform);
            newRecord.put("供应商联系人", sellerContact.name);
        }

        // if (record.nativeLogistics) {
        //     let logistics = record.nativeLogistics;
        //
        // }

        if(record.info_productItems){
            let items = record.info_productItems;
            let records = [];
            let hasShippingFee = false;
            items.forEach(r => {
                let sub = new LinkedHashMap();
                let keys = tapUtil.keysFromMap(newRecord);//Object.keys(newRecord);
                for(let index = 0 ;index < keys.length; index++){
                    let key = keys[index];
                    sub.put(key, newRecord.get(key));
                }
                if(!hasShippingFee) {
                    hasShippingFee = true;
                } else {
                    sub.put('运费（元）', 0);
                }
                sub.put('单品货号', r.cargoNumber);
                sub.put('描述', r.description);
                sub.put('子单实付金额（不包含运费）', r.itemAmount);
                sub.put('商品名称', r.name);
                sub.put('子单原始单价（元）', r.price);
                sub.put('产品ID（非在线产品为空）', bigintConvertAsString(r.productID));
                if(r.productImgUrl){
                    let urls = r.productImgUrl;
                    let allUrl = '';
                    for(let index = 0; index < urls.length; index++){
                        allUrl = allUrl + urls[index] + "; ";
                        // if(urls.length > index + 1){
                        //   allUrl + ', ';
                        // }
                    }
                    sub.put('商品图片url', allUrl);
                }
                sub.put('产品快照url', r.productSnapshotUrl);
                sub.put('子单数量', r.quantity);
                sub.put('退款金额（元）', r.refund);
                sub.put('skuID', bigintConvertAsString(r.skuID));
                sub.put('排序字段', r.sort);
                // sub['子订单状态'] = r.status);
                sub.put('商品明细条目ID', bigintConvertAsString(r.subItemID));
                // sub['类型'] = r.type;
                sub.put('售卖单位', r.unit);
                sub.put('重量', r.weight);
                sub.put('重量单位', r.weightUnit);
                sub.put('商品货号', r.productCargoNumber);
                sub.put('子单涨价或折扣', r.entryDiscount / 100);
                sub.put('订单销售属性ID', bigintConvertAsString(r.specId));
                sub.put('精度系数', r.quantityFactor);
                sub.put('子订单状态描述', r.statusStr);

                //WAIT_SELLER_AGREE 等待卖家同意
                //REFUND_SUCCESS 退款成功
                //REFUND_CLOSED 退款关闭
                //WAIT_BUYER_MODIFY 待买家修改
                //WAIT_BUYER_SEND 等待买家退货
                //WAIT_SELLER_RECEIVE 等待卖家确认收货
                let st = r.refundStatus;
                let stName = '';
                switch(st){
                    case 'WAIT_SELLER_AGREE': stName = '等待卖家同意';break;
                    case 'REFUND_SUCCESS': stName = '退款成功';break;
                    case 'REFUND_CLOSED': stName = '退款关闭';break;
                    case 'WAIT_BUYER_MODIFY': stName = '待买家修改';break;
                    case 'WAIT_BUYER_SEND': stName = '等待买家退货';break;
                    case 'WAIT_SELLER_RECEIVE': stName = '等待卖家确认收货';break;
                    default: stName = '-';
                }
                sub.put('退货状态', stName);

                sub.put('关闭原因', r.closeReason);

                // st = r.logisticsStatus;
                // //1 未发货 2 已发货 3 已收货 4 已经退货
                // //5 部分发货 8 还未创建物流订单
                // stName = '';
                // switch(st){
                //   case 1: stName = '未发货';break;
                //   case 2: stName = '已发货';break;
                //   case 3: stName = '已收货';break;
                //   case 4: stName = '已经退货';break;
                //   case 5: stName = '部分发货';break;
                //   case 8: stName = '还未创建物流订单';break;
                //   default: stName = '未知';
                // }
                // sub['物流状态'] = stName;
                sub.put('售中退款单号', r.refundId);
                sub.put('售后退款单号', r.refundIdForAs);
                sub.put('子订单关联码', r.relatedCode);


                if(r.skuInfos){
                    let infoStr = "";
                    r.skuInfos.forEach(skuInfo => {
                        infoStr = infoStr + skuInfo.name + ": " + skuInfo.value + "; "
                    })
                    sub.put('SKU属性描述', infoStr);
                }
                if(record.logistics) {
                    let logistics = record.logistics;
                    logistics.forEach(logi => {
                        if(logi.sendGoods && logi.sendGoods.length > 0) {
                            let sendGoods = logi.sendGoods;
                            sendGoods.forEach(good => {
                                if(good.goodName === r.name) {
                                    sub.put("货物单位", good.unit);
                                    sub.put("货物数量", good.quantity);
                                    sub.put("货物名称", bigintConvertAsString(good.goodName));
                                } else {
                                    return;
                                }
                            });

                            if(logi.receiver) {
                                let receiver = logi.receiver;
                                //sub.put("收货人", receiver.receiverName);
                                sub.put("收货地址", bigintConvertAsString(receiver.receiverProvince) + " "
                                    + bigintConvertAsString(receiver.receiverCity) + " "
                                    + bigintConvertAsString(receiver.receiverCounty) + " "
                                    + bigintConvertAsString(receiver.receiverAddress));
                                sub.put("收货人", bigintConvertAsString(receiver.receiverName));
                                sub.put("收货区号", bigintConvertAsString(receiver.receiverCountyCode));
                                sub.put("收货手机号码", bigintConvertAsString(receiver.receiverMobile));
                            }
                            if(logi.sender) {
                                let sender = logi.sender;
                                sub.put("发货人", bigintConvertAsString(sender.senderName));
                                sub.put("发货地址", bigintConvertAsString(sender.senderProvince) + " "
                                    + bigintConvertAsString(sender.senderCity) + " "
                                    + bigintConvertAsString(sender.senderCounty) + " "
                                    + bigintConvertAsString(sender.senderAddress));
                                sub.put("发货电话", bigintConvertAsString(sender.senderMobile));
                                sub.put("发货区号", bigintConvertAsString(sender.senderCountyCode));
                            }
                            // log.info("logistics " + JSON.stringify(Pretty(logistics));
                            // log.info("logistics.sendGoods " + logistics.sendGoods);

                            sub.put("物流公司", bigintConvertAsString(logi.logisticsCompanyName));
                            sub.put("物流编号", bigintConvertAsString(logi.logisticsId));
                            sub.put("物流公司ID", bigintConvertAsString(logi.logisticsCompanyId));
                            sub.put("运单号码", bigintConvertAsString(logi.logisticsBillNo));
                        }

                        let lstatus = logi.status;
                        //物流状态。WAITACCEPT:未受理;CANCEL:已撤销;ACCEPT:已受理;
                        // TRANSPORT:运输中;NOGET:揽件失败;SIGN:已签收;UNSIGN:签收异常
                        let statusName = '';
                        switch(lstatus){
                            case 'WAITACCEPT': statusName = '未受理';break;
                            case 'CANCEL': statusName = '已撤销';break;
                            case 'ACCEPT': statusName = '已受理';break;
                            case 'TRANSPORT': statusName = '运输中';break;
                            case 'NOGET': statusName = '揽件失败';break;
                            case 'SIGN': statusName = '已签收';break;
                            case 'UNSIGN': statusName = '签收异常';break;
                            case 'NONE': statusName = '订单尚未发货';break;
                            default: statusName = logi.message;
                        }
                        sub.put("物流状态", statusName);
                        sub.put("物流备注", bigintConvertAsString(logi.remarks));
                    });

                }
                records.push(sub);
            });
            return records;
        }
        return [newRecord];
    }

    finalHandle(record){
        //计算单价
        let price = coverFloat(record['子单原始单价（元）']);
        let quantity = coverFloat(record['子单数量']);
        let entryDiscount = coverFloat(record['子单涨价或折扣']);
        let itemAmount = coverFloat(record['子单实付金额（元）']);
        let redPackage = coverFloat(((price * quantity) + entryDiscount) - itemAmount);
        let total = 0;
        try{
            total = isValue(record['子单实付金额（元）']) ? parseFloat(record['子单实付金额（元）']) : 0;//实付金额
        } catch (e){
            total = 0;
        }
        let number = 0;
        try{
            number = isValue(record['子单数量']) ? parseInt(record['子单数量']) : 0;//运费
            record.put("子单实际单价（元）", coverFloat(((total + redPackage) / (number * 1.00))));//.toFixed(2))
            record.put("子单优惠金额（元）", -1 * (redPackage > 0 ? redPackage : 0));
        }catch (e ) {
            log.info("Error to set '子单实际单价（元）' into record, '子单数量' is an invalid argument ");
        }


        return record;
    }
}


var execCommand = {
    command: function (connectionConfig, nodeConfig, commandInfo) {
        let type = commandInfo.command;
        if ('undefined' === type || type == null || '' === type) return null;
        switch (type) {
            case 'executeQuery': {
                let params = commandInfo.params;
                if (!isParam(params) || null == params){
                    log.error("The funcName is empty in params map. Please contact technical support personnel");
                    return null;
                }
                let funcName = params.funcName;
                if (!isParam(funcName) || null == funcName || "" === funcName.trim()){
                    log.error("The funcName is empty. Please contact technical support personnel");
                    return null;
                }
                switch (funcName){
                    case 'getLogisticsInfos': return new CallCommandWithLogisticsInfos(connectionConfig, nodeConfig, params);
                }
            }
        }
        return null;
    }
}
class ExecuateCommand {
    connectionConfig;
    nodeConfig;
    commandInfo;
    constructor(connectionConfig, nodeConfig, commandInfo) {
        this.commandInfo = commandInfo;
        this.connectionConfig = connectionConfig;
        this.nodeConfig = nodeConfig;
    }

    command(){

    }
}

class CallCommandWithLogisticsInfos extends ExecuateCommand {
    //getLogisticsInfos
    //该方法只有结构化数据库源才能使用，可执行指定的数据库存储过程及自定义函数
    // ● funcName: getLogisticsInfos
    // ● params: 传入的参数
    //   orderId : id
    command(){
        let apiKey = this.connectionConfig.appKey;
        if (!isParam(apiKey) || null == apiKey || "" === apiKey.trim()){
            log.error("The App Key has expired. Please contact technical support personnel");
            return null;
        }
        let secretKey = this.connectionConfig.secretKey;
        if (!isParam(secretKey) || null == secretKey || "" === secretKey.trim()){
            log.error("The App Secret has expired. Please contact technical support personnel");
            return null;
        }

        let params = this.commandInfo.params;
        if (!isParam(params) || null == params){
            log.error("The commandInfo has expired. Please contact technical support personnel");
            return null;
        }
        let orderId = params.orderId;
        if (!isParam(orderId) || null == orderId){
            log.error("The Order id is empty, can not get LogisticsInfos. Please contact technical support personnel");
            return null;
        }

        let timeStamp = new Date().getTime();
        let accessToken = getConfig("access_token");
        let apiConfig = {
            "_aop_timestamp": BigInt(timeStamp),
            "fields": "company,name,sender,receiver,sendgood",
            "orderId": orderId,
            "access_token": accessToken,
            "webSite": '1688'
        };

        apiConfig._aop_signature = getSignatureRules(
            secretKey,
            "param2/1/com.alibaba.logistics/alibaba.trade.getLogisticsInfos.buyerView/" + apiKey,
            apiConfig
        );
        let logistics = invoker.invoke('getLogisticsInfos', apiConfig);
        if (!isParam(logistics) || null == logistics){
            log.warn("Can not get any logistics with Order id is {}, no http result.", orderId);
            return null;
        }
        let logisticsRes = logistics.result;
        if (!isParam(logisticsRes) || null == logisticsRes){
            log.warn("Can not get any logistics with Order id is {}, no http result.", orderId);
            return null;
        }
        let logisticsResList = logisticsRes.result;
        if (!isValue(logisticsResList)){
            let errorCode = isValue(logisticsRes.error_code) ? logisticsRes.error_code : (isValue(logisticsRes.errorCode) ? logisticsRes.errorCode : "0")
            log.info("Can not get logistics list, http code {}{}{}{}",
                logistics.httpCode,
                isValue(logisticsRes.error_code) ? ( ", error_code:" + logisticsRes.error_code) : (isValue(logisticsRes.errorCode) ? ( ", error_code:" + logisticsRes.errorCode) : ""),
                isValue(logisticsRes.error_message) ? (", error_message: " + logisticsRes.error_message) : (isValue(logisticsRes.errorMessage) ? (", error_message: " + logisticsRes.errorMessage) : ""),
                isValue(logisticsRes.exception) ? (", exception: " + logisticsRes.exception) : ""
            );
            return [{
                "order": orderId,
                "message": isValue(logisticsRes.error_message) ? logisticsRes.error_message : (isValue(logisticsRes.errorMessage) ? logisticsRes.errorMessage : "")
            }];
        }
        return logisticsResList;
    }
}

class APIFactoryImpl {
    connectionConfig;
    nodeConfig;

    constructor(connectionConfig, nodeConfig) {
        this.connectionConfig = connectionConfig;
        this.nodeConfig = nodeConfig;
    }

    getOrderList(){

    }

    getOrderDetail(orderId){
        if (!isValue(orderId)) {
            log.error("Order ID can not be empty");
            return null;
        }
        let timeStamp = new Date().getTime();
        let accessToken = getConfig("access_token");
        let apiKey = this.connectionConfig.appKey;
        if (!isParam(apiKey) || null == apiKey || "" === apiKey.trim()){
            log.error("Can not get order detail with order ID {}, the App Key has expired. Please contact technical support personnel", orderId);
            return null;
        }
        let secretKey = this.connectionConfig.secretKey;
        if (!isParam(secretKey) || null == secretKey || "" === secretKey.trim()){
            log.error("Can not get order detail with order ID {}, the App Secret has expired. Please contact technical support personnel", orderId);
            return null;
        }
        let apiConfig = {
            "_aop_signature": "",
            "_aop_timestamp": BigInt(timeStamp),
            "orderId": orderId,
            "webSite": "1688"
        };
        let singMap = {
            "_aop_timestamp": BigInt(timeStamp),
            "orderId": orderId,
            "webSite": "1688",
            "access_token": accessToken,
        }

        apiConfig._aop_signature = getSignatureRules(secretKey, "param2/1/com.alibaba.trade/alibaba.trade.get.buyerView/" + apiKey, singMap);

        let orders = invoker.invoke("getLogisticsInfos" , apiConfig);

        if (!isParam(orders) || null == orders){
            log.warn("Can not get order detail with order ID {}, http response is empty.", orderId);
            return null;
        }
        let httpRes = orders.result;
        if (!isParam(httpRes) || null == httpRes){
            log.warn("Can not get order detail with order ID {}, response body is empty.", orderId);
            return null;
        }
        let pageList = httpRes.result;
        //log.warn("{}", JSON.stringify(orders.result));
        if (!isValue(pageList)){
            log.warn("Can not get order detail with order ID {}, http code {}{}{}{}",
                orderId,
                orders.httpCode,
                isValue(httpRes.error_code)?(", error_code:" + httpRes.error_code) : "",
                isValue(httpRes.error_message)? (", error_message: " + httpRes.error_message) : "",
                isValue(httpRes.exception) ? (", exception: " + httpRes.exception) : ""
            );
            return null;
        }
        return httpRes;
    }

    setOrderDetailToOrderInfo(simpleInfo, detailInfo){
        simpleInfo.guaranteesTerms = detailInfo.guaranteesTerms;
        simpleInfo.nativeLogistics = detailInfo.nativeLogistics;
        simpleInfo.orderBizInfo = detailInfo.orderBizInfo;
        simpleInfo.productItems = detailInfo.productItems;
        simpleInfo.tradeTerms = detailInfo.tradeTerms;
        simpleInfo.receiverInfo = detailInfo.baseInfo.receiverInfo;
        simpleInfo.payChannelList = detailInfo.baseInfo.payChannelList;
        simpleInfo.sellerContact = detailInfo.baseInfo.sellerContact;
    }
}

function bigintConvert(value){
    if (!isValue(value)) return null;
    if (typeof value === 'string') return value;
    if (typeof value === 'bigint') return BigInt('' + value);
    if (!isNaN(value)) return BigInt("" + value);
    return value;
}

function bigintConvertAsString(value) {
    if (!isValue(value)) return "";
    return typeof value === "string" ? value : ("" + value);
}

function convertDateStr(dateStr) {
    if(!dateStr)
        return null;
    try {
        // 定义正则表达式
        // 匹配日期字符串，并提取年、月、日、小时、分钟、秒、毫秒和时区信息
        let year = dateStr.substr(0, 4);
        let month = dateStr.substr(4, 2);
        let day = dateStr.substr(6, 2);
        let hours = dateStr.substr(8, 2);
        let minutes = dateStr.substr(10, 2);
        let seconds = dateStr.substr(12, 2);
        let milliseconds = dateStr.substr(14, 3);
        let timezone = dateStr.substr(17, dateStr.length-17);

        let str = year + "-" + month + "-" + day + " " + hours + ":" + minutes + ":" + seconds + "." + milliseconds;
        //log.info("str " + str);
        let zone = parseInt(timezone.substring(0, 3), 10)
        //log.info("zone " + zone);
        let date = dateUtils.parseDate(str, 'yyyy-MM-dd hh:mm:ss.sss', zone);
        //log.info("date " + date);
        // 创建日期对象
        return date.getTime();
    } catch(t) {
        log.warn("Convert date string failed {}, data string: {}", t, dateStr);
    }
    return dateStr;
}

function coverFloat(value) {
    if (!isValue(value)) return 0.0;
    if (typeof value == 'number'){
        return value;
    } else {
        if (typeof value !== 'string'){
            value = '' + value;
        }
        try {
            return parseFloat(value);
        }catch (e) {
            return 0.0;
        }
    }
}
