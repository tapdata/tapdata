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
        this.read(false, connectionConfig, offset, pageSize, batchReadSender, (orderInfo, offset1) => {
            let baseInfo = orderInfo.baseInfo;
            let orderId = orderInfo.id;
            let addTime = baseInfo.createTime;
            let updateTime = baseInfo.modifyTime;

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

            batchReadSender.send({
                "afterData": handleRecord(orderInfo),
                "eventType": "i",
                "tableName": this.tableName
            }, this.tableName, offset1);
        })
    }

    streamReadV(connectionConfig, nodeConfig, offset, pageSize, streamReadSender) {
        offset = this.defaultStreamReadOffset(offset);
        this.read(true, connectionConfig, offset, pageSize, streamReadSender, (orderInfo, offset1) => {
            let baseInfo = orderInfo.baseInfo;//订单基础信息
            let orderNo = orderInfo.id;//订单ID
            let addTime = baseInfo.createTime;//订单创建时间
            let updateTime = baseInfo.modifyTime;//订单修改时间

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
            streamReadSender.send({
                "afterData": handleRecord(orderInfo),
                "eventType": !isValue(updateTime) || updateTime === addTime ? "i" : "u",
                "tableName": this.tableName,
            }, this.tableName, offset1);
        })
    }

    defaultBatchReadOffset(offset) {
        if (!isValue(offset)){
            offset = {};
        }
        if (!isValue(offset[this.tableName])){
            offset[this.tableName] = {
                    "hasNext":true,
                    "page": 1,
                    "pageSize": 50,
                    "createEndTime": dateUtils.timeStamp2Date(BigInt(new Date().getTime()), "yyyyMMddHHmmsssss") + "+0800",
                    "createStartTime":"19700101000000000+0800"
                };
        }
        if (!isValue(offset[this.tableName].hasNext) || !(typeof (offset[this.tableName].hasNext) === 'boolean')) offset[this.tableName].hasNext = true;
        if (!isValue(offset[this.tableName].page) || !(typeof (offset[this.tableName].page) === 'number')) offset[this.tableName].page = 1;
        if (!isValue(offset[this.tableName].pageSize) || !(typeof (offset[this.tableName].pageSize) === 'number')) offset[this.tableName].pageSize = 200;
        if (!isValue(offset[this.tableName].createEndTime)) offset[this.tableName].createEndTime = dateUtils.timeStamp2Date(BigInt(new Date().getTime()), "yyyyMMddHHmmsssss") + "+0800";
        if (!isValue(offset[this.tableName].createStartTime)) offset[this.tableName].createStartTime = '19700101000000000+0800';
        return offset;
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

    read(isStreamRead, connectionConfig, offset, pageSize, streamReadSender, eventHandle){
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
                let orderInfo = pageList[index];
                eventHandle(orderInfo, offset);
            }
        }
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
class ExecuateCommand{
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
            log.warn("Can not get logistics list, http code {}{}{}{}",
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
