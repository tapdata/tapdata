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
                "afterData": orderInfo,
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
                "afterData": orderInfo,
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
                    "createEndTime": dateUtils.timeStamp2Date(BigInt(new Date().getTime() + 3600000*8), "yyyyMMddHHmmsssss") + "+0800",
                    "createStartTime":"19700101000000000+0800"
                };
        }
        if (!isValue(offset[this.tableName].hasNext) || !(typeof (offset[this.tableName].hasNext) === 'boolean')) offset[this.tableName].hasNext = true;
        if (!isValue(offset[this.tableName].page) || !(typeof (offset[this.tableName].page) === 'number')) offset[this.tableName].page = 1;
        if (!isValue(offset[this.tableName].pageSize) || !(typeof (offset[this.tableName].pageSize) === 'number')) offset[this.tableName].pageSize = 200;
        if (!isValue(offset[this.tableName].createEndTime)) offset[this.tableName].createEndTime = dateUtils.timeStamp2Date(BigInt(new Date().getTime() + 3600000*8), "yyyyMMddHHmmsssss") + "+0800";
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
                "modifyStartTime": dateUtils.timeStamp2Date(BigInt(0 !== timeStream ? (timeStream + 3600000*8) : (new Date().getTime() + 3600000*8)), "yyyyMMddHHmmsssss") + '+0800',
            }
        }
        if (!isValue(offset[this.tableName].hasNext) || !(typeof (offset[this.tableName].hasNext) === 'boolean')) offset[this.tableName].hasNext = true;
        if (!isValue(offset[this.tableName].page) || !(typeof (offset[this.tableName].page) === 'number')) offset[this.tableName].page = 1;
        if (!isValue(offset[this.tableName].pageSize) || !(typeof (offset[this.tableName].pageSize) === 'number')) offset[this.tableName].pageSize = 200;
        if (!isValue(offset[this.tableName].modifyStartTime)) offset[this.tableName].modifyStartTime = dateUtils.timeStamp2Date(BigInt(0 !== timeStream ? (timeStream + 3600000*8) : (new Date().getTime() + 3600000*8)), "yyyyMMddHHmmsssss") + '+0800';
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
            let signatureRule = getSignatureRules(secretKey,"param2/1/cn.alibaba.open/com.alibaba.trade/alibaba.trade.fastCreateOrder-1/" + apiKey, {
                "_aop_timestamp": timeStamp,
                "access_token": accessToken
            });
            let apiConfig = {
                "page": offset[this.tableName].pageIndex,
                "pageSize": 200,
                "_aop_signature": signatureRule,
                "_aop_timestamp": BigInt(timeStamp)
            };
            if (isStreamRead){
                apiConfig.modifyStartTime = offset[this.tableName].modifyStartTime;
                apiConfig.modifyEndTime = offset[this.tableName].modifyEndTime;
                if (apiConfig.createStartTime)
                    delete apiConfig.createStartTime;
                if (apiConfig.createEndTime)
                    delete apiConfig.createEndTime;
            }else {
                apiConfig.createStartTime = offset[this.tableName].createStartTime;
                apiConfig.createEndTime = offset[this.tableName].createEndTime;
                if (apiConfig.modifyStartTime)
                    delete apiConfig.modifyStartTime;
                if (apiConfig.modifyEndTime)
                    delete apiConfig.modifyEndTime;
            }

            let orders = invoker.invoke("OrderListOfBuyer", apiConfig);

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
                offset[this.tableName].pageIndex = !isNaN(pageNo) ? (pageNo + 1) : (parseInt(pageNo) + 1);
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
