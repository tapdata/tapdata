var tableHandle = {
    handle: function (tableName) {
        if ('undefined' === tableName || tableName == null || '' === tableName) return new DefaultTable();
        switch (tableName) {
            case 'ShippingOrder': return new ShippingOrder();
        }
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
    //同一秒内的数据缓存，存在是不需要再次处理，（orderNo + "_C" + addTime + "_U" + updateTime）
    shippingOrderNoHistory = [];
    time = -1;

    batchReadV(connectionConfig, nodeConfig, offset, pageSize, batchReadSender) {
        offset = this.defaultBatchReadOffset(offset);
        this.read(connectionConfig, offset, pageSize, batchReadSender, (updateTime, addTime, orderInfo, offset1) => {
            batchReadSender.send({
                "afterData": orderInfo,
                "eventType": "i",
                "tableName": "ShippingOrder"
            }, "ShippingOrder", offset1);
        })
    }

    streamReadV(connectionConfig, nodeConfig, offset, pageSize, streamReadSender) {
        offset = this.defaultStreamReadOffset(offset);
        this.read(connectionConfig, offset, pageSize, streamReadSender, (updateTime, addTime, orderInfo, offset1) => {
            streamReadSender.send({
                "afterData": orderInfo,
                "eventType": !isValue(updateTime) || updateTime === addTime ? "i" : "u",
                "tableName": "ShippingOrder",
            }, "ShippingOrder", offset1);
        })
    }

    defaultBatchReadOffset(offset) {
        if (!isValue(offset)){
            offset = {};
        }
        if (!isValue(offset.ShippingOrder)){
            offset.ShippingOrder = {
                    'hasNext' : true,
                    'pageIndex' : 1,
                    'updateTimeStart': '1970-01-01 00:00:00',
                    'updateTimeEnd': dateUtils.timeStamp2Date(BigInt(new Date().getTime() + 3600000*8), "yyyy-MM-dd HH:mm:ss")
                 }
        }
        if (!isValue(offset.ShippingOrder.hasNext) || !(typeof (offset.ShippingOrder.hasNext) === 'boolean')) offset.ShippingOrder.hasNext = true;
        if (!isValue(offset.ShippingOrder.pageIndex) || !(typeof (offset.ShippingOrder.pageIndex) === 'number')) offset.ShippingOrder.pageIndex = 1;
        if (!isValue(offset.ShippingOrder.updateTimeStart)) offset.ShippingOrder.updateTimeStart = '1970-01-01 00:00:00';
        if (!isValue(offset.ShippingOrder.updateTimeEnd)) offset.ShippingOrder.updateTimeEnd = dateUtils.timeStamp2Date(BigInt(new Date().getTime() + 3600000*8), "yyyy-MM-dd HH:mm:ss");
        return offset;
    }

    defaultStreamReadOffset(offset){
        let timeStream = 0;
        if (!isNaN(offset)){
            timeStream = BigInt(offset);
        }
        if (!isValue(offset) || !(offset instanceof Map) || typeof (offset) !== 'object'){
            offset = {};
        }
        if (!isValue(offset.ShippingOrder)){
            offset.ShippingOrder = {
                'hasNext' : true,
                'pageIndex' : 1,
                'updateTimeStart': dateUtils.timeStamp2Date(BigInt(0 !== timeStream ? (timeStream + 3600000*8) : (new Date().getTime() + 3600000*8)), "yyyy-MM-dd HH:mm:ss"),
                'updateTimeEnd': '2999-12-31 23:59:59'
            }
        }
        if (!isValue(offset.ShippingOrder.hasNext) || !(typeof (offset.ShippingOrder.hasNext) === 'boolean')) offset.ShippingOrder.hasNext = true;
        if (!isValue(offset.ShippingOrder.pageIndex) || !(typeof (offset.ShippingOrder.pageIndex) === 'number')) offset.ShippingOrder.pageIndex = 1;
        if (!isValue(offset.ShippingOrder.updateTimeStart)) offset.ShippingOrder.updateTimeStart = dateUtils.timeStamp2Date(BigInt(0 !== timeStream ? (timeStream + 3600000*8) : (new Date().getTime() + 3600000*8)), "yyyy-MM-dd HH:mm:ss");
        if (!isValue(offset.ShippingOrder.updateTimeEnd)) offset.ShippingOrder.updateTimeEnd = '2999-12-31 23:59:59';
        return offset;
    }

    read(connectionConfig, offset, pageSize, streamReadSender, eventHandle){
        let openKeyId = connectionConfig.openKeyId;
        if (!isParam(openKeyId) || null == openKeyId || "" === openKeyId.trim()){
            log.error("Please make sure your openKeyId not empty");
            return null;
        }
        let secretKey = connectionConfig.secretKey;
        if (!isParam(secretKey) || null == secretKey || "" === secretKey.trim()){
            log.error("Please make sure your secretKey not empty");
            return null;
        }
        while(isAlive() && offset.ShippingOrder.hasNext){
            let timeStamp = new Date().getTime();
            let signatureRule = getSignatureRules(openKeyId, secretKey,"/open-api/order/purchase-order-infos", timeStamp);
            let goods = invoker.invoke("Shopping", {
                "pageNumber": offset.ShippingOrder.pageIndex,
                "pageSize": 200,
                "x-lt-signature": signatureRule,
                "x-lt-timestamp": BigInt(timeStamp),
                "updateTimeStart": offset.ShippingOrder.updateTimeStart,
                "updateTimeEnd": offset.ShippingOrder.updateTimeEnd
            });
            if (!isParam(goods) || null == goods){
                log.warn("Can not get any order with http response.");
                return null;
            }
            let result = goods.result;
            /**
             * {
                    "code":"0",
                    "info":{
                        "pageNo":1,
                        "count":1,
                        "pageSize":20,
                        "list":[]
                    },
                    "msg":""
                }
             * */
            if (!isParam(result) || null == result){
                log.warn("Can not get any order in response body.");
                return null;
            }
            let pageInfo = result.info;
            if (!isValue(pageInfo)){
                log.warn("Can not get order list, http code {}{}{}",
                    goods.httpCode,
                    isValue(result.msg)?(", msg:" + result.msg) : "",
                    isValue(result.error)?(", error: " + result.error) : ""
                );
                return null;
            }
            let pageNo = pageInfo.pageNo;

            let count = 0;
            try {
                count = !isNaN(pageInfo.count) ? pageInfo.count : parseInt(pageInfo.count);
            }catch (e){
                log.warn(exceptionUtil.eMessage(e));
                return null;
            }
            let pageSize = 0;
            try {
                pageSize = !isNaN(pageInfo.pageSize) ? pageInfo.pageSize : parseInt(pageInfo.pageSize);
            }catch (e) {
                log.warn(exceptionUtil.eMessage(e));
                return null;
            }
            let pageList = pageInfo.list;
            try{
                offset.ShippingOrder.pageIndex = !isNaN(pageNo) ? (pageNo + 1) : (parseInt(pageNo) + 1);
                //log.warn("index: {}, size: {}" , offset.ShippingOrder.pageIndex, pageList.length)
                offset.ShippingOrder.hasNext = (((pageNo - 1) * pageSize + pageList.length) < count);
                //log.warn("Has next: {}" , offset.ShippingOrder.hasNext)
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
                let orderNo = orderInfo.orderNo;

                let updateTime = 0;
                try {
                    updateTime = !isNaN(orderInfo.updateTime) ? orderInfo.updateTime : new Date("" + orderInfo.updateTime).getTime();
                }catch (e){
                    try {
                        updateTime = new Date("" + orderInfo.updateTime).getTime();
                    }catch (e1) {
                        updateTime = -1;
                    }
                }

                let addTime = 0;
                try {
                    addTime = !isNaN(orderInfo.addTime)? orderInfo.addTime : new Date("" + orderInfo.addTime).getTime();
                }catch (e){
                   try {
                       addTime = new Date("" + orderInfo.addTime).getTime();
                   }catch (e1){
                       log.warn("Can not get add time in order, order: {}", JSON.stringify(orderInfo));
                       continue;
                   }
                }

                let cacheKey = orderNo + "_C" + addTime + "_U" + updateTime;
                if (updateTime === this.time){
                    if (this.shippingOrderNoHistory.includes(cacheKey)){
                        log.info("The current data has been processed and is about to be ignored. Please be informed: OrderNo{}", cacheKey);
                        continue;
                    }
                    this.shippingOrderNoHistory.push(cacheKey);
                } else {
                    this.time = updateTime;
                    this.shippingOrderNoHistory = [];
                    this.shippingOrderNoHistory.push(cacheKey);
                }

                eventHandle(updateTime, addTime, orderInfo, offset);
            }
        }
    }
}
