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
            offset1.ShippingOrder.updateTimeStart = updateTime;
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
        let timeStream = !isNaN(offset) ? offset : 0;
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
                let csvRecords = this.csv(orderInfo);
                for (let index = 0; index < csvRecords.length; index++) {
                    eventHandle(updateTime, addTime, csvRecords[index], offset);
                }
            }
        }
    }

    csv(record) {
        function convertDateStr(dateStr) {
            if(!dateStr)
                return null;
            try {
                let date = DateUtil.parse(dateStr, 8);
                // 创建日期对象
                let time = date.getTime();
                if(time) {
                    return time < 10000 ? null : time;
                }
            } catch(t) {
                log.warn("convertDateStr failed " + t);
            }
            return null;
        }
        let newRecords = [];
        if(record.orderExtends && record.orderExtends.length > 0) {
            record.orderExtends.forEach((it) => {
                let newRecord = new LinkedHashMap();
                newRecord.put('备货状态', record.prepareTypeName);
                newRecord.put('添加时间', convertDateStr(record.addTime));
                newRecord.put('发货时间', convertDateStr(record.deliveryTime));
                newRecord.put('入仓时间', convertDateStr(record.storageTime));
                newRecord.put('紧急类型', record.urgentTypeName);
                newRecord.put('要求发货时间', convertDateStr(record.requestDeliveryTime));
                newRecord.put('分单时间', convertDateStr(record.allocateTime));
                newRecord.put('类型名称', record.typeName);
                newRecord.put('订单类型名称', record.categoryName);
                // 订单类型id: "" + record.category);
                newRecord.put('仓库名称', record.warehouseName);
                // 是否JIT母单（枚举值：是、否）【场景示例：JIT母单是预下单，比如下单500，不需要发货。需要发货时，比如要发货100，会产生一个JIT子单（即普通订单）数量100。】
                newRecord.put('JIT母单', record.isJitMotherName);
                // 订单标识: record.orderMarkId);
                newRecord.put('订单标识名称', record.orderMarkName);
                newRecord.put('收货时间', convertDateStr(record.receiptTime));
                newRecord.put('币种名称', record.currencyName);
                // 币种: record.currencyId);
                // 备货类型: record.prepareTypeId);
                newRecord.put('预约送货时间', convertDateStr(record.reserveTime));
                newRecord.put('状态名称', record.statusName);
                // 状态id: record.status,
                newRecord.put('添加人', record.addUid);
                // 仓库id: record.storageId);
                newRecord.put('供应商名称', record.supplierName);
                newRecord.put('订单编号', record.orderNo);
                newRecord.put('是否送货', record.isDeliveryName);
                newRecord.put('更新时间', convertDateStr(record.updateTime));
                newRecord.put('退货时间', convertDateStr(record.returnTime));
                newRecord.put('查验时间', convertDateStr(record.checkTime));
                // 首单id: record.firstMark,
                newRecord.put('首单标识', record.firstMarkName);
                newRecord.put('供应商SKU', it.supplierSku);
                newRecord.put('商品后缀', it.suffixZh);
                newRecord.put('下单数量', it.orderQuantity);
                newRecord.put('入仓数量', it.storageQuantity);
                newRecord.put('收货数量', it.receiptQuantity);
                newRecord.put('送货数量', it.deliveryQuantity);
                newRecord.put('次品数量', it.defectiveQuantity);
                newRecord.put('skc', it.skc);
                newRecord.put('skuCode', it.skuCode);
                newRecord.put('备注', it.remark);
                newRecord.put('供应商货号', it.supplierCode);
                newRecord.put('价格', it.price);
                newRecord.put('图片', it.imgPath);

                if(record.type) {
                    if(record.type === 1) {
                        newRecord['订单类型'] = "急采";
                    } else if(record.type === 2) {
                        newRecord['订单类型'] = "备货";
                    } else {
                        newRecord['订单类型'] = "未知";
                    }
                }
                if(record.requestDeliveryQuantity != undefined) {
                    newRecord['已要求发货数量'] = record.requestDeliveryQuantity;
                }
                if(record.noRequestDeliveryQuantity != undefined) {
                    newRecord['未要求发货数'] = record.noRequestDeliveryQuantity;
                }
                if(record.alreadyDeliveryQuantity != undefined) {
                    newRecord['已发货数量'] = record.alreadyDeliveryQuantity;
                }
                newRecords.push(newRecord)
            })
        }
        return newRecords;
    }
}
