var tableHandle = {
    handle: function (tableName) {
        if ('undefined' === tableName || tableName == null || '' === tableName) return new DefaultTable();
        switch (tableName) {
            case 'ShippingOrder': return new ShippingOrder();
        }
    }
}

class DefaultTable {
    batchRead(connectionConfig, nodeConfig, offset, pageSize, batchReadSender) {

    }

    streamRead(connectionConfig, nodeConfig, offset, pageSize, streamReadSender) {

    }
}

class ShippingOrder extends DefaultTable {
    batchRead(connectionConfig, nodeConfig, offset, pageSize, batchReadSender) {
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
        let hasNext = true;
        let pageIndex = 1;
        while(isAlive() && hasNext){
            let signatureRule = getSignatureRules(openKeyId, secretKey,"/open-api/order/purchase-order-infos");
            let time = new Date().getTime();
            let goods = invoker.invoke("Shopping", {
                "pageNumber": pageIndex,
                "x-lt-signature": signatureRule,
                "x-lt-timestamp":time
            });
            if (!isParam(goods) || null == goods){
                log.warn("Can not get any order with http resopnse.");
                return null;
            }
            let result = goods.result;
            if (!isParam(result) || null == result){
                log.warn("Can not get any order in response body.");
                return null;
            }
            batchReadSender.send({
                "afterData": result,
                "eventType": "i",
                "tableName": "ShippingOrder",
            }, "ShippingOrder", {'ShippingOrder': new Date().getTime()});
            hasNext = false;
        }
    }

    streamRead(connectionConfig, nodeConfig, offset, pageSize, streamReadSender) {

    }
}