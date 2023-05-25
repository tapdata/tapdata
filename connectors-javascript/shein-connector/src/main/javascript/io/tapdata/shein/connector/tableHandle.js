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

    }

    streamRead(connectionConfig, nodeConfig, offset, pageSize, streamReadSender) {

    }
}