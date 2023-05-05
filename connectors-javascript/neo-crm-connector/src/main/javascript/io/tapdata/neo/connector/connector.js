var invoker = loadAPI();

function batchRead(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {

}
function streamRead(connectionConfig, nodeConfig, offset, tableNameList, pageSize, streamReadSender) {

}
function connectionTest(connectionConfig) {
    return [{
        "test": "Example test item",
        "code": 1,
        "result": "Pass"
    }];
}
function commandCallback(connectionConfig, nodeConfig, commandInfo) {

}

function updateToken(connectionConfig, nodeConfig, apiResponse) {
    return null;
}

function discoverSchema(connectionConfig) {
    return [
        'MarketingLeads',
        'OpenSeaPool',
        'SourceMaterial',
        'ReasonForReturnOpenSeaPool',
        'OpenSeaPoolOperationLog',
        'ProductCatalog',
        'ProductCatalogTree',
        'ActivityMember'
    ];
}

