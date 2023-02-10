var batchStart = nowDate();
function discover_schema(connectionConfig) {
    let sessionToken = invoker.invoke('TAP_GET_TOKEN session api');
    let invoke = invoker.invoke('TAP_TABLE[allCard](PAGE_NONE)allCard',
        {"sessionToken": sessionToken.result.id});
    let tableList = [];
    for (let index = 0; index < invoke.result.length; index++) {
        let name = clearSpecial(invoke.result[index].name)
        tableList.push("Card_" + invoke.result[index].id + "_" + name + "" );
    }
    return tableList;
}

function clearSpecial(str) {
    return str.replaceAll(/\^|\.|\s+|\*|\?|\!|\/|\\|\$|\—+|\@|\%|\*|\~|\;|\:|\'|\"|\#|\&|\||\，|\,|\。|\`|\！|\[|\]|\？|\{|\}|\(|\)|\（|\）|\＜|\＞|\<|\>|\≤|\≥|\《|\》|\-|\+|\=/g, "");
}

function batch_read(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {
    let sessionToken = invoker.invoke('TAP_GET_TOKEN session api', {});
    let data = invoker.invoke('TAP_TABLE[allCard](PAGE_NONE)allCard',
        {"sessionToken": sessionToken.result.id});
    let id = tableName.split("_")[1];
    let thisCard = {};
    for (let index = 0; index < data.result.length; index++) {
        if (data.result[index].id == id) {
            thisCard = data.result[index];
            break;
        }
    }
    let invoke = invoker.invoke(
        'TAP_TABLE[queryExportFormat](PAGE_NONE:data)queryExportFormat',
        {"card-id": parseInt(id),"sessionToken": sessionToken.result.id});
    let resut = [];
    let temp = invoke.result;
    for (let index = 0; index < temp.length; index++) {
        log.warn(typeof temp[index]);
        temp[index]['Question_Name'] = thisCard.name;
        temp[index]['Question_ID'] = thisCard.id;
        temp[index]['Current_Date'] = nowDate();
        resut.push(temp[index]);
    }
    batchReadSender.send(resut, {}, false);
}

function ConnectionTest(connectionConfig) {

}
function connectionTest(connectionConfig) {

}
function connection_test(connectionConfig) {
    let sessionToken = invoker.invoke('TAP_GET_TOKEN session api');
    let invoke = invoker.invoke('TAP_TABLE[allCard](PAGE_NONE)allCard',
        {"sessionToken": sessionToken.result.id});
    return [{"TEST": " Check the account read database permission.", "CODE": invoke ? 1 : -1, "RESULT": invoke ? "Pass" : "Not pass"}];
}

