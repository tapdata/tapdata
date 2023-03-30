/**
 *  This is the toolkit encapsulated by Tap Data.
 * */
var invoker = loadAPI().httpConfigAsGlobal({"timeout":20000});
var OptionalUtil = {
    isEmpty: function (obj) {
        return typeof (obj) == 'undefined' || null == obj;
    },
    notEmpty: function (obj) {
        return !this.isEmpty(obj);
    }
}

function iterateAllData(apiName, offset, call) {
    if (OptionalUtil.isEmpty(apiName)) {
        log.error("Please specify the corresponding paging API name or URL .");
    }
    let res;
    do {
        let response = invoker.invoke(apiName, offset);
        res = response.result;
    } while (call(res, offset));
}

function commandAndConvertData(apiName, params, call){
    if (OptionalUtil.isEmpty(apiName)) {
        log.error("Please specify the corresponding paging API name or URL .");
    }
    let invokerData = invoker.invoke(apiName, params);
    return call(invokerData.result);
}
