/**
 *  This is the toolkit encapsulated by Tap Data.
 * */
var invoker = tapAPI.loadAPI();
var OptionalUtil = {
    isEmpty: function (obj) {
        return 'undefined' == obj || null == obj;
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
    let error;
    do {
        let response = invoker.invoke(apiName, offset);
        res = (core.toMap(response.result)).data;
        error = response.error;
    } while (call(res, offset, error));
}
