/**
 * @type global variable
 * @author Skeet
 * @description try catch时，获取catch的e中内容；判断应该返回的code；
 * */
var exceptionUtil = {
    /**
     * @type function
     * @author Skeet
     * @description 返回catch的e内容
     * @date 2023/3/29
     * */
    eMessage: function (e) {
        return e.message ? e.message : e
    },
    /**
     * @type function
     * @author Skeet
     * @description 通过httpCode返回连接测试成功状态
     * @date 2023/3/29
     * */
    statusCode: function (httpCode) {
        return httpCode >= 200 && httpCode < 300 ? 1 : -1
    }
}

