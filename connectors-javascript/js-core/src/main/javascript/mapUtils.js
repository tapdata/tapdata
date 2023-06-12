/**
 * @type global variable
 * @author Gavin
 * @description 操作对象（Map）相关工具方法
 * */
var mapUtils = {
    /**
     * @type function
     * @author Gavin
     * @description 将java js引擎包装过来的java对象转成js Map。
     * */
    asMap: function (obj){
        if (obj.toJSONString) {
            return JSON.parse(obj.toJSONString());
        }
        return tapUtil.toMap(obj);
    },
    /**
     * @type function
     * @author Gavin
     * @description 将Map中的所有kv根据key的进行排序，排序后生成一个k-v有序数组
     * @such as:
     *  input : {"no": 1, "name": "Gavin", "addr":"Tapdata"}
     *  output: ["addrTapdata", "nameGavin", "no1"]
     * */
    getParamsWithKeyValue: function (params){
        let keys = Object.keys(params);
        let paramArr = [];
        for (let i = 0; i < keys.length; i++) {
            let key = keys[i];
            let param = params[key];
            paramArr.push(key + "" + param);
        }
        paramArr.sort();
        return paramArr;
    },
    /**
     * @type function
     * @author Gavin
     * @description 将Map中的所有kv根据key的进行排序，排序后根据指定分割符生成一个k-v有序字符串， 默认分割符为空字符串
     * @such as:
     *  input : {"no": 1, "name": "Gavin", "addr":"Tapdata"}
     *  output: "addrTapdatanameGavinno1
     * */
    getParamsWithKeyValueAsString: function (params, splitStr){
        return mapUtils.getParamsWithKeyValue(params, splitStr)
            .join(('undefined' === splitStr || null == splitStr)? "" : splitStr);
    }
}