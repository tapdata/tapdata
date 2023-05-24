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
    }
}