/**
 * @type global variable
 * @author Gavin
 * @description 操作数组相关工具方法
 * */
var arrayUtils = {
    /**
     * @type function
     * @author Gavin
     * @description 获取数组中指定的元素
     * @param array 数组
     * @param index 需要获取元素在数组的索引
     * @return Object
     * @date 2023/2/13
     * */
    elementSearch: function (array, index) {
        return tapUtil.elementSearch(array, index);
    },
    /**
     * @type function
     * @author Gavin
     * @description 获取数组中第一个元素
     * @param array 数组
     * @return Object
     * @date 2023/2/13
     * */
    firstElement: function (array) {
        return tapUtil.elementSearch(array, 0);
    },
    /**
     * @type function
     * @author Gavin
     * @description 根据指定格式将对象数组转换为新对象数组
     * @param list Array<Object> 原始对象数组
     * @param convertMatch Object 转换格式
     * @return 对象数组 Array<Object>
     *     例如执行：let newArray = arrayUtils.convertList([{'key1':1,'key2':1},{'key1':2,'key2':2}],{'key1':'key','key2':'value'});
     *     你将得到：newArray = [{'key':1,'value':1},{'key':2,'value':2}];
     * @date 2023/2/13
     * */
    convertList: function (list, convertMatch) {
        return tapUtil.convertList(list, convertMatch);
    },
    /**
     * @type function
     * @author Gavin
     * @description 判断是否数组
     * @param Object 待判断对象
     * @return boolean 是否是数组
     * @date 2023/2/13
     * */
    isArray: function (list){
        return !('undefined' === list || null == list || !(list instanceof Array));
    },
    isNotEmptyArray: function (list){
        return arrayUtils.isArray(list) && list.length > 0;
    }
}