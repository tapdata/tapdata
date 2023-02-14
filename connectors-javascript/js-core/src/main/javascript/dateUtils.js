/**
 * @type global variable
 * @author Gavin
 * @description 操作时间相关工具方法
 * */
var dateUtils = {
    /**
     * @type function
     * @author Gavin
     * @description 获取当前并格式化 yyyy-mm-dd
     * @return 格式化（yyyy-MM-dd）年-月-日 字符串
     * @date 2023/2/13
     * */
    nowDate: function () {
        return tapUtil.nowToDateStr();
    },

    /**
     * @type function
     * @author Gavin
     * @description 获取当前时间并格式化 yyyy-MM-dd hh:mm:ss
     * @return 格式化（yyyy-MM-dd hh:mm:ss）年-月-日 时:分:秒 字符串
     * @date 2023/2/13
     * */
    nowDateTime: function () {
        return tapUtil.nowToDateTimeStr();
    },

    /**
     * @type function
     * @author Gavin
     * @description 根据时间戳进行格式化输出字符串 yyyy-MM-dd
     * @param time 时间戳 Number
     * @return 格式化（yyyy-MM-dd）年-月-日 字符串
     * @date 2023/2/13
     * */
    formatDate: function (time) {
        return tapUtil.longToDateStr(time);
    },

    /**
     * @type function
     * @author Gavin
     * @description 根据时间戳进行格式化输出字符串 yyyy-MM-dd hh:mm:ss
     * @param time 时间戳 Number
     * @return 格式化（yyyy-MM-dd hh:mm:ss）年-月-日 时:分:秒 字符串
     * @date 2023/2/13
     * */
    formatDateTime: function (time) {
        return tapUtil.longToDateStr(time);
    }
};
