/**
 * @type global variable
 * @author Gavin
 * @description 操作时间相关工具方法
 * */
// 给Date类添加了一个新的实例方法format
// Date.prototype.format = function (fmt) {
//     // debugger;
//     let o = {
//         'M+': this.getMonth() + 1, // 月份
//         'd+': this.getDate(), // 日
//         'h+': this.getHours(), // 小时
//         'm+': this.getMinutes(), // 分
//         's+': this.getSeconds(), // 秒
//         'q+': Math.floor((this.getMonth() + 3) / 3), // 季度
//         S: this.getMilliseconds() // 毫秒
//     }
//     if (/(y+)/.test(fmt)) {
//         fmt = fmt.replace(
//             RegExp.$1,
//             (this.getFullYear() + '').substr(4 - RegExp.$1.length)
//         )
//     }
//     for (let k in o) {
//         if (new RegExp('(' + k + ')').test(fmt)) {
//             fmt = fmt.replace(
//                 RegExp.$1,
//                 RegExp.$1.length === 1 ? o[k] : ('00' + o[k]).substr(('' + o[k]).length)
//             )
//         }
//     }
//     return fmt
// }

var dateUtils = {
    /**
     *
     * */
    timeStamp2Date: function (millSecondsStr, format){
        // let d = format ? new Date(millSecondsStr).format(format) : new Date(millSecondsStr).format('yyyy-MM-dd hh:mm:ss') // 默认日期时间格式 yyyy-MM-dd hh:mm:ss
        // return d.toLocaleString();
        return tapUtil.timeStamp2Date(millSecondsStr, format);
    },
    /**
     * @type function
     * @author Gavin
     * @description 获取当前并格式化 yyyy-mm-dd
     * @return 格式化（yyyy-MM-dd）年-月-日 字符串
     * @date 2023/2/13
     * */
    nowDate: function () {
        return dateUtils.formatDate(new Date().getTime());
        //return tapUtil.nowToDateStr();
    },

    /**
     * @type function
     * @author Gavin
     * @description 获取当前时间并格式化 yyyy-MM-dd hh:mm:ss
     * @return 格式化（yyyy-MM-dd hh:mm:ss）年-月-日 时:分:秒 字符串
     * @date 2023/2/13
     * */
    nowDateTime: function () {
        return dateUtils.formatDateTime(new Date().getTime());
        //return tapUtil.nowToDateTimeStr();
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
        if ('undefined' === time || null == time) return "1000-01-01";
        let date = dateUtils.timeStamp2Date(new Date().getTime(), 'yyyy-MM-dd');
        return date.length > 10 ? '9999-12-31' : date;
        //return tapUtil.longToDateStr(time);
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
        if ('undefined' === time || null == time) return "1000-01-01 00:00:00";
        let date = dateUtils.timeStamp2Date(new Date().getTime(), 'yyyy-MM-dd hh:mm:ss');
        return date.length > 10 ? '9999-12-31 23:59:59' : date;
        //return tapUtil.longToDateStr(time);
    },

    parseDate: function (dataStr, format, timeZoneNumber){
        return tapUtil.parseDate(dataStr, format, timeZoneNumber);
    }
};
