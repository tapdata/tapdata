/**
 * @type global variable
 * @author Gavin
 * @description 操作CSV相关工具方法
 * */
var csvUtils = {
    format: function (arrData) {
        if (undefined === arrData || null == arrData) return 'null';
        if (!Array.isArray(arrData)) {
            if (typeof (arrData) == "object" || arrData instanceof Map) arrData = [arrData];
        }
        let csv = '', row = '';
        for (let index in arrData[0])  row += index + ',';
        row = row.slice(0, -1)
        csv += row + '\r\n';
        for (let i = 0; i < arrData.length; i++) {
            let rows = '';
            for (let index in arrData[i]) {
                let arrValue = arrData[i][index] == null ? '' : '' + arrData[i][index];
                rows += arrValue + ',';
            }
            rows = rows.slice(0, rows.length - 1);
            csv += rows + '\r\n';
        }
        return csv;
    }
}