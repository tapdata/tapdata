function isParam(param) {
    return typeof (param) != 'undefined';
}


function nowDate() {
    return tapUtil.nowToDateStr();
}

function nowDateTime() {
    return tapUtil.nowToDateTimeStr();
}

function formatDate(time) {
    return tapUtil.longToDateStr(time);
}

function formatDateTime(time) {
    return tapUtil.longToDateStr(time);
}

function elementSearch(array, index) {
    return tapUtil.elementSearch(array, index);
}

function firstElement(array) {
    return tapUtil.elementSearch(array, 0);
}

function convertList(list, convertMatch) {
    return tapUtil.convertList(list, convertMatch);
}