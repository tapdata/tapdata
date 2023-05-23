/**
 *  This is the toolkit encapsulated by Tap Data.
 * */
var invoker = loadAPI();
var OptionalUtil = {
    isEmpty: function (obj) {
        return typeof (obj) == 'undefined' || null == obj;
    },
    notEmpty: function (obj) {
        return !this.isEmpty(obj);
    }
}

var globalTableConfig = {
    //急采
    "JiCai": {
        "supportRead": true,
        "supportWrite": false
    },
    //备货
    "BeiHuo": {
        "supportRead": true,
        "supportWrite": false
    }
}
