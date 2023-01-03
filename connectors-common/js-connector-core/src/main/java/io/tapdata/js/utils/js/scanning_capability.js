var fun = [];
var key_name_dif = [];
function _scanning_capabilities_in_java_script() {
    key_name_dif = Object.keys(new Object(this));
    for (var i = 0; i < key_name_dif.length ; i++ ) {
        if (typeof this[key_name_dif[i]] == 'function'){
            fun.push(key_name_dif[i]);
        }
    }
    return fun;
}