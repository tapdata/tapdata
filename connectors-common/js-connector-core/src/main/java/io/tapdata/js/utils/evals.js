var fun = [];
var key_name_dif = [];
function functionGet() {
    key_name_dif = Object.keys(new Object(this));
    for (var i = 0; i < key_name_dif.length ; i++ ) {
        if (typeof this[key_name_dif[i]] == 'function'){
            fun.push(key_name_dif[i]);
        }
    }
    return fun;
}