function cloneObj(obj) {
    // let newObj = obj instanceof Array ? [] : {};
    // for(let param in obj){
    //     let item = obj[param];
    //     newObj[param] = typeof item === 'object' ? cloneObj(obj) : item;
    // }
    // return newObj;
    return JSON.parse(JSON.stringify(obj));
}

function sha256_HMAC(value, key){
    return tapUtil.sha256_HMAC(value, key);
}


