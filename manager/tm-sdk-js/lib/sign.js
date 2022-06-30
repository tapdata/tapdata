/**
 * @author lg<lirufei0808@gmail.com>
 * @date 2021/8/5 下午5:45
 * @description
 */

const crypto = require('crypto');

function canonicalQueryString(parameters = {}) {
    return Object.keys(parameters)
        .filter(key => key !== "sign")
        .sort()
        .map(key => {
            let value = encodeURIComponent(parameters[key]);
            if (Array.isArray(value)) {
                return value.map( val => encodeURIComponent(key) + "=" + encodeURIComponent(val)).join('&');
            } else {
                return encodeURIComponent(key) + "=" + encodeURIComponent(value);
            }
        })
        .join("&");
}

function signString(stringToSign = '', accessKeySecret) {
    return crypto.createHmac('sha1', accessKeySecret).update(stringToSign).digest()
        .toString('base64');
}

module.exports = {
    canonicalQueryString,
    signString
}
