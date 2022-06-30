/**
 * @author lg<lirufei0808@gmail.com>
 * @date 2021/8/5 下午6:08
 * @description
 */

const { sign } = require('./index');

const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'.split('');
function randomStr(count) {
    count = count >= 0 ? count : 0;
    let str = '';
    let charLengths = chars.length;
    while (true) {
        count--;
        if (count === 0) {
            return str;
        }
        str += chars[Number((Math.random() * charLengths).toFixed(0))]
    }
}

const accessKey = '4rPGQhEOChGhUOryAhgLiodaTuqsvXuv';
const secretKey = '4IqWVsLqshfc6c2Hk1bGsIzYmIpVgf6L';
let params = {
    ts: new Date().getTime(),
    nonce: randomStr(10),
    signVersion: '1.0',
    accessKey: '4rPGQhEOChGhUOryAhgLiodaTuqsvXuv',
    sign: 'test'
};

let canonicalQueryString = sign.canonicalQueryString(params);
let stringToSign = "GET:" + canonicalQueryString;
let _sign = sign.signString(stringToSign, secretKey);
params.sign = _sign;

console.log(params.sign);

let queryString = Object.keys(params)
    .map(key => {
        let value = encodeURIComponent(params[key]);
        if (Array.isArray(value)) {
            return value.map( val => encodeURIComponent(key) + "=" + encodeURIComponent(val)).join('&');
        } else {
            return encodeURIComponent(key) + "=" + encodeURIComponent(value);
        }
    })
    .join("&");

console.log(queryString);
