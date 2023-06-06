package io.tapdata.js.connector.base;

import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.js.connector.base.ali.SecurityUtil;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;


public class JsUtil {
    public Object toMap(Object obj) {
        if (obj instanceof Function) {
            Function obj1 = (Function) obj;
            Object apply = obj1.apply(null);
            return apply;
        } else {
            return obj;
        }
    }

    public String nowToDateStr() {
        return this.longToDateStr(System.currentTimeMillis());
    }

    public String nowToDateTimeStr() {
        return this.longToDateTimeStr(System.currentTimeMillis());
    }

    public String longToDateStr(Long date) {
        if (null == date) return "1000-01-01";
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        String dateStr = formatter.format(new Date(date));
        return dateStr.length() > 10 ? "9999-12-31" : dateStr;
    }

    public String longToDateTimeStr(Long date) {
        if (null == date) return "1000-01-01 00:00:00";
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String dateStr = formatter.format(new Date(date));
        return dateStr.length() > 19 ? "9999-12-31 23:59:59" : dateStr;
    }

    public Object elementSearch(Object array, int index) {
        if (Objects.isNull(array)) return null;
        if (array instanceof Collection) {
            List<Object> collection = new ArrayList<>((Collection<Object>) array);
            return collection.size() >= index + 1 ? collection.get(index) : null;
        } else {
            return array;
        }
    }
    public Map<String,Object> mergeData(Object margeTarget,Object using){
        Map<String,Object> data = new HashMap<>();
        if(margeTarget instanceof Map){
            Map<String, Object> target = (Map<String, Object>) margeTarget;
            data.putAll(target);
        }
        if( using instanceof Map){
            Map<String,Object> from = (Map<String, Object>) using;
            data.putAll(from);
        }
        return data;
    }
    public Map<String,Object> mixedData(Object margeTarget,Object using){
        Map<String ,Object> data = new HashMap<>();
        if(margeTarget instanceof Map ){
            Map<String, Object> target = (Map<String, Object>) margeTarget;
            data.putAll(target);
        }
        if( using instanceof Map){
            Map<String,Object> from = (Map<String, Object>) using;
            data.putAll(from);
        }
        return data;
    }

    public Object convertList(Object list, Object convertMatch){
        if (convertMatch instanceof Map && list instanceof Collection){
            List<Map<String,Object>> listObj = (List<Map<String, Object>>) list;
            Map<String, String> match = (Map<String, String>) convertMatch;
            List<Object> afterConvert = new ArrayList<>();
            listObj.stream().filter(Objects::nonNull).forEach(map->{
                Map<String,Object> afterConvertMap = new HashMap<>();
                match.forEach((key,afterKey)->{
                    afterConvertMap.put(afterKey, map.get(key));
                });
                afterConvert.add(afterConvertMap);
            });
            return afterConvert;
        }else {
            return list;
        }
    }

    public String fromJson(Object obj){
        return Objects.isNull(obj)?"": TapSimplify.toJson(obj);
    }

    public String timeStamp2Date(Object millSecondsStr, String format){
        long millSeconds = millSecondsStr instanceof Number ? ((Number)millSecondsStr).longValue() : Long.parseLong((String.valueOf(millSecondsStr)));
        if (format == null || format.isEmpty()) {
            format = "yyyy-MM-dd HH:mm:ss";
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(new Date(millSeconds));
    }

    /**
     * sha256_HMAC加密
     * @param message 消息
     * @param secret  秘钥
     * @return 加密后字符串
     */
    public String sha256_HMAC(String message, String secret) {
        String hash = "";
        try {
            Mac sha256_HMAC = Mac.getInstance("HmacSHA256");
            SecretKeySpec secret_key = new SecretKeySpec(secret.getBytes(), "HmacSHA256");
            sha256_HMAC.init(secret_key);
            byte[] bytes = sha256_HMAC.doFinal(message.getBytes());
            hash = byteArrayToHexString(bytes);
            //System.out.println(hash);
        } catch (Exception e) {
            //System.out.println("Error HmacSHA256 ===========" + e.getMessage());
        }
        return hash;
    }
    /**
     * 将加密后的字节数组转换成字符串
     *
     * @param b 字节数组
     * @return 字符串
     */
    private String byteArrayToHexString(byte[] b) {
        StringBuilder hs = new StringBuilder();
        String stmp;
        for (int n = 0; b != null && n < b.length; n++) {
            stmp = Integer.toHexString(b[n] & 0XFF);
            if (stmp.length() == 1)
                hs.append('0');
            hs.append(stmp);
        }
        return hs.toString().toLowerCase();
    }

    public static final char[] digital = "0123456789ABCDEF".toCharArray();

    public String encodeHexAsStr(final byte[] bytes){
        if(bytes == null){
            return null;
        }
        char[] result = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            result[i*2] = digital[(bytes[i] & 0xf0) >> 4];
            result[i*2 + 1] = digital[bytes[i] & 0x0f];
        }
        return new String(result);
    }

    public byte[] decodeHexAsByteArr(final String str) {
        if(str == null){
            return null;
        }
        char[] charArray = str.toCharArray();
        if(charArray.length%2 != 0){
            throw new RuntimeException("hex str length must can mod 2, str:" + str);
        }
        byte[] bytes = new byte[charArray.length/2];
        for (int i = 0; i < charArray.length; i++) {
            char c = charArray[i];
            int b;
            if(c >= '0' && c <= '9'){
                b = (c-'0')<<4;
            }else if(c >= 'A' && c <= 'F'){
                b = (c-'A'+10)<<4;
            }else{
                throw new RuntimeException("unsport hex str:" + str);
            }
            c = charArray[++i];
            if(c >= '0' && c <= '9'){
                b |= c-'0';
            }else if(c >= 'A' && c <= 'F'){
                b |= c-'A'+10;
            }else{
                throw new RuntimeException("unsport hex str:" + str);
            }
            bytes[i/2] = (byte)b;
        }
        return bytes;
    }


    public String ali1688HmacSha1ToHexStr(String data, String key){
        return SecurityUtil.hmacSha1ToHexStr(data, key);
    }

    public static void main(String[] args) {
        JsUtil util = new JsUtil();
        String data = "param2/1/com.alibaba.trade/alibaba.trade.getBuyerOrderList/8668585_aop_timestamp1686034577415access_tokendb1e0dba-0ccb-4203-bcbf-27fc2e2fa0e1";
        String hmac = util.sha256_HMAC(data, "**J8uHFtF3MHy");
        //String hexAsStr = util.encodeHexAsStr(hmac.getBytes());
        System.out.println(hmac.toUpperCase());
        //27B46835E05251D2B0EAE628BB99C8DA9BE73DB8

        System.out.println(SecurityUtil.hmacSha1ToHexStr(data, "**J8uHFtF3MHy"));
    }
}
