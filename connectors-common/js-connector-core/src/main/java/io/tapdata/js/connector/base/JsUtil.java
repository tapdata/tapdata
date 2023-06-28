package io.tapdata.js.connector.base;

import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.js.connector.base.ali.SecurityUtil;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;


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
//        String data = "param2/1/com.alibaba.trade/alibaba.trade.getBuyerOrderList/8668585_aop_timestamp1686034577415access_tokendb1e0dba-0ccb-4203-bcbf-27fc2e2fa0e1";
//        String hmac = util.sha256_HMAC(data, "**J8uHFtF3MHy");
//        //String hexAsStr = util.encodeHexAsStr(hmac.getBytes());
//        System.out.println(hmac.toUpperCase());
//        //27B46835E05251D2B0EAE628BB99C8DA9BE73DB8
//
//        System.out.println(SecurityUtil.hmacSha1ToHexStr(data, "**J8uHFtF3MHy"));

        System.out.println(util.timeStamp2Date(System.currentTimeMillis(), "yyyyMMddHHmmsssss"));
    }

    public Date parseDate(String dataStr, String format, int timeZone){
        //SimpleDateFormat 格式化日期所用到的类
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
            SimpleTimeZone simpleTimeZone = new SimpleTimeZone(timeZone, "GMT");
            simpleDateFormat.setTimeZone(simpleTimeZone);
            return simpleDateFormat.parse(dataStr);
        } catch (Exception e){
            return null;
        }
    }

    public List<String> keysFromMap(Object map){
        if (map instanceof Map) {
            Map<String, Object> objectMap = (Map<String, Object>) map;
            return objectMap.keySet().stream().filter(Objects::nonNull).collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    public Object parse(Object dateObj) {
        Date date = null;
        if (dateObj == null) {
            return date;
        }
        try {
            if (dateObj instanceof String) {
                String dateFormat = determineDateFormat((String) dateObj);

                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
                date = simpleDateFormat.parse((String) dateObj);

            } else {
                // only can convert from long value.
                date = new Date(new BigDecimal(String.valueOf(dateObj)).longValue());
            }
        } catch (Exception e) {
            return dateObj;
        }
        return date;
    }
    public static Date parse(String dateString, String dateFormat, TimeZone timeZone) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
        //simpleDateFormat.setLenient(false); // Don't automatically convert invalid date.
        if (timeZone == null) {
            timeZone = TimeZone.getDefault();
        }
        simpleDateFormat.setTimeZone(timeZone);
        return simpleDateFormat.parse(dateString);
    }
    public Object parse(Object dateObj, Integer timezone) {
        Date date = null;
        if (dateObj == null) {
            return date;
        }
        try {
            if (timezone == null) {
                timezone = 8;
            }
            TimeZone timeZone = getTimeZone(timezone);
            if (dateObj instanceof String) {
                String dateFormat = determineDateFormat((String) dateObj);

                date = parse((String) dateObj, dateFormat, timeZone);

            } else {
                // only can convert from long value.
                date = new Date(new BigDecimal(String.valueOf(dateObj)).longValue());
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                String formatStr = sdf.format(date);
                date = (Date) parse(formatStr, timezone);
//                Calendar calendar = toCalendar(date)
//                calendar.setTimeZone(timeZone);
//                date = calendar.getTime();
            }
        } catch (Exception e) {
            return dateObj;
        }
        return date;
    }

    public static String determineDateFormat(String dateString) {
        for (String regexp : DATE_FORMAT_REGEXPS.keySet()) {
            if (dateString.matches(regexp)) {
                return DATE_FORMAT_REGEXPS.get(regexp);
            }
        }
        return null; // Unknown format.
    }
    public static TimeZone getTimeZone(Integer timeZoneOffset) {
        if (timeZoneOffset == null) {
            return TimeZone.getDefault();
        }
        StringBuilder sb = new StringBuilder("GMT");
        String str = String.valueOf(timeZoneOffset);
        if (str.length() <= 1 || (str.contains("-") && str.length() <= 2)) {
            if (str.contains("-")) {
                sb.append("-0").append(Math.abs(timeZoneOffset));
            } else {
                sb.append("+0").append(timeZoneOffset);
            }
        } else {
            sb.append(str);
        }
        sb.append(":00");

        return TimeZone.getTimeZone(sb.toString());
    }
    private static final Map<String, String> DATE_FORMAT_REGEXPS = new HashMap<String, String>() {{
        put("^\\d{8}$", "yyyyMMdd");
        put("^\\d{1,2}-\\d{1,2}-\\d{4}$", "dd-MM-yyyy");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}$", "yyyy-MM-dd");
        put("^\\d{1,2}/\\d{1,2}/\\d{4}$", "MM/dd/yyyy");
        put("^\\d{4}/\\d{1,2}/\\d{1,2}$", "yyyy/MM/dd");
        put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}$", "dd MMM yyyy");
        put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}$", "dd MMMM yyyy");
        put("^\\d{12}$", "yyyyMMddHHmm");
        put("^\\d{8}\\s\\d{4}$", "yyyyMMdd HHmm");
        put("^\\d{1,2}-\\d{1,2}-\\d{4}\\s\\d{1,2}:\\d{2}$", "dd-MM-yyyy HH:mm");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}$", "yyyy-MM-dd HH:mm");
        put("^\\d{1,2}/\\d{1,2}/\\d{4}\\s\\d{1,2}:\\d{2}$", "MM/dd/yyyy HH:mm");
        put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}$", "yyyy/MM/dd HH:mm");
        put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}\\s\\d{1,2}:\\d{2}$", "dd MMM yyyy HH:mm");
        put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}$", "dd MMMM yyyy HH:mm");
        put("^\\d{14}$", "yyyyMMddHHmmss");
        put("^\\d{8}\\s\\d{6}$", "yyyyMMdd HHmmss");
        put("^\\d{1,2}-\\d{1,2}-\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd-MM-yyyy HH:mm:ss"); // Oracle Date增量更新格式
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}.\\d{1}$", "yyyy-MM-dd HH:mm:ss.S");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}.\\d{2}$", "yyyy-MM-dd HH:mm:ss.SS");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}.\\d{3}$", "yyyy-MM-dd HH:mm:ss.SSS");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}.\\d{4}$", "yyyy-MM-dd HH:mm:ss.SSSS");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}.\\d{5}$", "yyyy-MM-dd HH:mm:ss.SSSSS");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}.\\d{6}$", "yyyy-MM-dd HH:mm:ss.SSSSSS");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}.\\d{7}$", "yyyy-MM-dd HH:mm:ss.SSSSSSS");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}.\\d{8}$", "yyyy-MM-dd HH:mm:ss.SSSSSSSS");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}.\\d{9}$", "yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
        put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}$", "yyyy/MM/dd HH:mm:ss");
        put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{1,6}$", "yyyy/MM/dd HH:mm:ss.SSSSSS");
        put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{7,9}$", "yyyy/MM/dd HH:mm:ss.SSSSSSSSS");
        put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd MMM yyyy HH:mm:ss");
        put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd MMMM yyyy HH:mm:ss");
        put("^\\d{1,2}/(0?[1-9]|1[012])/\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd/MM/yyyy HH:mm:ss");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{2}:\\d{2}Z$", "yyyy-MM-dd'T'HH:mm:ss'Z'");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{2}:\\d{2}\\.\\d{1}Z$", "yyyy-MM-dd'T'HH:mm:ss.S'Z'");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{2}:\\d{2}\\.\\d{2}Z$", "yyyy-MM-dd'T'HH:mm:ss.SS'Z'");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{2}:\\d{2}\\.\\d{3}Z$", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        // 4 and 5 precision is provided because formatter with 7 precision will break precision 4 and 5
        // more detail at: https://stackoverflow.com/questions/68411113/text-2021-06-22t182703-5577z-could-not-be-parsed-at-index-20
        put("^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{2}:\\d{2}\\.\\d{4}Z$", "yyyy-MM-dd'T'HH:mm:ss.SSSS'Z'");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{2}:\\d{2}\\.\\d{5}Z$", "yyyy-MM-dd'T'HH:mm:ss.SSSSS'Z'");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{2}:\\d{2}\\.\\d{6,7}Z$", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z'");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{2}:\\d{2}\\.\\d{1,3}[+-]\\d{2}$", "yyyy-MM-dd'T'HH:mm:ss.SSSX");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{2}:\\d{2}\\.\\d{1,3}[+-]\\d{4}$", "yyyy-MM-dd'T'HH:mm:ss.SSSXX");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\s[+-]\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss Z");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\s[+-]\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss. Z");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{1}\\s[+-]\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss.S XXX");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{2}\\s[+-]\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss.SS XXX");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{3}\\s[+-]\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss.SSS XXX");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{4}\\s[+-]\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss.SSSS XXX");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{5}\\s[+-]\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss.SSSSS XXX");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{6}\\s[+-]\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss.SSSSSS XXX");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{7}\\s[+-]\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss.SSSSSSS XXX");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{8}\\s[+-]\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss.SSSSSSSS XXX");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{9}\\s[+-]\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss.SSSSSSSSS XXX");
        put("^([a-zA-Z]{3})\\s([a-zA-Z]{3})\\s(\\d{2})\\s(\\d{2}):(\\d{2}):(\\d{2})\\s(([a-zA-Z]+)|(GMT(\\+|-)\\d{2}:00))\\s(\\d{4})$", "EEE MMM dd HH:mm:ss zzz yyyy");
    }};
}
