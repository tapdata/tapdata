package io.tapdata.common.postman.util;

import io.tapdata.common.postman.entity.ApiMap;
import io.tapdata.common.postman.entity.params.Api;
import io.tapdata.common.support.core.emun.TapApiTag;

import java.util.*;

public class ApiMapUtil {

    public static List<ApiMap.ApiEntity> tableApis(ApiMap apiMap){
        if (Objects.isNull(apiMap) || apiMap.isEmpty()) return Collections.emptyList();
        List<ApiMap.ApiEntity> list = new ArrayList<>();
        apiMap.forEach(entity->{
            if(TapApiTag.isTableName(entity.name())){
                Api requestApi = entity.api();
                requestApi.nameFullDetail(requestApi.name());
                list.add(entity);
            }
        });
        return list;
    }

    public static List<ApiMap.ApiEntity> tokenApis(ApiMap apiMap){
        if (Objects.isNull(apiMap) || apiMap.isEmpty()) return Collections.emptyList();
        List<ApiMap.ApiEntity> list = new ArrayList<>();
        apiMap.forEach(entity->{
            if(TapApiTag.isTokenApi(entity.name())){
                list.add(entity);
            }
        });
        return list;
    }

    public static Object getKeyFromMap(Object mapObj,String keyName){
        if (mapObj instanceof Map){
            int indexPoint = keyName.indexOf(".");
            String nameKey = keyName.substring(0, indexPoint < 0 ? keyName.length() : indexPoint);
            Object value = ((Map<String,Object>)mapObj).get(nameKey);
            if(indexPoint > 0 && indexPoint < keyName.length()) {
                String subKey = keyName.substring(indexPoint + 1);
                return getKeyFromMap(value, subKey);
            }else {
                return value ;
            }
        }else if(mapObj instanceof String){
            return mapObj;
        }
        return mapObj;
    }

    public static Object depthSearchParamFromMap(Object mapObj,String keyName){
        if (Objects.isNull(mapObj) || Objects.isNull(keyName)) return "";
        if (mapObj instanceof Map){
            Map<String,Object> map = (Map<String,Object>)mapObj;
            List<Object> waitMapList = new ArrayList<>();
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (keyName.equals(key)) return value;
                if (value instanceof Map || value instanceof Collection){
                    waitMapList.add(value);
                }
            }
            if (!waitMapList.isEmpty()){
                for (Object mapOrList : waitMapList) {
                    Object keyResult = depthSearchParamFromMap(mapOrList,keyName);
                    if (Objects.nonNull(keyResult)){
                        return keyResult;
                    }
                }
            }
        }else if (mapObj instanceof Collection){
            Collection list = (Collection) mapObj;
            for (Object arr : list) {
                Object keyResult = depthSearchParamFromMap(arr,keyName);
                if (Objects.nonNull(keyResult)){
                    return keyResult;
                }
            }
        }
        return "";
    }

    public static void depthSearchParamFromMap(Object mapObj,String keyName,List<Map.Entry<String,Object>> valueEntry){
        if (Objects.isNull(mapObj) || Objects.isNull(keyName)) return ;
        if (mapObj instanceof Map){
            Map<String,Object> map = (Map<String,Object>)mapObj;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (keyName.equals(key)) {
                    valueEntry.add(new AbstractMap.SimpleEntry<>(key,value));
                }
                if (value instanceof Map || value instanceof Collection)  depthSearchParamFromMap(entry.getValue(),keyName);
            }
        }else if (mapObj instanceof Collection){
            Collection list = (Collection) mapObj;
            for (Object arr : list) {
                depthSearchParamFromMap(arr,keyName);
            }
        }
    }

//    public static boolean isTapTableApi(String apiName) {
//        //String regx = ".*(TAP_TABLE\\[[^\\]]+).*";
////        System.out.println("START[123]JJK[526]".matches(regx));
////        System.out.println(Pattern.compile(regx).matcher("START[123]JJK[526]").group());
//        return Objects.nonNull(apiName) && apiName.matches(TapApiTag.TAP_TABLE.tagRegex());
//    }

//    public static boolean hasPageTag(ApiMap.ApiEntity apiEntity){
//        if (Objects.isNull(apiEntity)) return false;
//        String apiName = apiEntity.name();
//        if (Objects.isNull(apiName)) return false;
//        return apiName.matches(TapApiTag.PAGE_SIZE_PAGE_INDEX.tagRegex())
//                || apiName.matches(TapApiTag.FROM_TO.tagRegex())
//                || apiName.matches(TapApiTag.PAGE_LIMIT.tagRegex())
//                || apiName.matches(TapApiTag.PG_NONE.tagRegex())
//                || apiName.matches(TapApiTag.PAGE_TOKEN.tagRegex());
//    }

    public static void main(String[] args) {
        String table1 = "TAP_TABLE[558";
        String table2 = "TAP_TABLE[10xx]25ads";
        String table3 = "sfrTAP_TABLE[]";
        String table4 = "sderTAP_TABLE[10xx]";
        String table5 = "ADfr";
        System.out.println(table1 + " <-> " + (table1.matches(".*(TAP_TABLE\\[[^\\]]+).*")));
        System.out.println(table2 + " <-> " + (table2.matches(TapApiTag.TAP_TABLE.tagRegex())));
        System.out.println(table3 + " <-> " + (table3.matches(TapApiTag.TAP_TABLE.tagRegex())));
        System.out.println(table4 + " <-> " + (table4.matches(TapApiTag.TAP_TABLE.tagRegex())));
        System.out.println(table5 + " <-> " + (table5.matches(TapApiTag.TAP_TABLE.tagRegex())));

        Map<String,Object> map1= new HashMap<>();
        map1.put("acc","acc");
        map1.put("cvv",new HashMap<String,Object>(){{
            put("bbb","a");
            put("dd","a");
            put("er","asdf");
        }});
        System.out.println(depthSearchParamFromMap(map1,"acsc"));
    }
}
