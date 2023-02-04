package io.tapdata.common.postman.util;


import io.tapdata.common.postman.enums.PostParam;
import io.tapdata.common.postman.enums.Symbol;
import io.tapdata.common.support.core.emun.TapApiTag;

import java.util.*;

public class ReplaceTagUtil {
    public static String replaceToEmpty(String itemStr){
        return itemStr.replaceAll(Symbol.tags(),"");
    }
    public static String replace(String itemStr){
        if (null == itemStr ) return null;
        Symbol[] values = Symbol.values();
        if (values.length <= 0) return itemStr;
        for (Symbol value : values) {
            itemStr = itemStr.replaceAll(value.tag(),value.to());
        }
        return itemStr;
    }

    public static void main(String[] args) {
//        String regx = ".*(JJK\\[[^\\]]+).*";
//        System.out.println("START[123]JJK[526]".matches(regx));
//        System.out.println(Pattern.compile(regx).matcher("START[123]JJK[526]").group());

//        String expireStatus = "body.message=NO AUTH&&body.code=500021";
//        String[] propertiesArr = expireStatus.split("\\|\\||&&");
//        for (String s : propertiesArr) {
//            System.out.println(s);
//        }
        String url = "{{apiUrl}}/crm/v3/Deals?layout_id=184323000000064005&fields=Deal_Name,Owner,Account_Name,Amount&per_page=200&page=1";
        List<Map<String, Object>> query = new ArrayList<>();
        Map<String,Object> map = new HashMap<>();
        map.put("key","page");
        map.put("value",22);
        map.put("description","TAP_PAGE_INDEX");
        query.add(map);
        Map<String,Object> bodyMap = new HashMap<>();
        Map<String,Object> params = new HashMap<>();
        params.put("page",223);
        System.out.println(replaceUrlParams(query,bodyMap,params,url));
    }

    public static String replaceUrlParams(List<Map<String, Object>> query,Map<String,Object> bodyMap,Map<String,Object> params,String agoUrl){
        for (Map<String, Object> queryMap : query) {
            String key = String.valueOf(queryMap.get(PostParam.KEY));
            //Object value = queryMap.get(PostParam.VALUE);
            String desc = String.valueOf(queryMap.get(PostParam.DESCRIPTION));
            if(TapApiTag.isTapPageParam(desc)){
                Object value = params.get(key);
                if(Objects.nonNull(value)){
                    queryMap.put(PostParam.VALUE,value);
                    bodyMap.put(key,value);
                    String keyParam = "?" + key + "=";
                    if (!agoUrl.contains(keyParam)){
                        keyParam = "&" + key + "=";
                    }
                    if(agoUrl.contains(keyParam)){
                        int indexOf = agoUrl.indexOf(keyParam);
                        if(indexOf<0) indexOf = agoUrl.indexOf(keyParam);
                        int indexOfEnd = agoUrl.indexOf("&", indexOf+1);
                        String keyValueAgo = agoUrl.substring(indexOf,indexOfEnd < 0 ? agoUrl.length():indexOfEnd);
                        agoUrl = agoUrl.replaceAll(keyValueAgo.replace("?","\\?"),keyParam+value);
                    }

                }
            }
        }
        return agoUrl;
    }
}
