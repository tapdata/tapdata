package io.tapdata.api.postman;

import cn.hutool.json.JSONUtil;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class Analysis {
    private String packageName = "io.tapdata.coding.postman.auto";
    public static void main(String[] args) {
        Analysis analysis = new Analysis();
        analysis.builder();
    }
    public void builder(){
        //Map<String, Object> json = PostMain.json();
        Map<String,Object> json = JSONUtil.readJSONObject(new File("D:\\GavinData\\InstallPackage\\飞书\\data\\data\\Feishu_1658300789.zh_CN.postman_collection.json"), StandardCharsets.UTF_8);


        List<Object> variable = this.getList(PostParam.VARIABLE,json);

        Map<String,Object> info = this.getMap(PostParam.INFO,json);

        List<Object> list = this.getList(PostParam.ITEM,json);
        Map<String, List<Object>> listMap = item(list);

        List<Object> event = this.getList(PostParam.EVENT,json);

        String variableClass = this.variable(variable);
    }

    Map<String, Object> getMap(String key,Map<String,Object> collection){
        Object infoObj = collection.get(key);
        return (null != infoObj && infoObj instanceof Map)?((Map<String,Object>)infoObj):null;
    }
    List<Object> getList(String key, Map<String,Object> collection){
        Object infoObj = collection.get(key);
        return (null != infoObj && infoObj instanceof Collection)?(List<Object>)infoObj:null;
    }

    String variable(List<Object> variables){
        StringBuilder builder = new StringBuilder();
        builder.append("package ").append(packageName).append(";\n");
        //builder.append("").append(";\n");
        builder.append("public enum Variable{\n");

        if (null != variables && !variables.isEmpty()) {
            StringJoiner joiner = new StringJoiner(",\n");
            for (Object variable : variables) {
                if (null != variable && variable instanceof Map) {
                    Map<String,Object> var = (Map<String, Object>)variable;
                    Object key = var.get(PostParam.KEY);
                    Object value = var.get(PostParam.VALUE);
                    joiner.add(String.format("\t%s(\"%s\",\"%s\")",String.valueOf(key).toUpperCase(),key,value));
                }
            }
            builder.append(joiner.toString());
        }
        builder.append("\t;\n");
        builder.append("\tString keyName;\n");
        builder.append("\tString keyValue;\n");
        builder.append("\tVariable(String key,String value){\n");
        builder.append("\t\tthis.keyName = key;\n");
        builder.append("\t\tthis.keyValue = value;\n");
        builder.append("\t}\n");
        builder.append("\tpublic String keyValue(){\n");
        builder.append("\t\treturn this.keyValue;\n");
        builder.append("\t}\n");
        builder.append("\tpublic String keyName(){\n");
        builder.append("\t\treturn this.keyName;\n");
        builder.append("\t}\n");
        builder.append("}");
        return builder.toString();
    }

    Map<String,List<Object>> item(List<Object> item){
        if (null == item) return null;
        List<Object> list = new ArrayList<>();
        put(list,item);

        Map<String, List<Object>> listMap = list.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(obj -> {
            Map<String, Object> request = (Map<String, Object>) ((Map<String, Object>) obj).get(PostParam.REQUEST);
            Object urlObj = request.get(PostParam.URL);
            String url = "UN_KNOW_URL";
            if (null != urlObj){
                if (urlObj instanceof String){
                    url = (String) urlObj;
                }else if (urlObj instanceof Map){
                    url = String.valueOf(((Map<String,Object>) urlObj).get(PostParam.RAW));
                }
            }
            return ReplaceTagUtil.replace(url);
            //String id = String.valueOf(((Map<String, Object>) obj).get(PostParam.ID));
            //int lastIndexOf = id.lastIndexOf(".");
            //return id.substring(0, lastIndexOf);
        }));
        return listMap;
    }
    void put(List<Object> list,Object obj){
        if (null == obj) return;
        if (obj instanceof Map){
            Map<String,Object> map = (Map<String, Object>) obj;
            if (isMapFromItem(map)){
                list.add(map);
            }else{
                map.forEach( (key,value) -> put(list,value) );
            }
        }else if (obj instanceof Collection){
            Collection<Object> list1 = (Collection<Object>) obj;
            for (Object it : list1) {
                if (null == it) continue;
                put(list,it);
            }
        }
    }
    boolean isMapFromItem(Object mapObj){
        if (null != mapObj && mapObj instanceof Map){
            Map<String,Object> map = (Map<String, Object>) mapObj;
            Object request = map.get(PostParam.REQUEST);
            return null != request;
        }
        return false;
    }

    void api(List<Object> apiList,String idGroup){
        if (null == apiList || apiList.isEmpty()) return;
        StringBuilder builder = new StringBuilder();
        int lastIndexOf = idGroup.lastIndexOf(".");
        String packageName = idGroup.substring(lastIndexOf + 1);
        builder.append("package ").append(packageName).append(";\n");

        builder.append("public class ").append(packageName).append("{\n");
        for (Object api : apiList) {
            if (api instanceof Map){
                Map<String,Object> map = (Map<String, Object>) api;

            }
        }
        builder.append("}");
    }
    void builderFunction(Map<String,Object> api){

    }
}
