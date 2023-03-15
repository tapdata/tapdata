package io.tapdata.common.postman.entity.params;

import io.tapdata.common.postman.enums.PostParam;

import java.util.*;
import java.util.stream.Collectors;

public class Url {
    String raw;
    List<String> host;
    List<String> path;
    List<Map<String,Object>> query;
    List<Map<String,Object>> variable;
    public Url copyOne(){
        Url url = new Url();
        url.raw(this.raw);
        url.host(this.host);
        url.path(this.path);
        url.query(this.query);
        url.variable(this.variable);
        return url;
    }

    public static Url create(){
        return new Url();
    }
    public static Url create(String url){
        return Url.create().raw(url);
    }
    public static Url create(Map<String,Object> map){
        try {
            if (Objects.isNull(map)) return Url.create();
            Object rawObj = map.get(PostParam.RAW);
            Object hostObj = map.get(PostParam.HOST);
            Object pathObj = map.get(PostParam.PATH);
            Object queryObj = map.get(PostParam.QUERY);
            Object variableObj = map.get(PostParam.VARIABLE);

            String raw = Objects.isNull(rawObj) ? null : (String) rawObj;
            List<String> host = Objects.isNull(hostObj) ? null : (List<String>) hostObj;
            List<String> path = Objects.isNull(pathObj) ? null : (List<String>) pathObj;
            List<Map<String,Object>> query = Objects.isNull(queryObj) ? null : (List<Map<String,Object>>) queryObj;
            List<Map<String,Object>> variable = Objects.isNull(variableObj) ? null : (List<Map<String,Object>>) variableObj;
            return Url.create().raw(raw).host(host).path(path).query(query).variable(variable);
        }catch (Exception e){
            return Url.create();
        }
    }
    public static Url create(Object urlObj){
        if ( urlObj instanceof String) {
            return Url.create((String) urlObj);
        }else if(urlObj instanceof Map){
            Map<String,Object> map = (Map<String, Object>) urlObj;
            return Url.create(map);
        }
        return Url.create();
    }
    public String raw(){
        return this.raw;
    }
    public Url raw(String raw){
        this.raw = raw;
        return this;
    }
    public List<String> host(){
        return this.host;
    }
    public Url host(List<String> host){
        this.host = host;
        return this;
    }
    public List<String> path(){
        return this.path;
    }
    public Url path(List<String> path){
        this.path = path;
        return this;
    }
    public List<Map<String,Object>> query(){
        return this.query;
    }
    public Url query(List<Map<String,Object>> query){
        this.query = query;
        return this;
    }
    public List<Map<String,Object>> variable(){
        return this.variable;
    }
    public Url variable(List<Map<String,Object>> variable){
        this.variable = variable;
        return this;
    }

    public Url variableAssignment(Map<String, Object> params) {
        final String[] rawBack = {this.raw};
        List<Map<String,Object>> queryBack = new ArrayList<>();
        Map<String, Map<String, Object>> varMap = Objects.isNull(this.variable) ? new HashMap<>(): this.variable.stream().collect(Collectors.toMap(var -> String.valueOf(var.get(PostParam.KEY)), var -> var, (v1, v2) -> v2));
        Map<String,Map<String,Object>> queryMap = Objects.isNull(this.query) ? new HashMap<>(): this.query.stream().collect(Collectors.toMap(var -> String.valueOf(var.get(PostParam.KEY)), var -> var, (v1, v2) -> v2));
        varMap.forEach((var,varValue)->{
            Object attributeParamValue = Objects.isNull(params)? null: params.get(var);
            if (attributeParamValue instanceof  Map){
                attributeParamValue = ((Map<String,Object>)attributeParamValue).get(PostParam.VALUE);
            }
            Object valueObj = varValue.get(PostParam.VALUE);
            String val = Objects.isNull(valueObj)? "": String.valueOf(valueObj);
            if (Objects.nonNull(attributeParamValue) ){
                val = String.valueOf(attributeParamValue);
            }
            rawBack[0] = rawBack[0].replaceAll(":"+var,val);
        });
        queryMap.forEach((var,varValue)->{
            Object attributeParamValue = Objects.isNull(params)? null: params.get(var);
            if (attributeParamValue instanceof  Map){
                attributeParamValue = ((Map<String,Object>)attributeParamValue).get(PostParam.VALUE);
            }
            Object valueObj = varValue.get(PostParam.VALUE);
            String val = Objects.isNull(valueObj)? "": String.valueOf(valueObj);
            if (Objects.nonNull(attributeParamValue)){
                val = String.valueOf(attributeParamValue);
            }
            Map<String,Object> backMap = new HashMap<>();
            backMap.put(PostParam.KEY,var);
            backMap.put(PostParam.VALUE,val);
            backMap.put(PostParam.DESCRIPTION,varValue.get(PostParam.DESCRIPTION));
            queryBack.add(backMap);
        });
        if (Objects.nonNull(params) && !params.isEmpty()) {
            for (Map.Entry<String,Object> varEntry : params.entrySet()) {
                Object value = varEntry.getValue();
                if (value instanceof  Map){
                    value = ((Map<String,Object>)value).get(PostParam.VALUE);
                }
                rawBack[0] = rawBack[0].replaceAll("\\{\\{"+varEntry.getKey()+"}}",Objects.isNull(value)?"":String.valueOf(value));
            }
        }
        return Url.create().query(queryBack).variable(this.variable).raw(rawBack[0]).host(this.host).path(this.path);
    }
}
