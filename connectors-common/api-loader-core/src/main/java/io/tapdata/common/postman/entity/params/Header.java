package io.tapdata.common.postman.entity.params;

import io.tapdata.common.postman.enums.PostParam;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Header {
    String key;
    String value;
    String type;
    public Header copyOne(){
        Header header = new Header();
        header.key(key);
        header.value(value);
        header.type(type);
        return header;
    }
    public static Header create(){
        return new Header();
    }
    public static List<Header> create(List<Map<String,Object>> list){
        List<Header> headers = new ArrayList<>();
        try {
            list.stream().filter(Objects::nonNull).forEach(head->headers.add(Header.create(head)));
        }catch (Exception e){}
        return headers;
    }
    public static Header create(Map<String,Object> map){
        try {
            Object keyObj = map.get(PostParam.KEY);
            Object valueObj = map.get(PostParam.VALUE);
            Object typeObj = map.get(PostParam.TYPE);
            String key = Objects.isNull(keyObj)?null : (String) keyObj;
            String value = Objects.isNull(valueObj)?null : (String) valueObj;
            String type = Objects.isNull(typeObj)?null : (String) typeObj;
            return Header.create().key(key).value(value).type(type);
        }catch (Exception e){}
        return new Header();
    }
    public String key(){
        return this.key;
    }
    public Header key(String key){
        this.key = key;
        return this;
    }
    public String value(){
        return this.value;
    }
    public Header value(String value){
        this.value = value;
        return this;
    }
    public String type(){
        return this.type;
    }
    public Header type(String type){
        this.type = type;
        return this;
    }

    public String variableAssignment(String key,String value){
        return Objects.nonNull(this.value)? this.value.replaceAll(key,value) : null;
    }

    public Header variableAssignment(Map<String,Object> param){
        Header header = Header.create().key(this.key).type(type);
        String headValueBack = this.value;
        if (Objects.nonNull(param) && !param.isEmpty()){
            for (Map.Entry<String, Object> paramEntity : param.entrySet()) {
                String key = paramEntity.getKey();
                Object attributeParamValue = paramEntity.getValue();
                if (attributeParamValue instanceof  Map){
                    attributeParamValue = ((Map<String,Object>)attributeParamValue).get(PostParam.VALUE);
                }
                headValueBack = headValueBack.replaceAll("\\{\\{"+key+"}}", Objects.isNull(attributeParamValue) ? "" : String.valueOf(attributeParamValue));
            }
        }
        return header.value(headValueBack);
    }
}
