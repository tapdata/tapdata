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
    boolean disabled;
    public Header copyOne(){
        Header header = new Header();
        header.key(key);
        header.value(value);
        header.type(type);
        header.disabled(disabled);
        return header;
    }
    public static Header create(){
        return new Header();
    }
    public static List<Header> create(List<Map<String,Object>> list){
        List<Header> headers = new ArrayList<>();
        try {
            list.stream().filter(Objects::nonNull).forEach(head -> {
                Header header = Header.create(head);
                if (null != header){
                    headers.add(header);
                }
            });
        }catch (Exception e){}
        return headers;
    }
    public static Header create(Map<String,Object> map){
        try {
            Object keyObj = map.get(PostParam.KEY);
            String key = Objects.isNull(keyObj)? null : (String) keyObj;
            if (null == key || "".equals(key.trim())) return null;
            Object valueObj = map.get(PostParam.VALUE);
            Object typeObj = map.get(PostParam.TYPE);
            Object disabledObj = map.get(PostParam.DISABLED);
            boolean disabled = Objects.nonNull(disabledObj) && (disabledObj instanceof Boolean ? (Boolean) disabledObj : "true".equals(String.valueOf(disabledObj)));
            String value = Objects.isNull(valueObj)?null : (String) valueObj;
            String type = Objects.isNull(typeObj)?null : (String) typeObj;
            return Header.create().key(key).value(value).type(type).disabled(disabled);
        }catch (Exception e){}
        return null;
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
    public boolean disabled(){
        return this.disabled;
    }
    public Header disabled(boolean disabled){
        this.disabled = disabled;
        return this;
    }

    public String variableAssignment(String key,String value){
        return Objects.nonNull(this.value)? this.value.replaceAll(key,value) : null;
    }

    public Header variableAssignment(Map<String,Object> param){
        Header header = Header.create().key(this.key).type(type).disabled(disabled);
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

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }
}
