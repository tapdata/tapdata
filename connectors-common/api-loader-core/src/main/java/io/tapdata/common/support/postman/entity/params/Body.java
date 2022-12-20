package io.tapdata.common.support.postman.entity.params;

import io.tapdata.common.support.postman.enums.PostParam;

import java.util.Map;
import java.util.Objects;

public class Body {
    String mode;
    String raw;
    Map<String,Object> options;
    public static Body create(){
        return new Body();
    }
    public static Body create(Map<String,Object> map){
        try {
            String mode = (String) map.get(PostParam.METHOD);
            String raw = (String) map.get(PostParam.RAW);
            Map<String,Object> options = (Map<String,Object>) map.get(PostParam.OPTIONS);
            return Body.create().mode(mode).raw(raw).options(options);
        }catch (Exception e){}
        return Body.create();
    }
    public String mode(){
        return this.mode;
    }
    public Body mode(String mode){
        this.mode = mode;
        return this;
    }
    public String raw(){
        return this.raw;
    }
    public Body raw(String raw){
        this.raw = raw;
        return this;
    }
    public Map<String,Object> options(){
        return this.options;
    }
    public Body options(Map<String,Object> options){
        this.options = options;
        return this;
    }
    public Body variableAssignment(Map<String,Object> param){
        Body body = Body.create().mode(this.mode).options(this.options);
        String rowBack = Objects.isNull(this.raw)? "" : this.raw;
        if (Objects.nonNull(param) && !param.isEmpty()){
            for (Map.Entry<String, Object> objectEntry : param.entrySet()) {
                String key = objectEntry.getKey();
                Object attributeParamValue = objectEntry.getValue();
                if (attributeParamValue instanceof  Map){
                    attributeParamValue = ((Map<String,Object>)attributeParamValue).get(PostParam.VALUE);
                }
                rowBack = rowBack.replaceAll("\\{\\{"+key+"}}",Objects.isNull(attributeParamValue)?"":String.valueOf(attributeParamValue));
            }
        }
        return body.raw(rowBack);
    }
}
