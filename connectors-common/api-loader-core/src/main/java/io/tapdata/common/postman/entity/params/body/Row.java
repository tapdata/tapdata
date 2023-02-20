package io.tapdata.common.postman.entity.params.body;

import io.tapdata.common.postman.entity.params.Body;
import io.tapdata.common.postman.enums.PostParam;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.tapdata.entity.simplify.TapSimplify.fromJson;
import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class Row extends Body<String> {
    @Override
    public Body<String> autoSupplementData(Map<String, Object> bodyMap) {
        return this.raw((String) bodyMap.get(PostParam.RAW));
    }

    @Override
    public Body<String> variableAssignment(Map<String, Object> param) {
        String rowBack = Objects.isNull(this.raw) || "".equals(this.raw.trim()) ? "{}" : this.raw;
        if (Objects.nonNull(param) && !param.isEmpty()) {
            for (Map.Entry<String, Object> objectEntry : param.entrySet()) {
                String key = objectEntry.getKey();
                Object attributeParamValue = objectEntry.getValue();
                if (attributeParamValue instanceof Map) {
                    attributeParamValue = ((Map<String, Object>) attributeParamValue).get(PostParam.VALUE);
                }
                if (Objects.nonNull(attributeParamValue) && !"".equals(String.valueOf(attributeParamValue))) {
                    rowBack = rowBack.replaceAll("\\{\\{" + key + "}}", String.valueOf(attributeParamValue));
                }
            }
            this.raw(rowBack);
        }
        return this;
    }

    @Override
    public String bodyJson(Map<String, Object> appendMap) {
        try {
            String raw = super.raw();
            if (Objects.isNull(raw) || "".equals(raw.trim())) return "{}";
            Map<String,Object> bodyMap = (Map<String, Object>) fromJson(raw);
            Map<String,Object> jsonMap = new HashMap<>();
            bodyMap.forEach((key,value)->{
                jsonMap.put(key, Optional.ofNullable(appendMap.get(key)).orElse(value));
            });
            return toJson(jsonMap);
        } catch (Exception e){
            return "";
        }
    }

    @Override
    public Body<String> setContentType() {
        this.contentType = "application/json";
        return this;
    }
}

