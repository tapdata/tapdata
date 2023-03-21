package io.tapdata.common.postman.entity.params.body;

import io.tapdata.common.postman.entity.params.Body;
import io.tapdata.common.postman.enums.PostParam;

import java.util.*;

import static io.tapdata.entity.simplify.TapSimplify.fromJson;
import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class GraphQl extends Body<String> {
    private Set<String> keys = new HashSet<>();

    /**
     * {
     * "name": "GraphQlBody",
     * "request": {
     * "method": "POST",
     * "header": [],
     * "body": {
     * "mode": "graphql",
     * "graphql": {
     * "query": "sdasda",
     * "variables": "sasas"
     * }* 						},
     * "url": {
     * "raw": "{{test_url}}",
     * "host": [
     * "{{test_url}}"
     * ]
     * }
     * },
     * "response": []
     * }
     */

    @Override
    public Body<String> autoSupplementData(Map<String, Object> bodyMap) {
        try {
            Object o = bodyMap.get(PostParam.GRAPHQL);
            if (o instanceof Map) {
                Map<String,Object> config = (Map<String, Object>) o;
                Object o1 = config.get(PostParam.VARIABLES);
                if ( Objects.isNull(o1) || "".equals(String.valueOf(o1).trim())){
                    config.put(PostParam.VARIABLES,"{}");
                }
                return this.raw(toJson(config));
            }else {
                return this.raw("{\"query\":\"\",\"variables\":{}}");
            }
        }catch (Exception e){
            return this.raw("{\"query\":\"\",\"variables\":{}}");
        }
    }


    @Override
    public Body<String> copyOne() {
        GraphQl binary = new GraphQl();
        binary.mode(super.mode);
        binary.raw(super.raw);
        binary.options(super.options);
        binary.contentType = super.contentType;
        return binary;
    }

    @Override
    public Body<String> variableAssignment(Map<String, Object> param) {
        String rowBack = Objects.isNull(this.raw) || "".equals(this.raw.trim()) ? "" : this.raw;
        if (Objects.nonNull(param) && !param.isEmpty()) {
            for (Map.Entry<String, Object> objectEntry : param.entrySet()) {
                String key = objectEntry.getKey();
                Object attributeParamValue = objectEntry.getValue();
                if (attributeParamValue instanceof Map) {
                    attributeParamValue = ((Map<String, Object>) attributeParamValue).get(PostParam.VALUE);
                }
                if (Objects.isNull(attributeParamValue)) {
                    attributeParamValue = "null";
                }
                String regex = "\\{\\{" + key + "}}";
                if (rowBack.contains("{{" + key + "}}")) {
                    String value = attributeParamValue instanceof String ? (String) attributeParamValue : toJson(attributeParamValue);
                    rowBack = rowBack.replaceAll(regex, value);
                    keys.add(key);
                }
            }
            this.raw(rowBack);
        }
        return this;
    }

    @Override
    public String bodyJson(Map<String, Object> appendMap) {
        Body<String> stringBody = this.variableAssignment(appendMap);
        return stringBody.raw();
    }

    @Override
    public Body<String> setContentType() {
        this.contentType = "application/json";
        return this;
    }
}