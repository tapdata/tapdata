package io.tapdata.common.postman.entity.params.body;

import io.tapdata.common.postman.entity.params.Body;

import java.util.Map;

public class GraphQl extends Body<Map<String, Object>> {

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
    public Body<Map<String, Object>> autoSupplementData(Map<String, Object> bodyMap) {
        return this;
    }

    @Override
    public Body<Map<String, Object>> variableAssignment(Map<String, Object> param) {
        return this;
    }

    @Override
    public String bodyJson(Map<String, Object> appendMap) {
        return "{}";
    }

    @Override
    public Body<Map<String, Object>> setContentType() {
        this.contentType = "application/json";
        return this;
    }
}