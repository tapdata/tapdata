package io.tapdata.common.postman.entity.params.body;

import io.tapdata.common.postman.entity.params.Body;

import java.util.Map;

public class NoOne extends Body<Object> {
    @Override
    public Body<Object> autoSupplementData(Map<String, Object> bodyMap) {
        return this;
    }


    @Override
    public Body<Object> copyOne() {
        NoOne binary = new NoOne();
        binary.mode(super.mode);
        binary.raw(super.raw);
        binary.options(super.options);
        binary.contentType = super.contentType;
        return binary;
    }

    /**
     * {
     * 	"name": "NoOneBody",
     * 	"request": {
     * 		"method": "POST",
     * 		"header": [],
     * 		"url": {
     * 			"raw": "{{test_url}}",
     * 			"host": [
     * 				"{{test_url}}"
     * 			]
     *      }
     *  },
     * 	"response": []
     * }
     * */
    @Override
    public Body<Object> variableAssignment(Map<String, Object> param) {
        return this;
    }

    @Override
    public String bodyJson(Map<String, Object> appendMap) {
        return "{}";
    }

    @Override
    public Body<Object> setContentType() {
        this.contentType = "application/json";
        return this;
    }
}

