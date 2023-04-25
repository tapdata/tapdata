package io.tapdata.common.postman.entity.params.body;

import io.tapdata.common.postman.entity.params.Body;
import io.tapdata.common.postman.enums.PostParam;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Binary extends Body<Map<String, Object>> {
    @Override
    public Body<Map<String, Object>> copyOne() {
        Binary binary = new Binary();
        binary.mode(super.mode);
        binary.raw(super.raw);
        binary.options(super.options);
        binary.contentType = super.contentType;
        return binary;
    }

    @Override
    public Body<Map<String, Object>> autoSupplementData(Map<String, Object> bodyMap) {
        return this.raw((Map<String, Object>) Optional.ofNullable(bodyMap.get(String.valueOf(bodyMap.get(PostParam.MODE)))).orElse(new HashMap<>()));
    }

    /**
     *{
     *	"name": "BinaryBody",
     *	"request": {
     *		"method": "POST",
     *		"header": [],
     *		"body": {
     *			"mode": "file",
     *			"file": {
     *				"src": "/D:/GavinData/deskTop/temp/ca5bdaa3-8be7-42c9-b34a-7774ad1c852b.png"
     *          }
     *      },
     *		"url": {
     *			"raw": "{{test_url}}",
     *			"host": [
     *				"{{test_url}}"
     *			]
     *		}
     *	},
     *	"response": []
     *}
     */
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
        this.contentType = "*/*";
        return this;
    }
}

