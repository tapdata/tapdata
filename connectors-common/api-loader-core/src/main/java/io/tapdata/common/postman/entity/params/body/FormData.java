package io.tapdata.common.postman.entity.params.body;

import io.tapdata.common.postman.entity.params.Body;
import io.tapdata.common.postman.enums.PostParam;
import io.tapdata.common.postman.util.ReplaceTagUtil;

import java.util.*;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class FormData extends Body<List<Map<String, Object>>> {
    /**
     * {
     * "name": "FormDataBody",
     * "request": {
     * "method": "POST",
     * "header": [],
     * "body": {
     * "mode": "formdata",
     * "formdata": [
     * {
     * "key": "name",
     * "value": "aaa",
     * "type": "default"
     * }
     * ]
     * },
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
    public Body<List<Map<String, Object>>> autoSupplementData(Map<String, Object> bodyMap) {
        return this.raw(Optional.ofNullable((List<Map<String, Object>>) bodyMap.get((String) bodyMap.get(PostParam.METHOD))).orElse(new ArrayList<>()));
    }

    @Override
    public Body<List<Map<String, Object>>> variableAssignment(Map<String, Object> param) {
        if (Objects.isNull(this.raw) || this.raw.isEmpty()) return this;
        Map<String, Object> jsonObj = new HashMap<>();
        this.raw.stream().filter(Objects::nonNull).forEach(map -> {
            Object key = map.get(PostParam.KEY);
            if (Objects.isNull(key)) return;
            String keyStr = String.valueOf(key);
            Object valueObj = map.get(keyStr);
            jsonObj.put(keyStr, Optional.ofNullable(param.get(keyStr)).orElse(valueObj));
            if (valueObj instanceof String && ReplaceTagUtil.hasReplace((String) valueObj)){
                jsonObj.put(keyStr, ReplaceTagUtil.replace((String) valueObj,param));
            }
        });
        return this;
    }

    @Override
    public String bodyJson(Map<String, Object> appendMap) {
        List<Map<String, Object>> raw = super.raw();
        Map<String, Object> jsonObj = new HashMap<>();
        if (Objects.nonNull(raw) && !raw.isEmpty()) {
            raw.stream().filter(Objects::nonNull).forEach(map -> {
                Object key = map.get(PostParam.KEY);
                if (Objects.isNull(key)) return;
                String keyStr = String.valueOf(key);
                jsonObj.put(keyStr, Optional.ofNullable(appendMap.get(keyStr)).orElse(map.get(keyStr)));
            });
        }
        return toJson(jsonObj);
    }

    @Override
    public Body<List<Map<String, Object>>> setContentType() {
        this.contentType = "multipart/form-data; boundary=<calculated when request is sent>";
        return this;
    }
}


