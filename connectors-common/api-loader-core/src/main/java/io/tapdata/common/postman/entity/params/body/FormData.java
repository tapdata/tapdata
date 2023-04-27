package io.tapdata.common.postman.entity.params.body;

import io.tapdata.common.postman.entity.params.Body;
import io.tapdata.common.postman.enums.PostParam;
import io.tapdata.common.postman.util.ReplaceTagUtil;

import java.util.*;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class FormData extends Body<Map<String, Object>> {
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
    public Body<Map<String, Object>> autoSupplementData(Map<String, Object> bodyMap) {
        Map<String, Object> map = new HashMap<>();
        Optional.ofNullable((List<Map<String, Object>>) bodyMap.get((String) bodyMap.get(PostParam.MODE))).orElse(new ArrayList<>())
                .forEach(ent -> map.put((String) ent.get(PostParam.KEY), ent.get(PostParam.VALUE)));
        return this.raw(map);
    }

    @Override
    public Body<Map<String, Object>> copyOne() {
        FormData binary = new FormData();
        binary.mode(super.mode);
        binary.raw(super.raw);
        binary.options(super.options);
        binary.contentType = super.contentType;
        return binary;
    }

    @Override
    public Body<Map<String, Object>> variableAssignment(Map<String, Object> param) {
        if (Objects.isNull(this.raw) || this.raw.isEmpty()) return this;
        Map<String, Object> jsonObj = new HashMap<>();
        this.raw.entrySet().stream().filter(Objects::nonNull).forEach(ent -> {
            String key = ent.getKey();
            Object valueObj = ent.getValue();
            jsonObj.put(key, Optional.ofNullable(param.get(key)).orElse(valueObj));
            jsonObj.put(key, Optional.ofNullable(param.get(key)).orElse(valueObj));
            if (valueObj instanceof String && ReplaceTagUtil.hasReplace((String) valueObj)){
                jsonObj.put(key, ReplaceTagUtil.replace((String) valueObj,param));
            }
        });
        this.raw.putAll(jsonObj);
        return this;
    }

    @Override
    public String bodyJson(Map<String, Object> appendMap) {
        Map<String, Object> raw = super.raw();
        Map<String, Object> jsonObj = new HashMap<>();
        if (Objects.nonNull(raw) && !raw.isEmpty()) {
            raw.entrySet().stream().filter(Objects::nonNull).forEach(map -> {
                String key = map.getKey();
                if (Objects.isNull(key)) return;
                jsonObj.put(key, Optional.ofNullable(appendMap.get(key)).orElse(map.getValue()));
            });
        }
        return toJson(jsonObj);
    }

    @Override
    public Body<Map<String, Object>> setContentType() {
        this.contentType = "multipart/form-data; boundary=<calculated when request is sent>";
        return this;
    }
}


