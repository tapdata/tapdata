package io.tapdata.common.postman.entity.params.body;

import io.tapdata.common.postman.entity.params.Body;
import io.tapdata.common.postman.enums.PostParam;
import io.tapdata.common.postman.util.ReplaceTagUtil;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class FormUrlEncoded extends Body<Map<String, Object>> {
    @Override
    public Body<Map<String, Object>> autoSupplementData(Map<String, Object> bodyMap) {
        Map<String, Object> map = new HashMap<>();
        Optional.ofNullable((List<Map<String, Object>>) bodyMap.get((String) bodyMap.get(PostParam.MODE))).orElse(new ArrayList<>())
                .forEach(ent -> map.put((String) ent.get(PostParam.KEY), ent.get(PostParam.VALUE)));
        return this.raw(map);
    }

    @Override
    public Body<Map<String, Object>> copyOne() {
        FormUrlEncoded binary = new FormUrlEncoded();
        binary.mode(super.mode);
        binary.raw(super.raw);
        binary.options(super.options);
        binary.contentType = setContentType().contentType();
        return binary;
    }

    @Override
    public Body<Map<String, Object>> variableAssignment(Map<String, Object> param) {
        if (Objects.isNull(this.raw) || this.raw.isEmpty()) return this;
        Map<String, Object> jsonObj = new HashMap<>();
        this.raw.entrySet().stream().filter(Objects::nonNull).forEach(ent -> {
            String key = ent.getKey();
            Object valueObj = ent.getValue();
            jsonObj.put(key, null == valueObj ? (null == param.get(key) || "".equals(param.get(key)) ? param.get(key) : valueObj ) : valueObj);
            if (valueObj instanceof String && ReplaceTagUtil.hasReplace((String) valueObj)) {
                jsonObj.put(key, ReplaceTagUtil.replace((String) valueObj, param));
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
        StringJoiner joiner = new StringJoiner("&");
        jsonObj.forEach((k,v)->{
            joiner.add(k + "=" + new String(String.valueOf(v).getBytes(StandardCharsets.UTF_8),StandardCharsets.UTF_8));
        });
        return joiner.toString();
    }

    @Override
    public Body<Map<String, Object>> setContentType() {
        this.contentType = "application/x-www-form-urlencoded";
        return this;
    }
}