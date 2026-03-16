package com.tapdata.tm.worker.dto;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WorkersDeserializer extends JsonDeserializer<Map<String, ApiServerWorkerInfo>> {

    @Override
    public Map<String, ApiServerWorkerInfo> deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException {

        ObjectCodec codec = p.getCodec();
        JsonNode node = codec.readTree(p);

        Map<String, ApiServerWorkerInfo> result = new HashMap<>();

        if (node.isArray()) {
            for (JsonNode item : node) {
                ApiServerWorkerInfo info = codec.treeToValue(item, ApiServerWorkerInfo.class);
                String key = info.getOid();
                result.put(key, info);
            }
        } else if (node.isObject()) {
            for (Map.Entry<String, JsonNode> entry : node.properties()) {
                ApiServerWorkerInfo info =
                        codec.treeToValue(entry.getValue(), ApiServerWorkerInfo.class);
                result.put(entry.getKey(), info);
            }
        }

        return result;
    }
}