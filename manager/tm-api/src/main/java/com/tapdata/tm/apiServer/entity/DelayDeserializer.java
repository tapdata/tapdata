package com.tapdata.tm.apiServer.entity;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DelayDeserializer extends JsonDeserializer<List<Map<String, Number>>> {
    private static final Logger logger = LoggerFactory.getLogger(DelayDeserializer.class);

    @Override
    public List<Map<String, Number>> deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException {
        ObjectCodec codec = p.getCodec();
        JsonNode node = codec.readTree(p);
        List<Map<String, Number>> result = new ArrayList<>();
        if (node.isArray()) {
            for (JsonNode item : node) {
                result.add(parseItem(item, codec));
            }
        } else if (node.isObject()) {
            result.add(parseItem(node, codec));
        }
        return result;
    }

    Map<String, Number> parseItem(JsonNode node, ObjectCodec codec) {
        Map<String, Number> item = new HashMap<>();
        if (node.isNumber()) {
            item.put(node.asText(), 1);
        } else if (node.isObject()) {
            for (Map.Entry<String, JsonNode> entry : node.properties()) {
                try {
                    Number val = codec.treeToValue(entry.getValue(), Number.class);
                    item.put(entry.getKey(), val);
                } catch (Exception e) {
                    logger.debug("Unable parse value as a delay number: {}", e.getMessage(), e);
                }
            }
        }
        return item;
    }
}