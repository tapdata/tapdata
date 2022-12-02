package io.tapdata.connector.json.util;

import com.google.gson.stream.JsonReader;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonReaderUtil {

    public static Object traverseValue(JsonReader jsonReader) throws IOException {
        switch (jsonReader.peek()) {
            case STRING:
                return jsonReader.nextString();
            case BOOLEAN:
                return jsonReader.nextBoolean();
            case NUMBER:
                return new BigDecimal(jsonReader.nextString());
            case NULL:
                return null;
            case BEGIN_ARRAY:
                return traverseArray(jsonReader);
            case BEGIN_OBJECT:
                return traverseMap(jsonReader);
            default:
                throw new IllegalArgumentException("Traverse json file failed");
        }
    }

    public static Map<String, Object> traverseMap(JsonReader jsonReader) throws IOException {
        jsonReader.beginObject();
        Map<String, Object> data = new HashMap<>();
        while (jsonReader.hasNext()) {
            data.put(jsonReader.nextName(), traverseValue(jsonReader));
        }
        jsonReader.endObject();
        return data;
    }

    public static List<Object> traverseArray(JsonReader jsonReader) throws IOException {
        jsonReader.beginArray();
        List<Object> data = new ArrayList<>();
        while (jsonReader.hasNext()) {
            data.add(traverseValue(jsonReader));
        }
        jsonReader.endArray();
        return data;
    }
}
