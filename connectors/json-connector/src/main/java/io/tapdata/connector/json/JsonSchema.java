package io.tapdata.connector.json;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import io.tapdata.common.FileSchema;
import io.tapdata.connector.json.config.JsonConfig;
import io.tapdata.connector.json.util.JsonReaderUtil;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonSchema extends FileSchema {

    private final static String TAG = JsonSchema.class.getSimpleName();

    public JsonSchema(JsonConfig jsonConfig, TapFileStorage storage) {
        super(jsonConfig, storage);
    }

    @Override
    protected void sampleOneFile(Map<String, Object> sampleResult, TapFile tapFile) {
        if ("JSONObject".equals(((JsonConfig) fileConfig).getJsonType())) {
            sampleJsonObjectFile(sampleResult, tapFile);
        } else {
            sampleJsonArrayFile(sampleResult, tapFile);
        }
    }

    private void sampleJsonObjectFile(Map<String, Object> sampleResult, TapFile tapFile) {
        try (
                Reader reader = new InputStreamReader(storage.readFile(tapFile.getPath()), fileConfig.getFileEncoding());
                JsonReader jsonReader = new JsonReader(reader)
        ) {
            if (jsonReader.peek() != JsonToken.BEGIN_OBJECT) {
                throw new IllegalArgumentException(String.format("Json file %s is not json object type", tapFile.getPath()));
            }
            jsonReader.beginObject();
            if (jsonReader.hasNext() && jsonReader.peek() == JsonToken.NAME) {
                putValidIntoMap(sampleResult, "__key", jsonReader.nextName());
                Map<String, Object> dataMap = JsonReaderUtil.traverseMap(jsonReader);
                dataMap.forEach((k, v) -> putValidIntoMap(sampleResult, k, v));
            }
        } catch (Exception e) {
            TapLogger.error(TAG, "read json file error!", e);
        }
    }

    private void sampleJsonArrayFile(Map<String, Object> sampleResult, TapFile tapFile) {
        try (
                Reader reader = new InputStreamReader(storage.readFile(tapFile.getPath()), fileConfig.getFileEncoding());
                JsonReader jsonReader = new JsonReader(reader)
        ) {
            if (jsonReader.peek() != JsonToken.BEGIN_ARRAY) {
                throw new IllegalArgumentException(String.format("Json file %s is not json array type", tapFile.getPath()));
            }
            jsonReader.beginArray();
            if (jsonReader.hasNext() && jsonReader.peek() == JsonToken.BEGIN_OBJECT) {
                Map<String, Object> dataMap = JsonReaderUtil.traverseMap(jsonReader);
                dataMap.forEach((k, v) -> putValidIntoMap(sampleResult, k, v));
            }
        } catch (Exception e) {
            TapLogger.error(TAG, "read json file error!", e);
        }
    }
}
