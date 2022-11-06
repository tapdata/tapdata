package io.tapdata.connector.json;

import com.google.gson.stream.JsonReader;
import io.tapdata.common.FileSchema;
import io.tapdata.connector.json.config.JsonConfig;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;

public class JsonSchema extends FileSchema {

    private final static String TAG = JsonSchema.class.getSimpleName();
    private static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class); //json util

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
                Reader reader = new InputStreamReader(storage.readFile(tapFile.getPath()));
                JsonReader jsonReader = new JsonReader(reader)
        ) {
            jsonReader.beginObject();
            if (jsonReader.hasNext()) {
                putValidIntoMap(sampleResult, "__key", jsonReader.nextName());
                DataMap dataMap = jsonParser.fromJsonObject(jsonReader.nextString());
                dataMap.forEach((k, v) -> putValidIntoMap(sampleResult, k, v));
            }
        } catch (Exception e) {
            TapLogger.error(TAG, "read json file error!", e);
        }
    }

    private void sampleJsonArrayFile(Map<String, Object> sampleResult, TapFile tapFile) {
        try (
                Reader reader = new InputStreamReader(storage.readFile(tapFile.getPath()));
                JsonReader jsonReader = new JsonReader(reader)
        ) {
            jsonReader.beginArray();
            if (jsonReader.hasNext()) {
                DataMap dataMap = jsonParser.fromJsonObject(jsonReader.nextString());
                dataMap.forEach((k, v) -> putValidIntoMap(sampleResult, k, v));
            }
        } catch (Exception e) {
            TapLogger.error(TAG, "read json file error!", e);
        }
    }
}
