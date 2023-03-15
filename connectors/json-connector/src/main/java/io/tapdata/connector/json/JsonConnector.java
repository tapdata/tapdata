package io.tapdata.connector.json;

import com.google.gson.stream.JsonReader;
import io.tapdata.common.FileConnector;
import io.tapdata.common.FileOffset;
import io.tapdata.connector.json.config.JsonConfig;
import io.tapdata.connector.json.util.JsonReaderUtil;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.file.TapFile;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.util.JsonSchemaParser;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@TapConnectorClass("spec_json.json")
public class JsonConnector extends FileConnector {

    protected final static JsonSchemaParser SCHEMA_PARSER = new JsonSchemaParser();

    @Override
    protected void initConnection(TapConnectionContext connectorContext) throws Exception {
        fileConfig = new JsonConfig().load(connectorContext.getConnectionConfig());
        super.initConnection(connectorContext);
    }

    @Override
    protected void readOneFile(FileOffset fileOffset, TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer, AtomicReference<List<TapEvent>> tapEvents) throws Exception {
        if ("JSONObject".equals(((JsonConfig) fileConfig).getJsonType())) {
            readJsonObjectFile(fileOffset, tapTable, eventBatchSize, eventsOffsetConsumer, tapEvents);
        } else {
            readJsonArrayFile(fileOffset, tapTable, eventBatchSize, eventsOffsetConsumer, tapEvents);
        }
    }

    private void readJsonObjectFile(FileOffset fileOffset, TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer, AtomicReference<List<TapEvent>> tapEvents) throws Exception {
        long lastModified = storage.getFile(fileOffset.getPath()).getLastModified();
        storage.readFile(fileOffset.getPath(), is -> {
            try (
                    Reader reader = new InputStreamReader(is, fileConfig.getFileEncoding());
                    JsonReader jsonReader = new JsonReader(reader)
            ) {
                jsonReader.beginObject();
                while (isAlive() && jsonReader.hasNext()) {
                    String __key = jsonReader.nextName();
                    Map<String, Object> dataMap = JsonReaderUtil.traverseMap(jsonReader);
                    dataMap.put("__key", __key);
                    tapEvents.get().add(insertRecordEvent(dataMap, tapTable.getId()).referenceTime(lastModified));
                    if (tapEvents.get().size() == eventBatchSize) {
                        fileOffset.setDataLine(fileOffset.getDataLine() + eventBatchSize);
                        fileOffset.setPath(fileOffset.getPath());
                        eventsOffsetConsumer.accept(tapEvents.get(), fileOffset);
                        tapEvents.set(list());
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void readJsonArrayFile(FileOffset fileOffset, TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer, AtomicReference<List<TapEvent>> tapEvents) throws Exception {
        long lastModified = storage.getFile(fileOffset.getPath()).getLastModified();
        storage.readFile(fileOffset.getPath(), is -> {
            try (
                    Reader reader = new InputStreamReader(is, fileConfig.getFileEncoding());
                    JsonReader jsonReader = new JsonReader(reader)
            ) {
                jsonReader.beginArray();
                while (isAlive() && jsonReader.hasNext()) {
                    tapEvents.get().add(insertRecordEvent(JsonReaderUtil.traverseMap(jsonReader), tapTable.getId()).referenceTime(lastModified));
                    if (tapEvents.get().size() == eventBatchSize) {
                        fileOffset.setDataLine(fileOffset.getDataLine() + eventBatchSize);
                        fileOffset.setPath(fileOffset.getPath());
                        eventsOffsetConsumer.accept(tapEvents.get(), fileOffset);
                        tapEvents.set(list());
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        codecRegistry.registerFromTapValue(TapRawValue.class, "STRING", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return tapRawValue.getValue().toString();
            return "null";
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss"));
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS"));
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd"));

        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        initConnection(connectionContext);
        if (EmptyKit.isBlank(fileConfig.getModelName())) {
            return;
        }
        TapTable tapTable = table(fileConfig.getModelName());
        ConcurrentMap<String, TapFile> jsonFileMap = getFilteredFiles();
        JsonSchema jsonSchema = new JsonSchema((JsonConfig) fileConfig, storage);
        Map<String, Object> sample = jsonSchema.sampleEveryFileData(jsonFileMap);
        if (EmptyKit.isEmpty(sample)) {
            throw new RuntimeException("Load schema from json files error: no contents found!");
        }
        SCHEMA_PARSER.parse(tapTable, sample);
        consumer.accept(Collections.singletonList(tapTable));
        storage.destroy();
    }

}
