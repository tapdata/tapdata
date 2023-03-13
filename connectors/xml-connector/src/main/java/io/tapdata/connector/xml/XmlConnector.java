package io.tapdata.connector.xml;


import io.tapdata.common.FileConnector;
import io.tapdata.common.FileOffset;
import io.tapdata.connector.xml.config.XmlConfig;
import io.tapdata.connector.xml.handler.BigSaxDataHandler;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.exception.StopException;
import io.tapdata.file.TapFile;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.dom4j.io.SAXReader;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@TapConnectorClass("spec_xml.json")
public class XmlConnector extends FileConnector {

    private static final String TAG = XmlConnector.class.getSimpleName();

    @Override
    protected void initConnection(TapConnectionContext connectorContext) throws Exception {
        fileConfig = new XmlConfig().load(connectorContext.getConnectionConfig());
        super.initConnection(connectorContext);
    }

    @Override
    protected void readOneFile(FileOffset fileOffset, TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer, AtomicReference<List<TapEvent>> tapEvents) throws Exception {
        long lastModified = storage.getFile(fileOffset.getPath()).getLastModified();
        storage.readFile(fileOffset.getPath(), is -> {
            try (
                    Reader reader = new InputStreamReader(is, fileConfig.getFileEncoding())
            ) {
                SAXReader saxReader = new SAXReader();
                saxReader.setDefaultHandler(new BigSaxDataHandler()
                        .withPath(((XmlConfig) fileConfig).getXPath())
                        .withFlag(this::isAlive)
                        .withLastModified(lastModified)
                        .withConfig(fileOffset, tapTable, eventBatchSize, eventsOffsetConsumer, tapEvents));
                saxReader.read(reader);
            } catch (StopException ignored) {
            } catch (Exception e) {
                TapLogger.error(TAG, "read xml file error!", e);
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
        ConcurrentMap<String, TapFile> xmlFileMap = getFilteredFiles();
        XmlSchema xmlSchema = new XmlSchema((XmlConfig) fileConfig, storage);
        Map<String, Object> sample = xmlSchema.sampleEveryFileData(xmlFileMap);
        if (EmptyKit.isEmpty(sample)) {
            throw new RuntimeException("Load schema from xml files error: no headers and contents!");
        }
        makeTapTable(tapTable, sample, fileConfig.getJustString());
        consumer.accept(Collections.singletonList(tapTable));
        storage.destroy();
    }
}
