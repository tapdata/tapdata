package io.tapdata.connector.xml;

import io.tapdata.common.FileSchema;
import io.tapdata.connector.xml.config.XmlConfig;
import io.tapdata.connector.xml.handler.BigSaxSchemaHandler;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.exception.StopException;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import org.dom4j.io.SAXReader;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;

public class XmlSchema extends FileSchema {

    private final static String TAG = XmlSchema.class.getSimpleName();

    public XmlSchema(XmlConfig xmlConfig, TapFileStorage storage) {
        super(xmlConfig, storage);
    }

    @Override
    protected void sampleOneFile(Map<String, Object> sampleResult, TapFile tapFile) throws Exception {
        storage.readFile(tapFile.getPath(), is -> {
            try (
                    Reader reader = new InputStreamReader(is, fileConfig.getFileEncoding())
            ) {
                SAXReader saxReader = new SAXReader();
                saxReader.setDefaultHandler(new BigSaxSchemaHandler()
                        .withPath(((XmlConfig) fileConfig).getXPath())
                        .withSampleResult(sampleResult));
                saxReader.read(reader);
            } catch (StopException ignored) {
            } catch (Exception e) {
                TapLogger.error(TAG, "read xml file error!", e);
            }
        });
    }
}
