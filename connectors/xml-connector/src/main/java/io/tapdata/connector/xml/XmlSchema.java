package io.tapdata.connector.xml;

import io.tapdata.common.FileSchema;
import io.tapdata.connector.xml.config.XmlConfig;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import org.dom4j.Document;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.Map;

public class XmlSchema extends FileSchema {

    private final static String TAG = XmlSchema.class.getSimpleName();

    public XmlSchema(XmlConfig xmlConfig, TapFileStorage storage) {
        super(xmlConfig, storage);
    }

    @Override
    protected void sampleOneFile(Map<String, Object> sampleResult, TapFile tapFile) {
        try (
                Reader reader = new InputStreamReader(storage.readFile(tapFile.getPath()))
        ) {
            SAXReader saxReader = new SAXReader();
            Document document = saxReader.read(reader);
            List<Node> dataList = document.selectNodes(((XmlConfig)fileConfig).getXPath());
//            document.selectSingleNode()
//            jsonReader.beginObject();
//            if (jsonReader.hasNext()) {
//                putValidIntoMap(sampleResult, "__key", jsonReader.nextName());
//                DataMap dataMap = jsonParser.fromJsonObject(jsonReader.nextString());
//                dataMap.forEach((k, v) -> putValidIntoMap(sampleResult, k, v));
//            }
        } catch (Exception e) {
            TapLogger.error(TAG, "read xml file error!", e);
        }
    }
}
