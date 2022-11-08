import io.tapdata.connector.xml.handler.BigSaxHandler;
import org.dom4j.Document;
import org.dom4j.Node;
import org.dom4j.XPath;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {
        try (
                Reader reader = new InputStreamReader(Files.newInputStream(new File("/Users/jarad/Desktop/pom.xml").toPath()))
        ) {
            SAXReader saxReader = new SAXReader();
            saxReader.setDefaultHandler(new BigSaxHandler("/project/dependencies/dependency"));
            Document document = saxReader.read(reader);
            document.getText();
//            Map<String, String> map = new HashMap<>();
//            String nsURI = document.getRootElement().getNamespaceURI();
//            map.put("xmlns", nsURI);
//            XPath x = document.createXPath("//xmlns:project/xmlns:dependencies");
//            x.setNamespaceURIs(map);
//            List<Node> a = x.selectNodes(document);
//            a.forEach(node -> {
//                System.out.println(node.asXML());
//                System.out.println(node.getName());
//                System.out.println(node.getText());
//                System.out.println(node.getStringValue());
//            });
//            System.out.println("");
//            document.selectSingleNode()
//            jsonReader.beginObject();
//            if (jsonReader.hasNext()) {
//                putValidIntoMap(sampleResult, "__key", jsonReader.nextName());
//                DataMap dataMap = jsonParser.fromJsonObject(jsonReader.nextString());
//                dataMap.forEach((k, v) -> putValidIntoMap(sampleResult, k, v));
//            }
        }
    }
}
