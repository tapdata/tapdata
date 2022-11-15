import io.tapdata.connector.xml.handler.BigSaxSchemaHandler;
import io.tapdata.exception.StopException;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.util.HashMap;

public class Main {
    public static void main(String[] args) {
        HashMap<String, Object> map = new HashMap<>();
        try (
                Reader reader = new InputStreamReader(Files.newInputStream(new File("/Users/jarad/Desktop/pom.xml").toPath()))
        ) {
            SAXReader saxReader = new SAXReader();
            saxReader.setDefaultHandler(new BigSaxSchemaHandler()
                    .withPath("/project/dependencies/dependency")
                    .withSampleResult(map));
            saxReader.read(reader);

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
        } catch (Exception e) {
            if (!(e.getCause() instanceof StopException)) {
                e.printStackTrace();
            }
        }
        map.forEach((k, v) -> System.out.println(k + ":" + v.toString()));
    }
}
