package io.tapdata.mongodb.decoder;

import io.tapdata.mongodb.decoder.impl.DynamicDateFilterTime;
import org.bson.Document;
import org.bson.codecs.CustomDocumentDecoder;

public class CustomDocument {

    private CustomDocument(){}
    
    public static Document parse(String json){
        if (null == json || "".equals(json.trim())) {
            return new Document();
        }
        return Document.parse(json, new CustomDocumentDecoder()
                .registerCustomReader(DynamicDateFilterTime.DYNAMIC_DATE, originalValue -> new DynamicDateFilterTime().execute(originalValue, null)));

    }
}
