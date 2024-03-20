package io.tapdata.mongodb.decoder;

import io.tapdata.mongodb.decoder.impl.AutoUpdateDateFilterTime;
import org.bson.Document;
import org.bson.codecs.CustomDocumentDecoder;

public class CustomDocument {
    private CustomDocument(){}
    public static Document parse(String json){
        if (null == json || "".equals(json.trim())) {
            return new Document();
        }
        return Document.parse(json, new CustomDocumentDecoder()
                .registerCustomReader(AutoUpdateDateFilterTime.FUNCTION_NAME, originalValue -> new AutoUpdateDateFilterTime().execute(originalValue, null)));

    }
}
