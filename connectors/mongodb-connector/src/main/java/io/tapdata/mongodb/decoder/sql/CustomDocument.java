package io.tapdata.mongodb.decoder.sql;

import io.tapdata.mongodb.decoder.sql.autoUpdate.AutoUpdateDateFilterTime;
import org.bson.Document;
import org.bson.codecs.CustomDocumentDecoder;

public class CustomDocument {
    public static Document parse(String json){
        return Document.parse(json, new CustomDocumentDecoder()
                .registerCustomReader(AutoUpdateDateFilterTime.FUNCTION_NAME, originalValue -> new AutoUpdateDateFilterTime().execute(originalValue, null)));

    }
}
