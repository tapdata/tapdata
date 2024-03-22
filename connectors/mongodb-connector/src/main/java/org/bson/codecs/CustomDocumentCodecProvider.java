package org.bson.codecs;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;

public class CustomDocumentCodecProvider extends DocumentCodecProvider {
    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        if (clazz == Document.class) {
            return (Codec<T>) new CustomDocumentDecoder();
        }
        return super.get(clazz, registry);
    }
}
