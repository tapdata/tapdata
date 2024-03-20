package org.bson.codecs;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.Transformer;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Arrays.asList;
import static org.bson.codecs.BsonTypeClassMap.DEFAULT_BSON_TYPE_CLASS_MAP;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;


public class CustomDocumentDecoder extends DocumentCodec {
    private static final CodecRegistry DEFAULT_REGISTRY = fromProviders(asList(
            new ValueCodecProvider(),
            new IterableCodecProvider(),
            new BsonValueCodecProvider(),
            new CustomDocumentCodecProvider(),
            new MapCodecProvider()));
    private static final BsonTypeCodecMap DEFAULT_BSON_TYPE_CODEC_MAP = new BsonTypeCodecMap(
            DEFAULT_BSON_TYPE_CLASS_MAP,
            DEFAULT_REGISTRY
    );

    protected static final Map<String, DoCustomReader> CUSTOM_READER_MAP = new ConcurrentHashMap<>();

    public CustomDocumentDecoder() {

    }

    public CustomDocumentDecoder registerCustomReader(String tag, DoCustomReader reader) {
        CUSTOM_READER_MAP.put(tag, reader);
        return this;
    }

    public Document decode(BsonReader reader, DecoderContext decoderContext) {
       return (Document) decode(reader, decoderContext, null);
    }

    public Object decode(BsonReader reader, DecoderContext decoderContext, String oldKey) {
        Document document = new Document();
        reader.readStartDocument();
        try {
            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                String fieldName = reader.readName();
                Object readValue = readValue(reader, decoderContext, value -> value, fieldName);
                DoCustomReader customReader = CUSTOM_READER_MAP.get(fieldName);
                if (null != customReader) {
                    readValue = customReader.read(readValue);
                    if (null != oldKey) {
                        return readValue;
                    }
                }
                document.put(fieldName, readValue);
            }
        } finally {
            reader.readEndDocument();
        }
        return document;
    }

    Object readValue(final BsonReader reader,
                     final DecoderContext decoderContext,
                     final Transformer valueTransformer,
                     String oldKey) {
        BsonType bsonType = reader.getCurrentBsonType();
        if (bsonType == BsonType.NULL) {
            reader.readNull();
            return null;
        } else {
            Codec<?> codec = DEFAULT_BSON_TYPE_CODEC_MAP.get(bsonType);
            if (codec instanceof CustomDocumentDecoder) {
                return valueTransformer.transform(decode(reader, decoderContext, oldKey));
            }
            return valueTransformer.transform(codec.decode(reader, decoderContext));
        }
    }

    public interface DoCustomReader {
        Object read(Object originalValue);
    }
}