package com.tapdata.mongo;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;


public class StringCodec implements Codec<String> {
	@Override
	public void encode(final BsonWriter writer, final String value, final EncoderContext encoderContext) {
//        if (StringUtils.isNotBlank(value)) {
//            if (ObjectId.isValid(value)) {
//                writer.writeObjectId(new ObjectId(value));
//                return;
//            }
//        }
		writer.writeString(value);
	}

	@Override
	public String decode(final BsonReader reader, final DecoderContext decoderContext) {
		BsonType currentBsonType = reader.getCurrentBsonType();
		switch (currentBsonType) {
			case SYMBOL:
				return reader.readSymbol();
			case OBJECT_ID:
				return reader.readObjectId().toHexString();
			case JAVASCRIPT:
				return reader.readJavaScript();
			case JAVASCRIPT_WITH_SCOPE:
				return reader.readJavaScriptWithScope();
			default:
				return reader.readString();
		}
	}

	@Override
	public Class<String> getEncoderClass() {
		return String.class;
	}

}
