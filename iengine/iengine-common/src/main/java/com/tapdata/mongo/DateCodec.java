package com.tapdata.mongo;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.util.Date;

public class DateCodec implements Codec<Date> {
	@Override
	public void encode(final BsonWriter writer, final Date value, final EncoderContext encoderContext) {
		writer.writeDateTime(value.getTime());
	}

	@Override
	public Date decode(final BsonReader reader, final DecoderContext decoderContext) {
		BsonType currentBsonType = reader.getCurrentBsonType();
		switch (currentBsonType) {
			case TIMESTAMP:
				return new Date(Long.valueOf(reader.readTimestamp().getTime()) * 1000);
			default:
				return new Date(reader.readDateTime());
		}
	}

	@Override
	public Class<Date> getEncoderClass() {
		return Date.class;
	}
}
