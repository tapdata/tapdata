package com.tapdata.mongo;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.sql.Timestamp;

public class DateToTimestampCodec implements Codec<Timestamp> {

	@Override
	public Timestamp decode(BsonReader reader, DecoderContext decoderContext) {
		long time = reader.readDateTime();
		return new Timestamp(time);
	}

	@Override
	public void encode(BsonWriter writer, Timestamp value, EncoderContext encoderContext) {
		writer.writeDateTime(value.getTime());
	}

	@Override
	public Class<Timestamp> getEncoderClass() {
		return Timestamp.class;
	}
}
