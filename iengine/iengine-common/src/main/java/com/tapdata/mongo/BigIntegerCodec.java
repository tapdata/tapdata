package com.tapdata.mongo;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.math.BigInteger;

public class BigIntegerCodec implements Codec<BigInteger> {

	@Override
	public BigInteger decode(BsonReader reader, DecoderContext decoderContext) {
		return new BigInteger(reader.readString());
	}

	@Override
	public void encode(BsonWriter writer, BigInteger value, EncoderContext encoderContext) {
		writer.writeString(value.toString());
	}

	@Override
	public Class<BigInteger> getEncoderClass() {
		return BigInteger.class;
	}
}
