package com.tapdata.mongo;

import org.bson.BsonBinary;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class ByteArrayCodec implements Codec<byte[]> {

	@Override
	public byte[] decode(BsonReader reader, DecoderContext decoderContext) {
		BsonBinary bsonBinary = reader.readBinaryData();
		return bsonBinary.getData();
	}

	@Override
	public void encode(BsonWriter writer, byte[] value, EncoderContext encoderContext) {
		writer.writeBinaryData(new BsonBinary(value));
	}

	@Override
	public Class<byte[]> getEncoderClass() {
		return byte[].class;
	}
}
