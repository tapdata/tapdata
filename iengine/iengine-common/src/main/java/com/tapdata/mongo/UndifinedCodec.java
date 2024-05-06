/*
package com.tapdata.mongo;

import jdk.nashorn.internal.runtime.Undefined;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class UndifinedCodec implements Codec<Undefined> {

	@Override
	public Undefined decode(BsonReader reader, DecoderContext decoderContext) {
		return Undefined.getEmpty();
	}

	@Override
	public void encode(BsonWriter writer, Undefined value, EncoderContext encoderContext) {
		writer.writeNull();
	}

	@Override
	public Class<Undefined> getEncoderClass() {
		return Undefined.class;
	}
}
*/
