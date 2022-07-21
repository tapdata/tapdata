package com.tapdata.mongo;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.types.ObjectId;

/**
 * @author samuel
 * @Description
 * @create 2020-10-30 12:30
 **/
public class ObjectIdCodec implements Codec<ObjectId> {
	@Override
	public ObjectId decode(BsonReader bsonReader, DecoderContext decoderContext) {
		return bsonReader.readObjectId();
	}

	@Override
	public void encode(BsonWriter bsonWriter, ObjectId objectId, EncoderContext encoderContext) {
		bsonWriter.writeString(objectId.toHexString());
	}

	@Override
	public Class<ObjectId> getEncoderClass() {
		return ObjectId.class;
	}
}
