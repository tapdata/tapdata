package com.tapdata.mongo;

import org.bson.BsonInvalidOperationException;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.math.BigDecimal;

import static java.lang.String.format;


public class FloatCodec implements Codec<Float> {

	@Override
	public void encode(final BsonWriter writer, final Float value, final EncoderContext encoderContext) {
		if (value != null) {
			writer.writeDouble(new BigDecimal(value.toString()).doubleValue());
		} else {
			writer.writeNull();
		}
	}

	@Override
	public Float decode(final BsonReader reader, final DecoderContext decoderContext) {
		double value = decodeDouble(reader);
		if (value < -Float.MAX_VALUE || value > Float.MAX_VALUE) {
			throw new BsonInvalidOperationException(format("%s can not be converted into a Float.", value));
		}
		return (float) value;
	}

	private double decodeDouble(final BsonReader reader) {
		double doubleValue;
		BsonType bsonType = reader.getCurrentBsonType();
		switch (bsonType) {
			case INT32:
				doubleValue = reader.readInt32();
				break;
			case INT64:
				long longValue = reader.readInt64();
				doubleValue = longValue;
				if (longValue != (long) doubleValue) {
					throw invalidConversion(Double.class, longValue);
				}
				break;
			case DOUBLE:
				doubleValue = reader.readDouble();
				break;
			default:
				throw new BsonInvalidOperationException(format("Invalid numeric type, found: %s", bsonType));
		}
		return doubleValue;
	}

	private static <T extends Number> BsonInvalidOperationException invalidConversion(final Class<T> clazz, final Number value) {
		return new BsonInvalidOperationException(format("Could not convert `%s` to a %s without losing precision", value, clazz));
	}

	@Override
	public Class<Float> getEncoderClass() {
		return Float.class;
	}
}
