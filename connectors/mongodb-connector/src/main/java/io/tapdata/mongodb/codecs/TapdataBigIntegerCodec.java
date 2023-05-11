package io.tapdata.mongodb.codecs;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.math.BigInteger;

public class TapdataBigIntegerCodec implements Codec<BigInteger> {

    @Override
    public BigInteger decode(BsonReader reader, DecoderContext decoderContext) {
        return BigInteger.valueOf(reader.readInt64());
    }

    @Override
    public void encode(BsonWriter writer, BigInteger value, EncoderContext encoderContext) {
        if (value == null) {
            writer.writeNull();
            return;
        }
        writer.writeInt64(value.longValue());
    }

    @Override
    public Class<BigInteger> getEncoderClass() {
        return BigInteger.class;
    }

}
