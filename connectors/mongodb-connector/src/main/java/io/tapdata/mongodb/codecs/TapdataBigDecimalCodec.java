package io.tapdata.mongodb.codecs;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.math.RoundingMode;

public final class TapdataBigDecimalCodec implements Codec<BigDecimal> {

    @Override
    public void encode(final BsonWriter writer, final BigDecimal value, final EncoderContext encoderContext) {
        if (value == null) {
            writer.writeDecimal128(null);
            return;
        }
        if (value.precision() > 34) {
            writer.writeDecimal128(new Decimal128(value.setScale(value.scale() + 34 - value.precision(), RoundingMode.HALF_UP)));
        } else {
            writer.writeDecimal128(new Decimal128(value));
        }
    }

    @Override
    public BigDecimal decode(final BsonReader reader, final DecoderContext decoderContext) {
        return reader.readDecimal128().bigDecimalValue();
    }

    @Override
    public Class<BigDecimal> getEncoderClass() {
        return BigDecimal.class;
    }
}
