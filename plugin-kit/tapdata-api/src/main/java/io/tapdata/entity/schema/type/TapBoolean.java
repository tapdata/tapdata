package io.tapdata.entity.schema.type;

import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.impl.ToTapBooleanCodec;
import io.tapdata.entity.schema.value.TapBooleanValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.utils.InstanceFactory;

import static io.tapdata.entity.simplify.TapSimplify.tapBoolean;

public class TapBoolean extends TapType {
    public TapBoolean() {
        type = TYPE_BOOLEAN;
    }
    @Override
    public TapType cloneTapType() {
        return tapBoolean();
    }

    @Override
    public Class<? extends TapValue<?, ?>> tapValueClass() {
        return TapBooleanValue.class;
    }

    @Override
    public ToTapValueCodec<?> toTapValueCodec() {
        return InstanceFactory.instance(ToTapValueCodec.class, TapDefaultCodecs.TAP_BOOLEAN_VALUE);
    }
}
