package io.tapdata.entity.schema.type;

import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.impl.ToTapMapCodec;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.utils.InstanceFactory;

import static io.tapdata.entity.simplify.TapSimplify.tapMap;

public class TapMap extends TapType {
    public TapMap() {
        type = TYPE_MAP;
    }
    @Override
    public TapType cloneTapType() {
        return tapMap();
    }

    @Override
    public Class<? extends TapValue<?, ?>> tapValueClass() {
        return TapMapValue.class;
    }

    @Override
    public ToTapValueCodec<?> toTapValueCodec() {
        return InstanceFactory.instance(ToTapValueCodec.class, TapDefaultCodecs.TAP_MAP_VALUE);
    }
}
