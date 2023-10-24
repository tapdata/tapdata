package io.tapdata.entity.schema.type;

import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.impl.ToTapRawCodec;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.utils.InstanceFactory;

import static io.tapdata.entity.simplify.TapSimplify.tapRaw;

/**
 * 不是为了数据库写入， 而是我们在内存里写， 带到目标库。
 * 在完全不认识的字段时， 会有用户， 例如同构时， 可以使用这个在字段到目标库， 写入， 之后originValue， 都可以没有value
 */
public class TapRaw extends TapType {
    public TapRaw() {
        type = TYPE_RAW;
    }
    @Override
    public TapType cloneTapType() {
        return tapRaw();
    }

    @Override
    public Class<? extends TapValue<?, ?>> tapValueClass() {
        return TapRawValue.class;
    }

    @Override
    public ToTapValueCodec<?> toTapValueCodec() {
        return InstanceFactory.instance(ToTapValueCodec.class, TapDefaultCodecs.TAP_RAW_VALUE);
    }
}
