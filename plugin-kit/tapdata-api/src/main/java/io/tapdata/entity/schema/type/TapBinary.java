package io.tapdata.entity.schema.type;

import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.impl.ToTapBinaryCodec;
import io.tapdata.entity.schema.value.TapBinaryValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.utils.InstanceFactory;

import static io.tapdata.entity.simplify.TapSimplify.tapBinary;

public class TapBinary extends TapType {
    public TapBinary() {
        type = TYPE_BINARY;
    }
    /**
     * 字段的字节长度最大值
     */
    private Long bytes;
    public TapBinary bytes(Long length) {
        this.bytes = length;
        return this;
    }
    /**
     * 字段长度是否固定， 写一个字符， 补齐99个空字符的问题
     */
    private Boolean fixed;
    public TapBinary fixed(Boolean fixed) {
        this.fixed = fixed;
        return this;
    }

    private Long defaultValue;
    public TapBinary defaultValue(Long defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    private Integer byteRatio;
    public TapBinary byteRatio(Integer byteRatio) {
        this.byteRatio = byteRatio;
        return this;
    }
    public Long getBytes() {
        return bytes;
    }

    public void setBytes(Long bytes) {
        this.bytes = bytes;
    }

    public Boolean getFixed() {
        return fixed;
    }

    public void setFixed(Boolean fixed) {
        this.fixed = fixed;
    }

    public Long getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(Long defaultValue) {
        this.defaultValue = defaultValue;
    }

    public Integer getByteRatio() {
        return byteRatio;
    }

    public void setByteRatio(Integer byteRatio) {
        this.byteRatio = byteRatio;
    }

    @Override
    public TapType cloneTapType() {
        return tapBinary().bytes(bytes).defaultValue(defaultValue).byteRatio(byteRatio).fixed(fixed);
    }

    @Override
    public Class<? extends TapValue<?, ?>> tapValueClass() {
        return TapBinaryValue.class;
    }

    @Override
    public ToTapValueCodec<?> toTapValueCodec() {
        return InstanceFactory.instance(ToTapValueCodec.class, TapDefaultCodecs.TAP_BINARY_VALUE);
    }
}
