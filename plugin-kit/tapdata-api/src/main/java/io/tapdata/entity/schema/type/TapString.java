package io.tapdata.entity.schema.type;

import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.impl.ToTapStringCodec;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.utils.InstanceFactory;

import static io.tapdata.entity.simplify.TapSimplify.tapString;

public class TapString extends TapType {
    public TapString() {
        type = TYPE_STRING;
    }
    public TapString(Long bytes, Boolean fixed) {
        this();
        this.bytes = bytes;
        this.fixed = fixed;
    }
    /**
     * 字段类型的长度最大值， VARCHAR(100), 只支持100长度的字符串
     */
    private Long bytes;
    public TapString bytes(Long bytes) {
        this.bytes = bytes;
        return this;
    }
    /**
     * 字段长度是否固定， 写一个字符， 补齐99个空字符的问题
     */
    private Boolean fixed;
    public TapString fixed(Boolean fixed) {
        this.fixed = fixed;
        return this;
    }

    private Long defaultValue;
    public TapString defaultValue(Long defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    private Integer byteRatio;
    public TapString byteRatio(Integer byteRatio) {
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
        return tapString().fixed(fixed).bytes(bytes).byteRatio(byteRatio).defaultValue(defaultValue);
    }

    @Override
    public Class<? extends TapValue<?, ?>> tapValueClass() {
        return TapStringValue.class;
    }

    @Override
    public ToTapValueCodec<?> toTapValueCodec() {
        return InstanceFactory.instance(ToTapValueCodec.class, TapDefaultCodecs.TAP_STRING_VALUE);
    }
}
