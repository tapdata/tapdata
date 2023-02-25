package io.tapdata.entity.schema.value;

import io.tapdata.entity.schema.type.TapType;

public abstract class TapValue<T, P extends TapType> {
    /**
     * 数据源的原始值， 原始的类型， 例如mysql的类型， 对同构有用的
     */
    private Object originValue;
    public TapValue<T, P> originValue(Object originValue) {
        this.originValue = originValue;
        return this;
    }
    /**
     * 数据源原始值的类型， 例如VARCHAR， float(4)等等
     */
    private String originType;
    public TapValue<T, P> originType(String originType) {
        this.originType = originType;
        return this;
    }
    /**
     * TapType的标准值
     */
    protected T value;
    public TapValue<T, P> value(T value) {
        this.value = value;
        return this;
    }

    /**
     * 可以不填， 不填就和TapTable里的字段保持一样。
     * 如果有差异可以新建一个TapType填入。
     */
    private P tapType;
    public TapValue<T, P> tapType(P tapType) {
        this.tapType = tapType;
        return this;
    }

    public abstract TapType createDefaultTapType();

    public abstract Class<? extends TapType> tapTypeClass();

    public P getTapType() {
        return tapType;
    }

    public void setTapType(P tapType) {
        this.tapType = tapType;
    }

    public Object getOriginValue() {
        return originValue;
    }

    public void setOriginValue(Object originValue) {
        this.originValue = originValue;
    }

    public String getOriginType() {
        return originType;
    }

    public void setOriginType(String originType) {
        this.originType = originType;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "TapValue{" +
                "originValue=" + originValue +
                ", originType='" + originType + '\'' +
                ", value=" + value +
                ", tapType=" + tapType +
                '}';
    }
}
