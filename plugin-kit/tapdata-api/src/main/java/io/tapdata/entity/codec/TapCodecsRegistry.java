package io.tapdata.entity.codec;

import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapValue;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TapCodecsRegistry {
    private final Map<Class<?>, ToTapValueCodec<? extends TapValue<?, ?>>> classToTapValueCodecMap = new ConcurrentHashMap<>();
    private final Map<Class<? extends TapValue<?, ?>>, FromTapValueCodec<? extends TapValue<?, ?>>> classFromTapValueCodecMap = new ConcurrentHashMap<>();
    private final Map<Class<?>, String> tapTypeDataTypeMap = new ConcurrentHashMap<>();

//    private final Map<String, ToTapValueCodec<?>> fieldToTapValueCodecMap = new ConcurrentHashMap<>();

    public TapCodecsRegistry() {
    }

    public static TapCodecsRegistry create() {
        return new TapCodecsRegistry();
    }

    public <T extends TapValue<?, ?>> TapCodecsRegistry registerToTapValue(Class<?> anyClass, ToTapValueCodec<T> toTapValueCodec) {
        classToTapValueCodecMap.put(anyClass, toTapValueCodec);
        return this;
    }

    public void unregisterToTapValue(Class<?> anyClass) {
        classToTapValueCodecMap.remove(anyClass);
    }

    public <T extends TapValue<?, ?>> boolean isRegisteredFromTapValue(Class<T> tapValueClass) {
        return tapTypeDataTypeMap.containsKey(tapValueClass);
    }

    public <T extends TapValue<?, ?>> TapCodecsRegistry registerFromTapValue(Class<T> tapValueClass, FromTapValueCodec<T> fromTapValueCodec) {
        return registerFromTapValue(tapValueClass, null, fromTapValueCodec);
    }
    public <T extends TapValue<?, ?>> TapCodecsRegistry registerFromTapValue(Class<T> tapValueClass, String dataType, FromTapValueCodec<T> fromTapValueCodec) {
        if(dataType != null) {
            Type[] types = ((ParameterizedTypeImpl) tapValueClass.getGenericSuperclass()).getActualTypeArguments();
            if(types != null && types.length == 2 && types[1] instanceof Class) {
                Class<?> theTapTypeClass = (Class<?>) types[1];
                tapTypeDataTypeMap.put(theTapTypeClass, dataType);
            }
        }
        if(fromTapValueCodec != null)
            classFromTapValueCodecMap.put(tapValueClass, fromTapValueCodec);
        return this;
    }

    public void unregisterFromTapValue(Class<? extends TapValue<?, ?>> tapTypeClass) {
        classFromTapValueCodecMap.remove(tapTypeClass);
    }

//    public <T extends TapValue<?, ?>> TapCodecRegistry registerFieldToTapValue(String fieldName, ToTapValueCodec<T> toTapValueCodec) {
//        fieldToTapValueCodecMap.put(fieldName, toTapValueCodec);
//        return this;
//    }
//
//    public void unregisterFieldToTapValue(String fieldName) {
//        fieldToTapValueCodecMap.remove(fieldName);
//    }

    public ToTapValueCodec<?> getCustomToTapValueCodec(Class<?> clazz) {
        ToTapValueCodec<?> codec = classToTapValueCodecMap.get(clazz);
        return codec;
    }

    public ToTapValueCodec<?> getToTapValueCodec(Class<?> clazz) {
        ToTapValueCodec<?> codec = TapDefaultCodecs.instance.getToTapValueCodec(clazz);
        if(codec == null)
            codec = TapDefaultCodecs.instance.isRawCodec(clazz);
        return codec;
    }

    public <T extends TapValue<?, ?>> FromTapValueCodec<T> getFromTapValueCodec(Class<T> clazz) {
        FromTapValueCodec<T> codec = (FromTapValueCodec<T>) classFromTapValueCodecMap.get(clazz);
        if(codec == null) {
            codec = (FromTapValueCodec<T>) TapDefaultCodecs.instance.getFromTapValueCodec(clazz);
        }
        return codec;
    }

    public <T extends TapValue<?, ?>> FromTapValueCodec<T> getCustomFromTapValueCodec(Class<T> clazz) {
        FromTapValueCodec<T> codec = (FromTapValueCodec<T>) classFromTapValueCodecMap.get(clazz);
        return codec;
    }

    public <T extends TapValue<?, ?>> FromTapValueCodec<T> getDefaultFromTapValueCodec(Class<T> clazz) {
        FromTapValueCodec<T> codec = (FromTapValueCodec<T>) TapDefaultCodecs.instance.getFromTapValueCodec(clazz);
        return codec;
    }

    public <T extends TapValue<?, ?>> Object getValueFromDefaultTapValueCodec(T tapValue) {
        FromTapValueCodec<T> codec = (FromTapValueCodec<T>) TapDefaultCodecs.instance.getFromTapValueCodec((Class<? extends TapValue<?, ?>>) tapValue.getClass());
        if(codec != null) {
            return codec.fromTapValue(tapValue);
        }
        return null;
    }

    public String getDataTypeByTapType(Class<? extends TapType> tapTypeClass) {
        return tapTypeDataTypeMap.get(tapTypeClass);
    }

    public Map<Class<?>, String> getTapTypeDataTypeMap() {
        return tapTypeDataTypeMap;
    }

    public void setTapTypeDataTypeMap(Map<Class<?>, String> tapTypeDataTypeMap) {
        if(tapTypeDataTypeMap != null) {
            this.tapTypeDataTypeMap.clear();
            this.tapTypeDataTypeMap.putAll(tapTypeDataTypeMap);
        }
    }

    public TapCodecsRegistry withTapTypeDataTypeMap(Map<Class<?>, String> tapTypeDataTypeMap) {
        setTapTypeDataTypeMap(tapTypeDataTypeMap);
        return this;
    }

    //    public ToTapValueCodec<?> getFieldToTapValueCodec(String fieldName) {
//        ToTapValueCodec<?> codec = fieldToTapValueCodecMap.get(fieldName);
//        return codec;
//    }
}
