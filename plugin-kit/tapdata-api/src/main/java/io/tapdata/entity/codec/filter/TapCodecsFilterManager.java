package io.tapdata.entity.codec.filter;

import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.filter.impl.AllLayerMapIterator;
import io.tapdata.entity.codec.filter.impl.AllLayerMapIteratorFromTapValue;
import io.tapdata.entity.error.UnknownCodecException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.utils.InstanceFactory;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.field;

public class TapCodecsFilterManager {
    private static final String TAG = TapCodecsFilterManager.class.getSimpleName();
    private MapIteratorEx mapIteratorToTapValue;
    private MapIteratorEx mapIteratorFromTapValue;
    private final TapCodecsRegistry codecsRegistry;

    public TapCodecsFilterManager(TapCodecsRegistry codecsRegistry) {
        this.codecsRegistry = codecsRegistry;
        mapIteratorToTapValue = new AllLayerMapIterator();
        mapIteratorFromTapValue = new AllLayerMapIteratorFromTapValue();
//        mapIterator = new FirstLayerMapIterator();
    }

    public static TapCodecsFilterManager create(TapCodecsRegistry codecsRegistry) {
        return new TapCodecsFilterManager(codecsRegistry);
    }

    public void transformToTapValueMap(Map<String, Object> value, Map<String, TapField> nameFieldMap) {
        if(value == null)
            return;
        mapIteratorToTapValue.iterate(value, (name, entry) -> {
            Object theValue = entry;
            String fieldName = name;
            if(theValue != null && fieldName != null) {
                if((theValue instanceof TapValue)) {
                    TapLogger.warn(TAG, "Value {} for field {} already in TapValue format, no need do ToTapValue conversion. ", theValue, fieldName);
                   return null;
                }

                String dataType = null;
                TapType typeFromSchema = null;
                ToTapValueCodec<?> valueCodec = null;
                if(nameFieldMap != null) {
                    valueCodec = this.codecsRegistry.getCustomToTapValueCodec(theValue.getClass());

                    TapField field = nameFieldMap.get(fieldName);
                    if(field != null) {
                        dataType = field.getDataType();
                        typeFromSchema = field.getTapType();
                        if(typeFromSchema != null && valueCodec == null)
                            valueCodec = typeFromSchema.toTapValueCodec();
                    }
                }
                boolean handleByTypeCodec = false;
                if(valueCodec == null) {
                    valueCodec = this.codecsRegistry.getToTapValueCodec(theValue.getClass());
                    handleByTypeCodec = true;
                }
//                if(valueCodec == null)
//                    throw new UnknownCodecException("toTapValueMap codec not found for value class " + theValue.getClass());
                if(valueCodec != null) {
                    TapValue tapValue = valueCodec.toTapValue(theValue, typeFromSchema);
                    if(tapValue == null && !handleByTypeCodec) {
                        TapLogger.warn(TAG, "Value Codec {} from model convert TapValue failed, value {}", valueCodec.getClass().getSimpleName(), theValue);
                        valueCodec = this.codecsRegistry.getToTapValueCodec(theValue.getClass());
                        if(valueCodec != null) {
                            tapValue = valueCodec.toTapValue(theValue, typeFromSchema);
                            if(tapValue == null) {
                                TapLogger.warn(TAG, "Value Codec {} from type convert TapValue failed, value {}", valueCodec.getClass().getSimpleName(), theValue);
                            } else {
                                if(typeFromSchema != null && !typeFromSchema.getClass().equals(tapValue.tapTypeClass())) {
                                    typeFromSchema = null;
                                }
                            }
                        }
                    }
                    if(tapValue == null) {
                        tapValue = InstanceFactory.instance(ToTapValueCodec.class, TapDefaultCodecs.TAP_RAW_VALUE)
                                .toTapValue(theValue, typeFromSchema);
                    }
                    tapValue.setOriginType(dataType);
                    if(typeFromSchema == null)
                        typeFromSchema = tapValue.createDefaultTapType();
                    tapValue.setTapType(typeFromSchema);
                    tapValue.setOriginValue(theValue);
//                    entry.setValue(tapValue);
                    return tapValue;
                }
            }
            return null;
        });
    }

    public Map<String, TapField> transformFromTapValueMap(Map<String, Object> tapValueMap) {
        return transformFromTapValueMap(tapValueMap, null);
    }

    public Map<String, TapField> transformFromTapValueMap(Map<String, Object> tapValueMap, Map<String, TapField> sourceNameFieldMap) {
        Map<String, TapField> nameFieldMap = sourceNameFieldMap != null ? sourceNameFieldMap : new LinkedHashMap<>();
        mapIteratorFromTapValue.iterate(tapValueMap, (fieldName, object) -> {
//            Object object = stringTapValueEntry.getValue();
            if(object instanceof TapValue) {
                TapValue<?, ?> theValue = (TapValue<?, ?>) object;
//                String fieldName = stringTapValueEntry.getKey();
                if(fieldName != null) {
                    FromTapValueCodec<TapValue<?, ?>> fromTapValueCodec = this.codecsRegistry.getFromTapValueCodec((Class<TapValue<?, ?>>) theValue.getClass());
                    if(fromTapValueCodec == null)
                        throw new UnknownCodecException("fromTapValueMap codecs not found for value class " + theValue.getClass());

//                    stringTapValueEntry.setValue(fromTapValueCodec.fromTapValue(theValue));
                    if(!nameFieldMap.containsKey(fieldName)) {
                        //Handle inserted new field
                        nameFieldMap.put(fieldName, field(fieldName, theValue.getOriginType()).tapType(theValue.getTapType()));
                    }
                    //TODO Handle updated tapType field?
                    //TODO Handle deleted field?
                    return fromTapValueCodec.fromTapValue(theValue);
                }
            } else if(object != null) {
                TapLogger.warn(TAG, "transformFromTapValueMap failed as object is not TapValue, but type {} value {}", object.getClass(), object);
            }
            return null;
        });
        return nameFieldMap;
    }

    public String getDataTypeByTapType(Class<? extends TapType> tapTypeClass) {
        return codecsRegistry.getDataTypeByTapType(tapTypeClass);
    }

    public Map<Class<?>, String> getTapTypeDataTypeMap() {
        return codecsRegistry.getTapTypeDataTypeMap();
    }

    public ToTapValueCodec<?> getToTapValueCodec(Object value) {
        return this.codecsRegistry.getToTapValueCodec(value.getClass());
    }

    public MapIteratorEx getMapIteratorToTapValue() {
        return mapIteratorToTapValue;
    }

    public void setMapIteratorToTapValue(MapIteratorEx mapIteratorToTapValue) {
        this.mapIteratorToTapValue = mapIteratorToTapValue;
    }

    public TapCodecsRegistry getCodecsRegistry() {
        return codecsRegistry;
    }
}
