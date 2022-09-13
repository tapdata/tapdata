package io.tapdata.entity.codec.filter;

import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.detector.TapDetector;
import io.tapdata.entity.codec.detector.impl.NewFieldDetector;
import io.tapdata.entity.codec.filter.impl.AllLayerMapIterator;
import io.tapdata.entity.codec.filter.impl.AllLayerMapIteratorFromTapValue;
import io.tapdata.entity.error.UnknownCodecException;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JavaTypesToTapTypes;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.*;

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

    public void transformToTapValueMap(Map<String, Object> value, Map<String, TapField> nameFieldMap, TapDetector... detectors) {
        if(value == null)
            return;
        NewFieldDetector newFieldDetector = null;
        if(detectors != null) {
            for(TapDetector detector : detectors) {
                if(newFieldDetector == null && detector instanceof NewFieldDetector) {
                    newFieldDetector = (NewFieldDetector) detector;
                }
            }
        }
        AtomicReference<NewFieldDetector> newFieldDetectorRef = new AtomicReference<>(newFieldDetector);
        mapIteratorToTapValue.iterate(value, (name, entry, recursive) -> {
            Object theValue = entry;
            String fieldName = name;
            if(theValue != null && fieldName != null) {
                if((theValue instanceof TapValue)) {
                    TapLogger.debug(TAG, "Value {} for field {} already in TapValue format, no need do ToTapValue conversion. ", theValue, fieldName);
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
                    typeFromSchema = JavaTypesToTapTypes.toTapType(theValue);
                    handleByTypeCodec = true;
                }
//                if(valueCodec == null)
//                    throw new UnknownCodecException("toTapValueMap codec not found for value class " + theValue.getClass());
                if(valueCodec != null) {
                    TapValue tapValue = valueCodec.toTapValue(theValue, typeFromSchema);
                    if(tapValue == null && !handleByTypeCodec) {
                        TapLogger.debug(TAG, "Value Codec {} from model convert TapValue failed, value {}", valueCodec.getClass().getSimpleName(), theValue);
                        valueCodec = this.codecsRegistry.getToTapValueCodec(theValue.getClass());
                        if(valueCodec != null) {
                            tapValue = valueCodec.toTapValue(theValue, typeFromSchema);
                            if(tapValue == null) {
                                TapLogger.debug(TAG, "Value Codec {} from type convert TapValue failed, value {}", valueCodec.getClass().getSimpleName(), theValue);
                            } else {
                                if(typeFromSchema != null && !typeFromSchema.getClass().equals(tapValue.tapTypeClass())) {
                                    typeFromSchema = JavaTypesToTapTypes.toTapType(theValue);
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

                    if(handleByTypeCodec) {
                        //Means new field.
                        if(!recursive && newFieldDetectorRef.get() != null) {
                            newFieldDetectorRef.get().detected(field(fieldName, typeFromSchema.getClass().getSimpleName()).tapType(typeFromSchema));
                        }
                    }

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
        mapIteratorFromTapValue.iterate(tapValueMap, (fieldName, object, recursive) -> {
//            Object object = stringTapValueEntry.getValue();
            if(object instanceof TapValue) {
                TapValue<?, ?> theValue = (TapValue<?, ?>) object;
//                String fieldName = stringTapValueEntry.getKey();
                if(fieldName != null) {
                    FromTapValueCodec<TapValue<?, ?>> fromTapValueCodec = this.codecsRegistry.getCustomFromTapValueCodec((Class<TapValue<?, ?>>) theValue.getClass());
                    if(fromTapValueCodec != null) {
                        if(theValue instanceof TapMapValue) {
                            transformFromTapValueMap(((TapMapValue) theValue).getValue());
                        } else if(theValue instanceof TapArrayValue) {
                            transformFromTapValueMap(fieldName, (TapArrayValue) theValue, sourceNameFieldMap);
                        }
                    } else {
                        fromTapValueCodec = this.codecsRegistry.getDefaultFromTapValueCodec((Class<TapValue<?, ?>>) theValue.getClass());
                    }
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
                TapLogger.debug(TAG, "transformFromTapValueMap failed as object is not TapValue, but type {} value {}", object.getClass(), object);
            }
            return null;
        });
        return nameFieldMap;
    }

    public Map<String, TapField> transformFromTapValueMap(String theFieldName, TapArrayValue tapValueArray, Map<String, TapField> sourceNameFieldMap) {
        Map<String, TapField> nameFieldMap = sourceNameFieldMap != null ? sourceNameFieldMap : new LinkedHashMap<>();

        EntryFilter entryFilter = (fieldName, object, recursive) -> {
//            Object object = stringTapValueEntry.getValue();
            if(object instanceof TapValue) {
                TapValue<?, ?> theValue = (TapValue<?, ?>) object;
//                String fieldName = stringTapValueEntry.getKey();
                if(fieldName != null) {
                    FromTapValueCodec<TapValue<?, ?>> fromTapValueCodec = this.codecsRegistry.getCustomFromTapValueCodec((Class<TapValue<?, ?>>) theValue.getClass());
                    if(fromTapValueCodec != null) {
                        if(theValue instanceof TapMapValue) {
                            transformFromTapValueMap(((TapMapValue) theValue).getValue());
                        } else if(theValue instanceof TapArrayValue) {
                            transformFromTapValueMap(theFieldName, (TapArrayValue) theValue, sourceNameFieldMap);
                        } else {

                        }
                    } else {
                        fromTapValueCodec = this.codecsRegistry.getDefaultFromTapValueCodec((Class<TapValue<?, ?>>) theValue.getClass());
                    }
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
                TapLogger.debug(TAG, "transformFromTapValueMap failed as object is not TapValue, but type {} value {}", object.getClass(), object);
            }
            return null;
        };

        int i = 0;
        List<Object> newList = new ArrayList<>();
        for(Object obj : tapValueArray.getValue()) {
            if(obj instanceof TapMapValue) {
                transformFromTapValueMap(((TapMapValue) obj).getValue(), sourceNameFieldMap);
                newList.add(((TapMapValue) obj).getValue());
//                mapIteratorFromTapValue.iterate(((TapMapValue) obj).getValue(), entryFilter);
            } else if(obj instanceof TapArrayValue){
                transformFromTapValueMap(theFieldName, (TapArrayValue) obj, sourceNameFieldMap);
                newList.add(((TapArrayValue) obj).getValue());
            } else {
                Object newValue = entryFilter.filter(theFieldName + "." + i, obj, true);
                if(newValue != null) {
                    newList.add(newValue);
                } else {
                    newList.add(obj);
                }
            }
            i++;
        }
        tapValueArray.setValue(newList);

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
