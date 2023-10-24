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
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JavaTypesToTapTypes;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
        transformToTapValueMap(value, nameFieldMap, null, detectors);
    }
    public void transformToTapValueMap(Map<String, Object> value, Map<String, TapField> nameFieldMap, Map<String, TapValue<?, ?>> valueMap, TapDetector... detectors) {
        if(value == null)
            return;
        NewFieldDetector newFieldDetector = null;
        ToTapValueCheck toTapValueCheck = null;
        if(detectors != null) {
            for(TapDetector detector : detectors) {
                if(newFieldDetector == null && detector instanceof NewFieldDetector) {
                    newFieldDetector = (NewFieldDetector) detector;
                } else if(toTapValueCheck == null && detector instanceof ToTapValueCheck) {
                    toTapValueCheck = (ToTapValueCheck) detector;
                }
            }
        }
        AtomicReference<NewFieldDetector> newFieldDetectorRef = new AtomicReference<>(newFieldDetector);
        AtomicReference<ToTapValueCheck> toTapValueCheckRef = new AtomicReference<>(toTapValueCheck);
        mapIteratorToTapValue.iterate(value, (name, entry, recursive) -> {
            Object theValue = entry;
            String fieldName = name;
            TapValue<?, ?> originTapValue = null;
            if(theValue != null && fieldName != null) {
                if((theValue instanceof TapValue)) {
                    TapLogger.debug(TAG, "Value {} for field {} already in TapValue format, no need do ToTapValue conversion. ", theValue, fieldName);
                    return null;
                }

                String dataType = null;
                TapType typeFromSchema = null;
                ToTapValueCodec<?> valueCodec = null;

                boolean newField = false;

                originTapValue = valueMap != null ? valueMap.get(name) : null;

                if(nameFieldMap != null) {
                    valueCodec = this.codecsRegistry.getCustomToTapValueCodec(theValue.getClass());

                    TapField field = nameFieldMap.get(fieldName);
                    if(field != null) {
                        dataType = field.getDataType();
                        typeFromSchema = field.getTapType();
                        if(typeFromSchema != null && valueCodec == null) {
                            valueCodec = getValueCodec(typeFromSchema);
                            boolean isTypeQualified = true;
                            switch (typeFromSchema.getType()) {
                                case TapType.TYPE_ARRAY:
                                    if(!(theValue instanceof Collection)) isTypeQualified = false;
                                    break;
                                case TapType.TYPE_MAP:
                                    if(!(theValue instanceof Map)) isTypeQualified = false;
                                    break;
                                case TapType.TYPE_STRING:
                                    if(!(theValue instanceof String)) isTypeQualified = false;
                                    break;
                                case TapType.TYPE_NUMBER:
                                    if(!(theValue instanceof Number)) isTypeQualified = false;
                                default:
                                    break;
                            }
                            if(!isTypeQualified) {
                                valueCodec = null;
                                newField = true;
                            }
                        }
                    } else {
                        newField = true;
                    }
                }

                if(newField && valueCodec == null) {
                    valueCodec = getTapValueCodec(theValue);
                    typeFromSchema = JavaTypesToTapTypes.toTapType(theValue);
                }
//                if(valueCodec == null)
//                    throw new UnknownCodecException("toTapValueMap codec not found for value class " + theValue.getClass());
                if(valueCodec != null) {
                    TapValue tapValue = valueCodec.toTapValue(theValue, typeFromSchema);
                    if(tapValue == null && !newField) {
                        TapLogger.debug(TAG, "Value Codec {} from model convert TapValue failed, value {}", valueCodec.getClass().getSimpleName(), theValue);
                        valueCodec = getTapValueCodec(theValue);
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
                    if(typeFromSchema == null)
                        typeFromSchema = tapValue.createDefaultTapType();
                    //noinspection unchecked
                    tapValue.setTapType(typeFromSchema);
                    if(!theValue.equals(tapValue.getValue())) {
                        tapValue.setOriginValue(theValue);
                    }
                    tapValue.setOriginType(dataType);

                    if(newField) {
                        //Means new field.
                        if(!recursive && newFieldDetectorRef.get() != null) {
                            newFieldDetectorRef.get().detected(field(fieldName, typeFromSchema.getClass().getSimpleName()).tapType(typeFromSchema));
                        }
                    }
                    if(originTapValue != null) {
                        if(originTapValue.getValue().equals(tapValue.getValue())) {
                            if(tapValue.getOriginValue() == null) {
                                tapValue.setOriginValue(originTapValue.getOriginValue());
                                tapValue.setOriginType(originTapValue.getOriginType());
                            }
                        }
                    }
                    if(toTapValueCheckRef.get() == null)
                        return tapValue;
                    else
                        if(!toTapValueCheckRef.get().check(name, tapValue.getValue()))
                            throw new StopFilterException();
                    return null;
                }
                //Means new field.
                if(newField && !recursive && newFieldDetectorRef.get() != null && typeFromSchema != null) {
                    newFieldDetectorRef.get().detected(field(fieldName, typeFromSchema.getClass().getSimpleName()).tapType(typeFromSchema));
                }
            }
            if(originTapValue != null && originTapValue.getValue().equals(entry)) {
                if(toTapValueCheckRef.get() == null)
                    return originTapValue;
                else
                    if(!toTapValueCheckRef.get().check(name, originTapValue.getValue()))
                        throw new StopFilterException();
                return null;
            }
            if(toTapValueCheckRef.get() == null)
                return entry;
            else
                if(!toTapValueCheckRef.get().check(name, entry))
                    throw new StopFilterException();

            return null;
        });
    }

    private ToTapValueCodec<?> getTapValueCodec(Object theValue) {
        return this.codecsRegistry.getToTapValueCodec(theValue.getClass());
    }

    private ToTapValueCodec<?> getValueCodec(TapType typeFromSchema) {
        switch (typeFromSchema.getType()) {
            case TapType.TYPE_DATE:
            case TapType.TYPE_DATETIME:
            case TapType.TYPE_TIME:
            case TapType.TYPE_ARRAY:
            case TapType.TYPE_MAP:
            case TapType.TYPE_YEAR:
                return typeFromSchema.toTapValueCodec();
        }
        return null;
    }

    public Map<String, TapValue<?, ?>> transformFromTapValueMap(Map<String, Object> tapValueMap) {
        return transformFromTapValueMap(tapValueMap, null);
    }

    public Map<String, TapValue<?, ?>> transformFromTapValueMap(Map<String, Object> tapValueMap, Map<String, TapField> sourceNameFieldMap) {
        Map<String, TapValue<?, ?>> valueMap = new ConcurrentHashMap<>();
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
                    if(sourceNameFieldMap != null && !sourceNameFieldMap.containsKey(fieldName)) {
                        //Handle inserted new field
                        sourceNameFieldMap.put(fieldName, field(fieldName, theValue.getOriginType()).tapType(theValue.getTapType()));
                    }
                    //TODO Handle updated tapType field?
                    //TODO Handle deleted field?
                    Object value = fromTapValueCodec.fromTapValue(theValue);
//                    theValue.setValue(null);
                    if(theValue.getOriginValue() != null)
                        valueMap.put(fieldName, theValue);
                    return value;
                }
            } /*else if(object != null) {
                TapLogger.debug(TAG, "transformFromTapValueMap failed as object is not TapValue, but type {} value {}", object.getClass(), object);
            }*/
            return null;
        });
        return valueMap;
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
