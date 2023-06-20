package io.tapdata.flow.engine.V2.node.hazelcast.processor.util;

import java.util.Collection;
import java.util.Map;

/**
 * @author GavinXiao
 * @description TapObjectTranslator create by Gavin
 * @create 2023/6/19 15:38
 **/
public interface TapObjectTranslator {
    public static Object translator(
            Object translatorObj,
            TranslatorNull translatorNull,
            TranslatorCollection translatorCollection,
            TranslatorMap translatorMap,
            TranslatorArray translatorArray,
            TranslatorDefault translatorDefault){
        if (null == translatorObj) {
            return translatorNull.translator();
        } else if (translatorObj instanceof Collection){
            return translatorCollection.translator((Collection<?>) translatorObj);
        } else if (translatorObj instanceof Map){
            return translatorMap.translator((Map<?, ?>) translatorObj);
        } else if (translatorObj.getClass().isArray()) {
            return translatorArray.translator((Object[]) translatorObj);
        }
        return translatorDefault.translator(translatorObj);
    }


    public static void translator(
            Object translatorObj,
            TranslatorNullV2 translatorNull,
            TranslatorCollectionV2 translatorCollection,
            TranslatorMapV2 translatorMap){
        translator(translatorObj, translatorNull,translatorCollection, translatorMap, obj->{}, obj -> {});
    }
    public static void translator(
            Object translatorObj,
            TranslatorNullV2 translatorNull,
            TranslatorCollectionV2 translatorCollection,
            TranslatorMapV2 translatorMap,
            TranslatorArrayV2 translatorArray,
            TranslatorDefaultV2 translatorDefault){
        if (null == translatorObj) {
            translatorNull.translator();
        } else if (translatorObj instanceof Collection){
            translatorCollection.translator((Collection<?>) translatorObj);
        } else if (translatorObj instanceof Map){
            translatorMap.translator((Map<?, ?>) translatorObj);
        } else if (translatorObj.getClass().isArray()) {
            translatorArray.translator((Object[]) translatorObj);
        } else {
            translatorDefault.translator(translatorObj);
        }
    }

    interface TranslatorNull {
        Object translator();
    }
    interface TranslatorCollection {
        Object translator(Collection<?> obj);
    }
    interface TranslatorMap {
        Object translator(Map<?, ?> obj);
    }
    interface TranslatorArray {
        Object translator(Object[] obj);
    }
    interface TranslatorDefault {
        Object translator(Object obj);
    }

    interface TranslatorNullV2 {
        void translator();
    }
    interface TranslatorCollectionV2{
        void translator(Collection<?> obj);
    }
    interface TranslatorMapV2{
        void translator(Map<?, ?> obj);
    }
    interface TranslatorArrayV2{
        void translator(Object[] obj);
    }
    interface TranslatorDefaultV2{
        void translator(Object obj);
    }
}
