package io.tapdata.pdk.core.api.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.parser.deserializer.ObjectDeserializer;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.TypeHolder;
import io.tapdata.pdk.core.utils.TapConstants;

import java.lang.reflect.Type;
import java.util.List;

@Implementation(JsonParser.class)
public class JsonParserImpl implements JsonParser {
    List<AbstractClassDetector> abstractClassDetectors;
    ParserConfig parserConfig;

    public JsonParser config( List<AbstractClassDetector> abstractClassDetectors){
        this.abstractClassDetectors = abstractClassDetectors;
        this.parserConfig = new ParserConfig();
        return this;
    }

    public JsonParserImpl() {

    }

    @Override
    public String toJsonWithClass(Object obj) {
        return JSON.toJSONString(obj, SerializerFeature.WriteClassName, SerializerFeature.DisableCircularReferenceDetect/*, SerializerFeature.SortField, SerializerFeature.MapSortField*/);
//        return PREFIX + obj.getClass().getName() + SUFFIX + JSON.toJSONString(obj);
    }

    @Override
    public Object fromJsonWithClass(String json) {
        return fromJsonWithClass(json, null);
    }

    @Override
    public Object fromJsonWithClass(String json, ClassLoader classLoader) {
        ParserConfig parserConfig = new ParserConfig();
        if (classLoader != null)
            parserConfig.setDefaultClassLoader(classLoader);
        return JSON.parseObject(json, DataMap.class, parserConfig, Feature.OrderedField, /*Feature.UseNativeJavaObject, */Feature.DisableCircularReferenceDetect);
//        if(json.startsWith(PREFIX)) {
//            int pos = json.indexOf(SUFFIX);
//            if(pos > 0) {
//                String classStr = json.substring(PREFIX.length(), pos);
//                if(classLoader != null) {
//                    try {
//                        Class<?> clazz = classLoader.loadClass(classStr);
//                        return JSON.parseObject(json.substring(pos + SUFFIX.length()), clazz);
//                    } catch (ClassNotFoundException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        }
//
//        return null;
    }

    @Override
    public String toJson(Object obj, ToJsonFeature... features) {
        if (features != null && features.length > 0) {
            //XXX to force adding WriteMapNullValue feature as the only one we have.
            return JSON.toJSONString(obj, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteMapNullValue/*, SerializerFeature.SortField, SerializerFeature.MapSortField*/);
        } else {
            return JSON.toJSONString(obj, SerializerFeature.DisableCircularReferenceDetect/*, SerializerFeature.SortField, SerializerFeature.MapSortField*/);
        }
    }

    @Override
    public byte[] toJsonBytes(Object obj, ToJsonFeature... features) {
        if (features != null && features.length > 0) {
            //XXX to force adding WriteMapNullValue feature as the only one we have.
            return JSON.toJSONBytes(obj, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteMapNullValue/*, SerializerFeature.SortField, SerializerFeature.MapSortField*/);
        } else {
            return JSON.toJSONBytes(obj, SerializerFeature.DisableCircularReferenceDetect/*, SerializerFeature.SortField, SerializerFeature.MapSortField*/);
        }
    }

    @Override
    public <T> T fromJsonBytes(byte[] jsonBytes, Class<T> clazz) {
        return JSON.parseObject(jsonBytes, clazz, Feature.OrderedField, /*Feature.UseNativeJavaObject, */Feature.DisableCircularReferenceDetect);
    }

    @Override
    public DataMap fromJsonObject(String json) {
        return JSON.parseObject(json, DataMap.class, Feature.OrderedField, /*Feature.UseNativeJavaObject, */Feature.DisableCircularReferenceDetect);
    }
    @Override
    public List<?> fromJsonArray(String json) {
        return JSON.parseObject(json, List.class, Feature.OrderedField, /*Feature.UseNativeJavaObject, */Feature.DisableCircularReferenceDetect);
    }

    @Override
    public Object fromJson(String json) {
        return JSON.parseObject(json, Feature.OrderedField, /*Feature.UseNativeJavaObject, */Feature.DisableCircularReferenceDetect);
    }

    @Override
    public <T> T fromJson(String json, Type clazz) {
        return JSON.parseObject(json, clazz, Feature.OrderedField, /*Feature.UseNativeJavaObject, */Feature.DisableCircularReferenceDetect);
    }
//    @Override
//    public <T> T fromJson(String json, Class<T> clazz) {
//        return JSON.parseObject(json, clazz, Feature.OrderedField, /*Feature.UseNativeJavaObject, */Feature.DisableCircularReferenceDetect);
//    }

    @Override
    public <T> T fromJson(String json, Class<T> clazz) {
        if (null != this.abstractClassDetectors) {
            return fromJson(json, clazz, this.abstractClassDetectors);
        }else {
            return JSON.parseObject(json, clazz, Feature.OrderedField, /*Feature.UseNativeJavaObject, */Feature.DisableCircularReferenceDetect);
        }
    }

    @Override
    public <T> T fromJson(String json, Class<T> clazz, List<AbstractClassDetector> abstractClassDetectors) {
        if (null == abstractClassDetectors){
            return JSON.parseObject(json, clazz, Feature.OrderedField, /*Feature.UseNativeJavaObject, */Feature.DisableCircularReferenceDetect);
        }
        if (!abstractClassDetectors.isEmpty() ) {
            ( null == parserConfig ? parserConfig = new ParserConfig() : parserConfig)
                    .putDeserializer(TapType.class, new AbstractResultDeserializer(abstractClassDetectors));
        }
        return JSON.parseObject(json, clazz, parserConfig, Feature.OrderedField, /*Feature.UseNativeJavaObject, */Feature.DisableCircularReferenceDetect);
    }

    @Override
    public <T> T fromJson(String json, TypeHolder<T> typeHolder) {
        return JSON.parseObject(json, typeHolder.getType(), Feature.OrderedField, /*Feature.UseNativeJavaObject, */Feature.DisableCircularReferenceDetect);
    }

    @Override
    public <T> T fromJson(String json, TypeHolder<T> typeHolder, List<AbstractClassDetector> abstractClassDetectors) {
        ParserConfig parserConfig = null;
        if (abstractClassDetectors != null && !abstractClassDetectors.isEmpty()) {
            parserConfig = new ParserConfig() {
                @Override
                public ObjectDeserializer getDeserializer(Type type) {
                    if (type == TapType.class) {
                        return new AbstractResultDeserializer(abstractClassDetectors);
                    }
                    return super.getDeserializer(type);
                }
            };
        }
        return JSON.parseObject(json, typeHolder.getType(), parserConfig, Feature.OrderedField, /*Feature.UseNativeJavaObject, */Feature.DisableCircularReferenceDetect);
    }

    public static void main(String[] args) {
        System.out.println("============JsonParser Takes Whit TapData And Not Set Config Before======================================");
        goJsonParserTakesWhitTapDataAndNotSetConfigBefore(100,1000);
        System.out.println("\n");
        System.out.println("============JSON.ParseObject Takes Whit TapData And Not Set Config Before================================");
        goJSONParseObjectTakesWhitTapDataAndNotSetConfigBefore(100,1000);
        System.out.println("\n");
        System.out.println("============JsonParser Takes Whit No TapData And Not Set Config Before===================================");
        goJsonParserTakesWhitNoTapDataAndNotSetConfigBefore(100,1000);
        System.out.println("\n");
        System.out.println("============JsonParser Takes Whit TapData Not Set Config Before==========================================");
        goJsonParserTakesWhitTapDataNotSetConfigBefore(100,1000);
        System.out.println("\n");
        System.out.println("============JsonParser Takes Whit TapData And Set Config Before==========================================");
        goJsonParserTakesWhitTapDataAndSetConfigBefore(100,1000);
        System.out.println("\n");
    }
    public static void goJsonParserTakesWhitTapDataAndNotSetConfigBefore(int testTimes,int runs){
        long total = 0;
        long max = 0;
        long min = 0;
        for (int i = 0; i < testTimes ; i++) {
            long test = goJsonParserTakesWhitTapDataAndNotSetConfigBefore(runs);
            min = i == 0 ? test : Math.min(min, test);
            total += test;
            max = Math.max(max,test);
        }
        System.out.println("|==============>Average QPS of " + testTimes + " times: " + total / testTimes);
        System.out.println("|==============>Max QPS of " + testTimes + " times: " + max);
        System.out.println("|==============>Min QPS of " + testTimes + " times: " + min);
    }
    public static void goJSONParseObjectTakesWhitTapDataAndNotSetConfigBefore(int testTimes,int runs){
        long total = 0;
        long max = 0;
        long min = 0;
        for (int i = 0; i < testTimes ; i++) {
            long test = goJSONParseObjectTakesWhitTapDataAndNotSetConfigBefore(runs);
            min = i == 0 ? test : Math.min(min, test);
            total += test;
            if (max<=test){
                max = test;
            }
        }
        System.out.println("|==============>Average QPS of " + testTimes + " times: " + total / testTimes);
        System.out.println("|==============>Max QPS of " + testTimes + " times: " + max);
        System.out.println("|==============>Min QPS of " + testTimes + " times: " + min);
    }
    public static void goJsonParserTakesWhitNoTapDataAndNotSetConfigBefore(int testTimes,int runs){
        long total = 0;
        long max = 0;
        long min = 0;
        for (int i = 0; i < testTimes ; i++) {
            long test = goJsonParserTakesWhitNoTapDataAndNotSetConfigBefore(runs);
            min = i == 0 ? test : Math.min(min, test);
            total += test;
            if (max<=test){
                max = test;
            }
        }
        System.out.println("|==============>Average QPS of " + testTimes + " times: " + total / testTimes);
        System.out.println("|==============>Max QPS of " + testTimes + " times: " + max);
        System.out.println("|==============>Min QPS of " + testTimes + " times: " + min);
    }
    public static void goJsonParserTakesWhitTapDataNotSetConfigBefore(int testTimes,int runs){
        long total = 0;
        long max = 0;
        long min = 0;
        for (int i = 0; i < testTimes ; i++) {
            long test = goJsonParserTakesWhitTapDataNotSetConfigBefore(runs);
            min = i == 0 ? test : Math.min(min, test);
            total += test;
            if (max<=test){
                max = test;
            }
        }
        System.out.println("|==============>Average QPS of " + testTimes + " times: " + total / testTimes);
        System.out.println("|==============>Max QPS of " + testTimes + " times: " + max);
        System.out.println("|==============>Min QPS of " + testTimes + " times: " + min);
    }
    public static void goJsonParserTakesWhitTapDataAndSetConfigBefore(int testTimes,int runs){
        long total = 0;
        long max = 0;
        long min = 0;
        for (int i = 0; i < testTimes ; i++) {
            long test = goJsonParserTakesWhitTapDataAndSetConfigBefore(runs);
            min = i == 0 ? test : Math.min(min, test);
            total += test;
            if (max<=test){
                max = test;
            }
        }
        System.out.println("|==============>Average QPS of " + testTimes + " times: " + total / testTimes);
        System.out.println("|==============>Max QPS of " + testTimes + " times: " + max);
        System.out.println("|==============>Min QPS of " + testTimes + " times: " + min);
    }


    public static long goJsonParserTakesWhitTapDataAndNotSetConfigBefore(int testTimes){
        JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
        String json = "{\"id\":\"adfs\",\"nameFieldMap\":{\"a\":{\"name\":\"a\",\"dataType\":\"varchar\",\"tapType\":{\"type\":8,\"bit\":32}}}}";
        Object value = JSON.parseObject(json, TapTable.class, TapConstants.tapdataParserConfig, Feature.DisableCircularReferenceDetect);//parseObject(jsonString, parameterTypes[i], TapConstants.tapdataParserConfig);
        long time = System.currentTimeMillis();
        for(int i = 0; i< testTimes; i++) {
            Object value1 = jsonParser.fromJson(json, TapTable.class, TapConstants.abstractClassDetectors);
        }
        //System.out.println("JsonParser takes whit TapData and Not set config before: " + (System.currentTimeMillis() - time));
        return System.currentTimeMillis() - time;
    }
    public static long goJSONParseObjectTakesWhitTapDataAndNotSetConfigBefore(int testTimes) {
        String json = "{\"id\":\"adfs\",\"nameFieldMap\":{\"a\":{\"name\":\"a\",\"dataType\":\"varchar\",\"tapType\":{\"type\":8,\"bit\":32}}}}";
        long time = System.currentTimeMillis();
        for(int i = 0; i< testTimes; i++) {
            Object value1 = JSON.parseObject(json, TapTable.class, TapConstants.tapdataParserConfig, Feature.DisableCircularReferenceDetect);//parseObject(jsonString, parameterTypes[i], TapConstants.tapdataParserConfig);
        }
        //System.out.println("JSON.parseObject takes whit TapData and Not set config before: " + (System.currentTimeMillis() - time));
        return System.currentTimeMillis() - time;
    }
    public static long goJsonParserTakesWhitNoTapDataAndNotSetConfigBefore(int testTimes) {
        JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
        String json1 = "{\"id\":\"adfs\",\"nameFieldMap\":{\"a\":{\"name\":\"a\",\"dataType\":\"varchar\"}}}";
        long time = System.currentTimeMillis();
        for(int i = 0; i< testTimes; i++) {
            Object value1 = jsonParser.fromJson(json1, TapTable.class, TapConstants.abstractClassDetectors);
        }
        //System.out.println("JsonParser takes whit no TapData and Not set config before: :" + (System.currentTimeMillis() - time));
        return System.currentTimeMillis() - time;
    }
    public static long goJsonParserTakesWhitTapDataNotSetConfigBefore( int testTimes){
        JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
        String json = "{\"id\":\"adfs\",\"nameFieldMap\":{\"a\":{\"name\":\"a\",\"dataType\":\"varchar\",\"tapType\":{\"type\":8,\"bit\":32}}}}";
        long time = System.currentTimeMillis();
        for(int i = 0; i< testTimes; i++) {
            Object value1 = jsonParser.fromJson(json, TapTable.class);
        }
        //System.out.println("JsonParser takes whit TapData Not set config before: " + (System.currentTimeMillis() - time));
        return System.currentTimeMillis() - time;
    }
    public static long goJsonParserTakesWhitTapDataAndSetConfigBefore(int testTimes){
        JsonParser jsonParser = InstanceFactory.instance(JsonParser.class).config(TapConstants.abstractClassDetectors);
        String json = "{\"id\":\"adfs\",\"nameFieldMap\":{\"a\":{\"name\":\"a\",\"dataType\":\"varchar\",\"tapType\":{\"type\":8,\"bit\":32}}}}";
        long time = System.currentTimeMillis();
        for(int i = 0; i< testTimes; i++) {
            Object value5 = jsonParser.fromJson(json, TapTable.class);
        }
        //System.out.println("JsonParser takes whit TapData and set config before: " + (System.currentTimeMillis() - time));
        return System.currentTimeMillis() - time;
    }
}
