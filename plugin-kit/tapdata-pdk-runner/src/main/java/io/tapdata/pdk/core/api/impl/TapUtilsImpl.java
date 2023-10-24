package io.tapdata.pdk.core.api.impl;

import com.alibaba.fastjson.JSON;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.TapUtils;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.pdk.core.dag.TapDAGNode;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.entity.utils.ReflectionUtil;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Implementation(TapUtils.class)
public class TapUtilsImpl implements TapUtils {

    @Override
    public ScheduledFuture<?> interval(Runnable runnable, int seconds) {
        return ExecutorsManager.getInstance().getScheduledExecutorService().schedule(runnable, seconds, TimeUnit.SECONDS);
    }

    @Override
    public Map<String, Object> cloneMap(Map<String, Object> map) {
        return (Map<String, Object>) clone(map);
    }

    @Override
    public String getStackTrace(Throwable throwable) {
        final StringWriter sw = new StringWriter();
        try(PrintWriter pw = new PrintWriter(sw, true)) {
            throwable.printStackTrace(pw);
            return sw.getBuffer().toString();
        }
    }

    public Object clone(Object obj) {
        if(obj instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) obj;
            Map<Object, Object> cloneMap = null;
            try {
                cloneMap = map.getClass().newInstance();
            } catch (Throwable ignored) {
                cloneMap = new LinkedHashMap<>();
            }
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                cloneMap.put(entry.getKey(), clone(entry.getValue()));
            }
            return cloneMap;
        } else if(obj instanceof Collection) {
            Collection<?> list = (Collection<?>) obj;
            Collection<Object> cloneList = null;
            try {
                cloneList = list.getClass().newInstance();
            } catch (Throwable ignored) {
                cloneList = new ArrayList<>();
            }
            for(Object o : list) {
                cloneList.add(clone(o));
            }
            return cloneList;
        } else {
            //The object here should be primitive value.
            return obj;
        }
    }

    private Object cloneObjectUsingSerializable(Object obj) {
        if (ReflectionUtil.isPrimitiveOrWrapper(obj.getClass()) || obj instanceof String) {
            return obj;
        } else if(obj instanceof Serializable) {
            Serializable serializable = (Serializable) obj;
            Serializable clone = SerializationUtils.clone(serializable);
            return clone;
        }
        return obj;
    }

    private Object cloneObjectUsingReflection(Object obj){
        try{
            if(ReflectionUtil.isPrimitiveOrWrapper(obj.getClass()) || obj instanceof String) {
                return obj;
            }
            if(obj instanceof Date) {
                Date date = (Date) obj;
                return new Date(date.getTime());
            }
            if(!ReflectionUtil.canBeInitiated(obj.getClass())) {
                return null;
            }
            Object clone = obj.getClass().newInstance();
            Field[] fields = ReflectionUtil.getFields(obj.getClass());
            for (Field field : fields) {
                field.setAccessible(true);
                Object fieldValue = field.get(obj);
                if(fieldValue == null || Modifier.isFinal(field.getModifiers())){
                    continue;
                }
                if(Map.class.isAssignableFrom(field.getType()) || Collection.class.isAssignableFrom(field.getType())) {
                    field.set(clone, clone(fieldValue));
                } else if(field.getType().isPrimitive() || field.getType().equals(String.class)
                        || field.getType().getSuperclass().equals(Number.class)
                        || field.getType().equals(Boolean.class)){
                    field.set(clone, fieldValue);
                } else {
                    if(fieldValue == obj){
                        field.set(clone, clone);
                    } else {
                        if(ReflectionUtil.canBeInitiated(field.getType())) {
                            field.set(clone, cloneObjectUsingReflection(fieldValue));
                        }
                    }
                }
            }
            return clone;
        } catch(Throwable e){
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String... args) {
        Date date = new Date();
        System.out.println("date " + date);

        TapDAGNode node = new TapDAGNode();
        node.setChildNodeIds(Arrays.asList("1", "3"));
        DataMap dataMap = new DataMap();

        TapTable table1 = new TapTable("t1")
                .add(new TapField().name("f").dataType("aaa"))
                .add(new TapField().name("a").dataType("aa"));
        dataMap.put("aaa", 1);
//        defaultMap.put("table", table1);
        node.setConnectionConfig(dataMap);
        TapTable table = new TapTable()
                .add(new TapField().name("f").dataType("aaa"))
                .add(new TapField().name("a").dataType("aa"));
//        node.setTable(table);
        TapInsertRecordEvent insertRecordEvent = new TapInsertRecordEvent();
        insertRecordEvent.setAfter(new HashMap<String, Object>(){{
            put("aa", "bb");
            put("cc", date);
//            put("ccca", node);
        }});
        insertRecordEvent.setInfo(new HashMap<String, Object>(){ {
            put("aaa", Arrays.asList("1", "2"));
            put("bbb", 1);
        }});
//        insertRecordEvent.setPdkId("aaaa");
        insertRecordEvent.setTableId(table.getId());

//        TapInsertRecordEvent clone = (TapInsertRecordEvent) new TapUtilsImpl().clone(insertRecordEvent);
//        TapInsertRecordEvent insertRecordEvent1 = JSON.parseObject(JSON.toJSONString(insertRecordEvent), TapInsertRecordEvent.class);

        HashMap embedded = new HashMap<String, Object>(){{
            put("aa", "bb");
            put("cc", date);
//            put("ccca", node);
        }};
        Map<String, Object> map = new HashMap<String, Object>(){ {
            put("aaa", Arrays.asList("1", "2"));
            put("bbb", 1);
            put("embedded", embedded);
        }};

//        long time = System.currentTimeMillis();
//        for(int i = 0; i < 1000000; i++) {
//            new TapUtilsImpl().clone(map);
//        }
//        System.out.println("reflection count per second "  + (1000000d / ((System.currentTimeMillis() - time) / 1000d)));
////        System.out.println("takes "  + (System.currentTimeMillis() - time));
//
//
//        time = System.currentTimeMillis();
//        for(int i = 0; i < 1000000; i++) {
//            JSON.parseObject(JSON.toJSONString(map), Map.class);
//        }
//        System.out.println("json count per second "  + (1000000d / ((System.currentTimeMillis() - time) / 1000d)));


        long time = System.currentTimeMillis();
        for(int i = 0; i < 1000000; i++) {
//            new TapUtilsImpl().clone(insertRecordEvent);
            insertRecordEvent.clone(new TapInsertRecordEvent());
        }
        System.out.println("reflection count per second "  + (1000000d / ((System.currentTimeMillis() - time) / 1000d)));
//        System.out.println("takes "  + (System.currentTimeMillis() - time));


        time = System.currentTimeMillis();
        for(int i = 0; i < 1000000; i++) {
            JSON.parseObject(JSON.toJSONString(insertRecordEvent), TapInsertRecordEvent.class);
        }
        System.out.println("json count per second "  + (1000000d / ((System.currentTimeMillis() - time) / 1000d)));


//        time = System.currentTimeMillis();
//        for(int i = 0; i < 1000000; i++) {
//            JSON.parseObject(JSON.toJSONString(insertRecordEvent), TapInsertRecordEvent.class);
//        }
//        System.out.println("jackson count per second "  + (1000000d / ((System.currentTimeMillis() - time) / 1000d)));

    }
}
