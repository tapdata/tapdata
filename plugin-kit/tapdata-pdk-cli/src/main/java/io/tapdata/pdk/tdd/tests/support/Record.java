package io.tapdata.pdk.tdd.tests.support;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.*;
import io.tapdata.entity.schema.value.DateTime;

import java.sql.Time;
import java.util.*;

public class Record extends HashMap {
    public static Record create(){
        return new Record();
    }
    private Record(){
        super();
    }
    public Record builder(String key,Object value){
        this.put(key,value);
        return this;
    }

    public Record reset(){
        return new Record();
    }

    public static Record[] testStart(int needCount){
        if (needCount<1) return new Record[0];
        Record[] records = new Record[needCount];
        for (int i = 0; i < needCount; i++) {
            records[i] = Record.create()
                    .builder("id", System.nanoTime())
                    .builder("name", "Test-"+i)
                    .builder("text", "Test-"+System.currentTimeMillis());
        }
        return records;
    }

    public static Record[] testRecordWithTapTable(TapTable table,int needCount){
        Record[] records = new Record[needCount];
        if (needCount<1 || null==table) return records;
        LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
        for (int i = 0; i < needCount; i++) {
            Record record = new Record();
            nameFieldMap.forEach((key,field)->{
                TapType type = field.getTapType();
                String keyName = field.getName();
                switch (type.getType()) {
                    case TapType.TYPE_ARRAY:{
                        List<String> list = new ArrayList<>();
                        list.add(UUID.randomUUID().toString());
                        list.add(UUID.randomUUID().toString());
                        record.builder(keyName,list);
                    };break;
                    case TapType.TYPE_BINARY:{
                        record.builder(keyName,UUID.randomUUID().getLeastSignificantBits());
                    };break;
                    case TapType.TYPE_BOOLEAN:{
                        record.builder(keyName,Math.random()*10+50>1000);
                    };break;
                    case TapType.TYPE_DATE:{
                        record.builder(keyName,new Date());
                    };break;
                    case TapType.TYPE_DATETIME:{
                        record.builder(keyName,new DateTime());
                    };break;
                    case TapType.TYPE_MAP:{
                        Map<String,Object> map = new HashMap<>();
                        map.put(UUID.randomUUID().toString(),UUID.randomUUID().toString());
                        map.put(UUID.randomUUID().toString(),UUID.randomUUID().toString());
                        record.builder(keyName,map);
                    };break;
                    case TapType.TYPE_NUMBER:{
                        record.builder(keyName,Math.random()*5+50);
                    };break;
                    case TapType.TYPE_RAW:{
                        record.builder(keyName,null);
                    };break;
                    case TapType.TYPE_STRING:{
                        record.builder(keyName,UUID.randomUUID().toString());
                    };break;
                    case TapType.TYPE_TIME:{
                        record.builder(keyName,new DateTime().toTime());
                    };break;
                    case TapType.TYPE_YEAR:{
                        record.builder(keyName,new Date().getYear());
                    };break;
                    default:record.builder(keyName,null);;
                }
            });
            records[i] = record;
        }
        return records;
    }
}
