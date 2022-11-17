package io.tapdata.pdk.tdd.tests.support;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.*;
import io.tapdata.entity.schema.value.DateTime;
import org.apache.commons.math3.analysis.function.Floor;

import java.sql.Time;
import java.util.*;

import static io.tapdata.entity.utils.JavaTypesToTapTypes.*;

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
                String type = field.getDataType();
                String keyName = field.getName();
                switch (type) {
                    case JAVA_Array:{
                        List<String> list = new ArrayList<>();
                        list.add(UUID.randomUUID().toString());
                        list.add(UUID.randomUUID().toString());
                        record.builder(keyName,list);
                    };break;
                    case JAVA_Binary:{
                        record.builder(keyName,UUID.randomUUID().getLeastSignificantBits());
                    };break;
                    case JAVA_Integer:{
                        Date date = new Date();
                        record.builder(keyName,date.getSeconds());
                    };break;
                    case JAVA_Date:{
                        record.builder(keyName,new Date());
                    };break;
                    case JAVA_Map:{
                        Map<String,Object> map = new HashMap<>();
                        map.put(UUID.randomUUID().toString(),UUID.randomUUID().toString());
                        map.put(UUID.randomUUID().toString(),UUID.randomUUID().toString());
                        record.builder(keyName,map);
                    };break;
                    case JAVA_String:{
                        record.builder(keyName,UUID.randomUUID().toString());
                    };break;
                    case JAVA_BigDecimal:{
                        record.builder(keyName,UUID.randomUUID().toString());
                    }break;
                    case JAVA_Boolean:{
                        record.builder(keyName,Math.random()*10+50>55);
                    }break;
                    case JAVA_Float:{
                        record.builder(keyName, Float.parseFloat(""+(Math.random()*10+50)));
                    }break;
                    case JAVA_Long:{
                        record.builder(keyName,System.nanoTime());
                    }break;
                    case JAVA_Double:{
                        record.builder(keyName,Double.parseDouble(""+(Math.random()*10+50)));
                    }break;
                    default:record.builder(keyName,null);;
                }
            });
            records[i] = record;
        }
        return records;
    }
}
