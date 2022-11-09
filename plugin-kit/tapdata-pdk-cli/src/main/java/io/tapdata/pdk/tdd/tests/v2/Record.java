package io.tapdata.pdk.tdd.tests.v2;

import java.util.HashMap;
import java.util.UUID;

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
}
