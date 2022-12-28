package io.tapdata.common.postman.pageStage;

import io.tapdata.common.postman.entity.ApiMap;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.common.support.APIInvoker;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

public class TapPage {
    private AtomicBoolean task;
    public Boolean isAlive(){
        return task.get();
    }
    public TapPage task(AtomicBoolean task){
        this.task = task;
        return this;
    }

    private Object offset;
    private int batchCount;
    private String tableName;
    private BiConsumer<List<TapEvent>, Object> consumer;
    private APIInvoker invoker;
    private ApiMap.ApiEntity api;
    private Map<String,Object> apiParam;
    public static TapPage create(){
        return new TapPage();
    }
    public TapPage offset(Object offset){
        this.offset = offset;
        return this;
    }
    public TapPage apiParam(Map<String,Object> apiParam){
        this.apiParam = apiParam;
        return this;
    }
    public Map<String,Object> apiParam(){
        return this.apiParam;
    }
    public TapPage batchCount(int batchCount){
        this.batchCount = batchCount;
        return this;
    }
    public TapPage consumer(BiConsumer<List<TapEvent>, Object> consumer){
        this.consumer = consumer;
        return this;
    }
    public TapPage invoker(APIInvoker invoker){
        this.invoker = invoker;
        return this;
    }
    public TapPage api(ApiMap.ApiEntity api){
        this.api = api;
        return this;
    }
    public TapPage tableName(String tableName){
        this.tableName = tableName;
        return this;
    }
    public Object offset(){
        return this.offset;
    }
    public int batchCount(){
        return this.batchCount;
    }
    public BiConsumer<List<TapEvent>, Object> consumer(){
        return this.consumer;
    }
    public APIInvoker invoker(){
        return this.invoker;
    }
    public ApiMap.ApiEntity api(){
        return this.api;
    }
    public String tableName(){
        return this.tableName;
    }
}
