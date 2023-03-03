package io.tapdata.zoho.service.zoho.schemaLoader;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ZoHoOffset;
import io.tapdata.zoho.service.connectionMode.ConnectionMode;
import io.tapdata.zoho.service.zoho.loader.ContractsOpenApi;
import io.tapdata.zoho.service.zoho.schema.Schemas;
import io.tapdata.zoho.utils.Checker;

import java.util.*;
import java.util.function.BiConsumer;

public class ContractsSchema extends Schema implements SchemaLoader {
    private static final String TAG = ContractsSchema.class.getSimpleName();
    ContractsOpenApi contractsOpenApi;
    @Override
    public SchemaLoader configSchema(TapConnectionContext tapConnectionContext) {
        this.contractsOpenApi = ContractsOpenApi.create(tapConnectionContext);
        return this;
    }

/**
 public List<TapEvent> rawDataCallbackFilterFunction(Map<String, Object> issueEventData) {
 if (Checker.isEmpty(issueEventData)){
 TapLogger.debug(TAG,"WebHook of ZoHo patch body is empty, Data callback has been over.");
 return null;
 }
 Object listObj = issueEventData.get("array");
 if (Checker.isEmpty(listObj) || !(listObj instanceof List)){
 TapLogger.debug(TAG,"WebHook of ZoHo patch body is empty, Data callback has been over.");
 return null;
 }
 List<Map<String,Object>> dataEventList = (List<Map<String, Object>>)listObj;
 final List<TapEvent>[] events = new List[]{new ArrayList<>()};
 //@TODO BiConsumer<List<TapEvent>, Object> consumer;
 //@TODO 获取筛选条件
 ContextConfig contextConfig = contractsOpenApi.veryContextConfigAndNodeConfig();
 TapConnectionContext context = contractsOpenApi.getContext();
 String modeName = contextConfig.connectionMode();
 ConnectionMode instance = ConnectionMode.getInstanceByName(context, modeName);
 if (null == instance){
 throw new CoreException("Connection Mode must be not empty or not null.");
 }
 dataEventList.forEach(eventMap->{
 EventBaseEntity instanceByEventType = EventBaseEntity.getInstanceByEventType(eventMap);
 if (Checker.isEmpty(instanceByEventType)){
 TapLogger.debug(TAG,"An event type with unknown origin was found and cannot be processed .");
 return;
 }
 events[0].add(instanceByEventType.outputTapEvent("Tickets",instance));
 });
 return events[0];
 }
 * */

    @Override
    public void streamRead(Object offsetState, int recordSize, StreamReadConsumer consumer) {

    }

    @Override
    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {
        this.read(batchCount, offset, consumer, Boolean.TRUE);
    }

    @Override
    public long batchCount() throws Throwable {
        return 0;
    }

    public void read(int readSize, Object offsetState, BiConsumer<List<TapEvent>, Object> consumer,boolean isBatchRead ){
        final List<TapEvent>[] events = new List[]{new ArrayList<>()};
        int pageSize = Math.min(readSize, ContractsOpenApi.MAX_PAGE_LIMIT);
        int fromPageIndex = 1;//从第几个工单开始分页
        TapConnectionContext context = this.contractsOpenApi.getContext();
        String modeName = context.getConnectionConfig().getString("connectionMode");
        ConnectionMode connectionMode = ConnectionMode.getInstanceByName(context, modeName);
        if (null == connectionMode){
            throw new CoreException("Connection Mode is not empty or not null.");
        }
        String tableName =  Schemas.Products.getTableName();
        if (Checker.isEmpty(offsetState)) offsetState = ZoHoOffset.create(new HashMap<>());
        final Object offset = offsetState;
        final String sortBy = isBatchRead?"createdTime":"modifiedTime";
        while (isAlive()){
            List<Map<String, Object>> list = contractsOpenApi.page(fromPageIndex, pageSize,sortBy);
            if (Checker.isEmpty(list) || list.isEmpty()) break;
            fromPageIndex += pageSize;
            list.stream().filter(Objects::nonNull).forEach(product->{
                if (!isAlive()) return;
                Map<String, Object> oneProduct = connectionMode.attributeAssignment(product,tableName,contractsOpenApi);
                if (Checker.isEmpty(oneProduct) || oneProduct.isEmpty()) return;
                Object modifiedTimeObj = oneProduct.get(sortBy);
                long referenceTime = System.currentTimeMillis();
                if (Checker.isNotEmpty(modifiedTimeObj) && modifiedTimeObj instanceof String) {
                    referenceTime = this.parseZoHoDatetime((String) modifiedTimeObj);
                    ((ZoHoOffset) offset).getTableUpdateTimeMap().put(tableName, referenceTime);
                }
                events[0].add(( TapSimplify.insertRecordEvent(oneProduct, tableName).referenceTime(referenceTime) ));
                if (events[0].size() != readSize) return;
                consumer.accept(events[0], offset);
                events[0] = new ArrayList<>();
            });
        }
        if (events[0].isEmpty()) return;
        consumer.accept(events[0], offsetState);
    }
}
