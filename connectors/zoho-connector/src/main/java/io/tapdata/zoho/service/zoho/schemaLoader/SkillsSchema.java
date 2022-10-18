package io.tapdata.zoho.service.zoho.schemaLoader;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ZoHoOffset;
import io.tapdata.zoho.service.connectionMode.ConnectionMode;
import io.tapdata.zoho.service.zoho.loader.ProductsOpenApi;
import io.tapdata.zoho.service.zoho.loader.SkillsOpenApi;
import io.tapdata.zoho.service.zoho.schema.Schemas;
import io.tapdata.zoho.utils.Checker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class SkillsSchema implements SchemaLoader {

    private static final String TAG = SkillsSchema.class.getSimpleName();
    SkillsOpenApi skillsOpenApi;
    @Override
    public SchemaLoader configSchema(TapConnectionContext tapConnectionContext) {
        this.skillsOpenApi = SkillsOpenApi.create(tapConnectionContext);
        return this;
    }

    @Override
    public void streamRead(Object offsetState, int recordSize, StreamReadConsumer consumer) {

    }

    @Override
    public Object timestampToStreamOffset(Long time) {
        return null;
    }

    @Override
    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {
        this.read(batchCount,offset,consumer,Boolean.TRUE);
    }

    @Override
    public long batchCount() throws Throwable {
        return 0;
    }

    public void read(int readSize, Object offsetState, BiConsumer<List<TapEvent>, Object> consumer,boolean isBatchRead ){
        final List<TapEvent>[] events = new List[]{new ArrayList<>()};
        int pageSize = Math.min(readSize, SkillsOpenApi.MAX_PAGE_LIMIT);
        int fromPageIndex = 1;//从第几个工单开始分页
        TapConnectionContext context = this.skillsOpenApi.getContext();
        String modeName = context.getConnectionConfig().getString("connectionMode");
        ConnectionMode connectionMode = ConnectionMode.getInstanceByName(context, modeName);
        if (null == connectionMode){
            throw new CoreException("Connection Mode is not empty or not null.");
        }
        String tableName =  Schemas.Products.getTableName();
        //@TODO 获取departmentID
        String departmentId = "";
        while (true){
            List<Map<String, Object>> list = skillsOpenApi.page(departmentId,fromPageIndex, pageSize);
            if (Checker.isEmpty(list) || list.isEmpty()) break;
            fromPageIndex += pageSize;
            list.stream().forEach(product->{
                Map<String, Object> oneProduct = connectionMode.attributeAssignment(product,tableName,skillsOpenApi);
                if (Checker.isEmpty(oneProduct) || oneProduct.isEmpty()) return;
                Object modifiedTimeObj = oneProduct.get("modifiedTime");
                long referenceTime = System.currentTimeMillis();
                if (Checker.isNotEmpty(modifiedTimeObj) && modifiedTimeObj instanceof String) {
                    referenceTime = this.parseZoHoDatetime((String) modifiedTimeObj);
                    ((ZoHoOffset) offsetState).getTableUpdateTimeMap().put(tableName, referenceTime);
                }
                events[0].add(( TapSimplify.insertRecordEvent(oneProduct, tableName).referenceTime(referenceTime) ));
                if (events[0].size() != readSize) return;
                consumer.accept(events[0], offsetState);
                events[0] = new ArrayList<>();
            });
        }
        if (events[0].size() <= 0) return;
        consumer.accept(events[0], offsetState);
    }
}
