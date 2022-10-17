package io.tapdata.zoho.service.zoho.schemaLoader;

import cn.hutool.core.date.DateUtil;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ZoHoOffset;
import io.tapdata.zoho.enums.ModuleEnums;
import io.tapdata.zoho.service.connectionMode.ConnectionMode;
import io.tapdata.zoho.service.zoho.loader.OrganizationFieldsOpenApi;
import io.tapdata.zoho.service.zoho.schema.Schemas;
import io.tapdata.zoho.utils.Checker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class OrganizationFieldsSchema implements SchemaLoader {
    private static final String TAG = TickersSchema.class.getSimpleName();
    private OrganizationFieldsOpenApi fieldLoader;
    @Override
    public SchemaLoader configSchema(TapConnectionContext tapConnectionContext) {
        this.fieldLoader = OrganizationFieldsOpenApi.create(tapConnectionContext);
        return this;
    }

    @Override
    public List<TapEvent> rawDataCallbackFilterFunction(Map<String, Object> issueEventData) {
        return null;
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
        List<TapEvent> events = new ArrayList<>();
        TapConnectionContext context = fieldLoader.getContext();
        String modeName = context.getConnectionConfig().getString("connectionMode");
        ConnectionMode connectionMode = ConnectionMode.getInstanceByName(context, modeName);
        if (null == connectionMode){
            throw new CoreException("Connection Mode is not empty or not null.");
        }
        String table = Schemas.Departments.getTableName();
        List<Map<String, Object>> listDepartment = fieldLoader.list(ModuleEnums.TICKETS, null, null);//分页数
        if (Checker.isNotEmpty(listDepartment) && !listDepartment.isEmpty()) {
            for (Map<String, Object> stringObjectMap : listDepartment) {
                Map<String, Object> department = connectionMode.attributeAssignment(stringObjectMap, table);
                if (Checker.isNotEmpty(department) && !department.isEmpty()) {
                    long referenceTime = System.currentTimeMillis();
                    ((ZoHoOffset) offset).getTableUpdateTimeMap().put(table, referenceTime);
                    events.add(TapSimplify.insertRecordEvent(department, table).referenceTime(referenceTime));
                    if (events.size() == batchCount) {
                        consumer.accept(events, offset);
                        events = new ArrayList<>();
                    }
                }
            }
        }
        if (events.size()>0){
            consumer.accept(events, offset);
        }
    }

    @Override
    public long batchCount() throws Throwable {
        return this.fieldLoader.count(ModuleEnums.TASKS);
    }
}
