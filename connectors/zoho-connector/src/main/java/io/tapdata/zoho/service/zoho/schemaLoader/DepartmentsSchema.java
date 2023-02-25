package io.tapdata.zoho.service.zoho.schemaLoader;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ZoHoOffset;
import io.tapdata.zoho.service.connectionMode.ConnectionMode;
import io.tapdata.zoho.service.zoho.loader.DepartmentOpenApi;
import io.tapdata.zoho.service.zoho.schema.Schemas;
import io.tapdata.zoho.utils.Checker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class DepartmentsSchema extends Schema implements SchemaLoader {
    private static final String TAG = DepartmentsSchema.class.getSimpleName();
    private DepartmentOpenApi departmentOpenApi;
    @Override
    public SchemaLoader configSchema(TapConnectionContext tapConnectionContext) {
        this.departmentOpenApi = DepartmentOpenApi.create(tapConnectionContext);
        return this;
    }

    @Override
    public void streamRead(Object offsetState, int recordSize, StreamReadConsumer consumer) {

    }

    @Override
    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {
        final List<TapEvent>[] events = new List[]{new ArrayList<>()};
        int pageSize = Math.min(batchCount, departmentOpenApi.MAX_PAGE_LIMIT);
        int fromPageIndex = 0;//从0开始分页
        TapConnectionContext context = departmentOpenApi.getContext();
        String modeName = context.getConnectionConfig().getString("connectionMode");
        ConnectionMode connectionMode = ConnectionMode.getInstanceByName(context, modeName);
        if (null == connectionMode){
            throw new CoreException("Connection Mode is not empty or not null.");
        }
        String table = Schemas.Departments.getTableName();
        while (isAlive()){
            List<Map<String, Object>> listDepartment = departmentOpenApi.list(null, null, fromPageIndex, pageSize, null);//分页数
            if (Checker.isNotEmpty(listDepartment) && !listDepartment.isEmpty()) {
                fromPageIndex += pageSize;
                for (Map<String, Object> stringObjectMap : listDepartment) {
                    if (!isAlive()) break;
                    Map<String, Object> department = connectionMode.attributeAssignment(stringObjectMap, table,departmentOpenApi);
                    if (Checker.isNotEmpty(department) && !department.isEmpty()) {
                        Object modifiedTimeObj = department.get("modifiedTime");
                        long referenceTime = System.currentTimeMillis();
                        if (Checker.isNotEmpty(modifiedTimeObj) && modifiedTimeObj instanceof String) {
                            referenceTime = this.parseZoHoDatetime((String) modifiedTimeObj);
                            if (Checker.isEmpty(offset)) offset = ZoHoOffset.create(new HashMap<>());
                            ((ZoHoOffset) offset).getTableUpdateTimeMap().put(table, referenceTime);
                        }
                        events[0].add(TapSimplify.insertRecordEvent(department, table).referenceTime(referenceTime));
                        if (events[0].size() == batchCount) {
                            consumer.accept(events[0], offset);
                            events[0] = new ArrayList<>();
                        }
                    }
                }
            }else {
                break;
            }
        }
        if (!events[0].isEmpty()){
            consumer.accept(events[0], offset);
        }
    }

    @Override
    public long batchCount() throws Throwable {
        return departmentOpenApi.getDepartmentCount();
    }
}
