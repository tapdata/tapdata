package io.tapdata.pdk.core.workflow.engine.driver.task;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class TaskManager {
    private List<Task.TableFilter> tableFilters = new CopyOnWriteArrayList<>();
    public void init(List<Map<String, Object>> tasks) {
        if(tasks == null)
            return;
        for(Map<String, Object> taskMap : tasks) {
            Task task = Task.build(taskMap);
            if(task != null) {
                Task.TableFilter tableFilter = task.getTableFilter();
                if(tableFilter != null && !tableFilters.contains(tableFilter))
                    tableFilters.add(tableFilter);
            }
        }
    }

    public void filterTable(TapTable table, String tag) {
        for(Task.TableFilter filter : tableFilters) {
            CommonUtils.ignoreAnyError(() -> filter.table(table), tag);
        }
    }
}
