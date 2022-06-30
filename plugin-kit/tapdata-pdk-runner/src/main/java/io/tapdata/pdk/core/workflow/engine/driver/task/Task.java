package io.tapdata.pdk.core.workflow.engine.driver.task;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.mapping.type.TapMapping;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.Switch64;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public abstract class Task {
    private static final String TAG = Task.class.getSimpleName();

    public static final int ON_TABLE = 1;
    public static final int ON_RECORD = 2;

    protected TableFilter tableFilter;
    public Task supportTableFilter(TableFilter tableFilter) {
        this.tableFilter = tableFilter;
        return this;
    }

    public interface TableFilter {
        void table(TapTable table);
    }

    protected String type;
    private static final Map<String, Class<?>> classCacheMap = new ConcurrentHashMap<>();

    public static Task build(DataMap info) {
        return Task.build(((Map<String, Object>)info));
    }
    public static Task build(Map<String, Object> info) {
        String type = null;
        Object typeObj = info.get("type");
        if(typeObj instanceof String)
            type = (String) typeObj;

        if(type == null)
            return null;

        String taskClassStr = Task.class.getPackage().getName() + "." + type + "Task";
        Class<?> taskClass = classCacheMap.get(taskClassStr);
        if(taskClass == null) {
            synchronized (classCacheMap) {
                taskClass = classCacheMap.get(taskClassStr);
                if(taskClass == null) {
                    try {
                        taskClass = Class.forName(taskClassStr);
                        if(!TapMapping.class.isAssignableFrom(taskClass)) {
                            return null;
                        }
                        classCacheMap.put(taskClassStr, taskClass);
                    } catch (Throwable e) {
                        e.printStackTrace();
                        TapLogger.error(TAG, "taskClassStr {} for type {} info {}, class forName failed, {}", taskClassStr, type, info, e.getMessage());
                        return null;
                    }
                }
            }
        }

        try {
            Task task = (Task) taskClass.getConstructor().newInstance();
            task.type = type;
            task.from(info);
            return task;
        } catch (Throwable e) {
            e.printStackTrace();
            TapLogger.error(TAG, "taskClass {} for type {} info {}, initiate or from failed, {}", taskClass, type, info, e.getMessage());
        }
        return null;
    }

    protected abstract void from(Map<String, Object> info);

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public TableFilter getTableFilter() {
        return tableFilter;
    }
}
