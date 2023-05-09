package io.tapdata.aspect;

import com.tapdata.tm.commons.function.ThrowableFunction;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;

import java.util.List;

/**
 * 任务错误数据跳过
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/4/11 11:23 Create
 */
public class SkipErrorDataAspect extends DataNodeAspect<SkipErrorDataAspect> {

    private TapTable tapTable;
    private List<TapRecordEvent> tapRecordEvents;
    private PDKMethodInvoker pdkMethodInvoker;
    private ThrowableFunction<Void, List<TapRecordEvent>, Throwable> writeRecordFunction;

    public SkipErrorDataAspect tapTable(TapTable tableName) {
        this.tapTable = tableName;
        return this;
    }

    public SkipErrorDataAspect tapRecordEvents(List<TapRecordEvent> tapRecordEvents) {
        this.tapRecordEvents = tapRecordEvents;
        return this;
    }

    public SkipErrorDataAspect pdkMethodInvoker(PDKMethodInvoker pdkMethodInvoker) {
        this.pdkMethodInvoker = pdkMethodInvoker;
        return this;
    }

    public SkipErrorDataAspect writeOneFunction(ThrowableFunction<Void, List<TapRecordEvent>, Throwable> writeOneConsumer) {
        this.writeRecordFunction = writeOneConsumer;
        return this;
    }

    public TapTable getTapTable() {
        return tapTable;
    }

    public List<TapRecordEvent> getTapRecordEvents() {
        return tapRecordEvents;
    }

    public PDKMethodInvoker getPdkMethodInvoker() {
        return pdkMethodInvoker;
    }

    public ThrowableFunction<Void, List<TapRecordEvent>, Throwable> getWriteRecordFunction() {
        return writeRecordFunction;
    }
}
