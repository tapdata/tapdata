package io.tapdata.observable.logging.appender;

import com.tapdata.tm.commons.schema.MonitoringLogsDto;

import java.util.List;

/**
 * @author GavinXiao
 * @description JSProcessNodeAppender create by Gavin
 * @create 2023/5/11 19:00
 **/
public class JSProcessNodeAppender extends BaseTaskAppender<MonitoringLogsDto> {

    private JSProcessNodeAppender(String taskId) {
        super(taskId);
    }

    public static JSProcessNodeAppender create(String taskId) {
        return new JSProcessNodeAppender(taskId);
    }

    @Override
    public void append(MonitoringLogsDto log) {

    }

    @Override
    public void append(List<MonitoringLogsDto> logs) {

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }
}
