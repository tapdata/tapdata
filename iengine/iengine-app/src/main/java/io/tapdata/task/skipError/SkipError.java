package io.tapdata.task.skipError;

import com.tapdata.tm.commons.task.dto.ErrorEvent;

import java.util.List;

public abstract class SkipError {
    public abstract boolean match(List<ErrorEvent> errorEventList,ErrorEvent errorEvent);
}
