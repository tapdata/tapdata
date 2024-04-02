package io.tapdata.task.skipError;

import com.tapdata.constant.CollectionUtil;
import com.tapdata.tm.commons.task.dto.ErrorEvent;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class ErrorMessageSkip extends SkipError{
    @Override
    public boolean match(List<ErrorEvent> errorEventList, ErrorEvent errorEvent) {
        if(CollectionUtil.isEmpty(errorEventList) || errorEvent == null) return false;
        String message = errorEvent.getMessage();
        if(StringUtils.isBlank(message))return false;
        for(ErrorEvent event:errorEventList){
            if(event.getMessage().equals(message))return true;
        }
        return false;
    }
}
