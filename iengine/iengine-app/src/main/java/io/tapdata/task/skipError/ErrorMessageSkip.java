package io.tapdata.task.skipError;

import com.tapdata.constant.CollectionUtil;
import com.tapdata.tm.commons.task.dto.ErrorEvent;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class ErrorMessageSkip extends SkipError{
    @Override
    public boolean match(List<ErrorEvent> errorEventList, ErrorEvent errorEvent) {
        if(CollectionUtils.isEmpty(errorEventList) || errorEvent == null) return false;
        if(StringUtils.isBlank(errorEvent.getMessage()) || StringUtils.isBlank(errorEvent.getCode()))return false;
        for(ErrorEvent event:errorEventList){
            if(event.getCode().equals(errorEvent.getCode()) && event.getMessage().equals(errorEvent.getMessage()))return true;
        }
        return false;
    }
}
