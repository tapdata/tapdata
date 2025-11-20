package io.tapdata.task.skipError;

import com.tapdata.constant.CollectionUtil;
import com.tapdata.tm.commons.task.dto.ErrorEvent;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ErrorMessageSkip extends SkipError{
    @Override
    public boolean match(List<ErrorEvent> errorEventList, ErrorEvent errorEvent) {
        if(CollectionUtils.isEmpty(errorEventList) || errorEvent == null) return false;
        if(StringUtils.isBlank(errorEvent.getMessage()) || StringUtils.isBlank(errorEvent.getCode()))return false;
        for(ErrorEvent event:errorEventList){
            if(event.getCode().equals(errorEvent.getCode()) && removeHashCodeAndTime(event.getMessage()).equals(removeHashCodeAndTime(errorEvent.getMessage())))return true;
        }
        return false;
    }

    protected String removeHashCodeAndTime(String message) {
        if (StringUtils.isBlank(message)) return message;
        Pattern pattern = Pattern.compile("\\w+@([a-fA-F0-9]{1,8})");
        Matcher matcher = pattern.matcher(message);
        String hashCode = "";
        if (matcher.find()) {
            hashCode =  matcher.group(1);
        }
        return message.replace(hashCode, "").replaceAll("(\"tableId\":\"[^\"]*\"),\\s*\"time\":\\d+\\s*,\\s*(\"type\":\\d+)", "$1,$2");
    }
}
