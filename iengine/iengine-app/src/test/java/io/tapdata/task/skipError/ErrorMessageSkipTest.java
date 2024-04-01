package io.tapdata.task.skipError;

import com.tapdata.tm.commons.task.dto.ErrorEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class ErrorMessageSkipTest {
    @Test
    void errorMessageSkip(){
        ErrorMessageSkip errorMessageSkip = new ErrorMessageSkip();
        List<ErrorEvent> errorEventList = new ArrayList<>();
        ErrorEvent event = new ErrorEvent("test",null);
        errorEventList.add(event);
        Assertions.assertTrue(errorMessageSkip.match(errorEventList,event));
    }

    @Test
    void errorMessageNotSkip(){
        ErrorMessageSkip errorMessageSkip = new ErrorMessageSkip();
        List<ErrorEvent> errorEventList = new ArrayList<>();
        errorEventList.add(new ErrorEvent("test2",null));
        ErrorEvent event = new ErrorEvent("test",null);
        Assertions.assertFalse(errorMessageSkip.match(errorEventList,event));
    }

    @Test
    void errorMessageErrorEventIsNull(){
        ErrorMessageSkip errorMessageSkip = new ErrorMessageSkip();
        List<ErrorEvent> errorEventList = new ArrayList<>();
        errorEventList.add(new ErrorEvent("test2",null));
        Assertions.assertFalse(errorMessageSkip.match(errorEventList,null));
    }

    @Test
    void errorMessageErrorEventsIsNull(){
        ErrorMessageSkip errorMessageSkip = new ErrorMessageSkip();
        Assertions.assertFalse(errorMessageSkip.match(null,new ErrorEvent("test2",null)));
    }
}
