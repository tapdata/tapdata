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
        ErrorEvent event = new ErrorEvent("test","110",null);
        errorEventList.add(event);
        Assertions.assertTrue(errorMessageSkip.match(errorEventList,event));
    }

    @Test
    void errorMessageSkipMessageIsNull(){
        ErrorMessageSkip errorMessageSkip = new ErrorMessageSkip();
        List<ErrorEvent> errorEventList = new ArrayList<>();
        ErrorEvent event = new ErrorEvent(null,"110",null);
        errorEventList.add(event);
        Assertions.assertFalse(errorMessageSkip.match(errorEventList,event));
    }

    @Test
    void errorMessageNotSkip(){
        ErrorMessageSkip errorMessageSkip = new ErrorMessageSkip();
        List<ErrorEvent> errorEventList = new ArrayList<>();
        errorEventList.add(new ErrorEvent("test2","110",null));
        ErrorEvent event = new ErrorEvent("test","110",null);
        Assertions.assertFalse(errorMessageSkip.match(errorEventList,event));
    }

    @Test
    void errorMessageErrorEventIsNull(){
        ErrorMessageSkip errorMessageSkip = new ErrorMessageSkip();
        List<ErrorEvent> errorEventList = new ArrayList<>();
        errorEventList.add(new ErrorEvent("test2","110",null));
        Assertions.assertFalse(errorMessageSkip.match(errorEventList,null));
    }

    @Test
    void errorMessageErrorEventsIsNull(){
        ErrorMessageSkip errorMessageSkip = new ErrorMessageSkip();
        Assertions.assertFalse(errorMessageSkip.match(null,new ErrorEvent("test2","110",null)));
    }

    @Test
    void errorMessageErrorEventMessageIsNull(){
        ErrorMessageSkip errorMessageSkip = new ErrorMessageSkip();
        List<ErrorEvent> errorEventList = new ArrayList<>();
        errorEventList.add(new ErrorEvent(null,"110",null));
        Assertions.assertFalse(errorMessageSkip.match(errorEventList,new ErrorEvent(null,"110",null)));
    }

    @Test
    void errorMessageSkipWithRemoveHashCodeAndTime(){
        ErrorMessageSkip errorMessageSkip = new ErrorMessageSkip();
        List<ErrorEvent> errorEventList = new ArrayList<>();
        String message1 = "Data to be written: io.DGG.entity.event.dml.TapInsertRecordEvent@25f2a0e1: {\"after\":{\"id\":7,\"name\":\"777\"},\"containsIllegalDate\":false,\"tableId\":\"AA_1105\",\"time\":1763626901843,\"type\":300}";
        ErrorEvent event1 = new ErrorEvent(message1,"110",null);
        errorEventList.add(event1);
        String message2 = "Data to be written: io.DGG.entity.event.dml.TapInsertRecordEvent@701fe650: {\"after\":{\"id\":7,\"name\":\"777\"},\"containsIllegalDate\":false,\"tableId\":\"AA_1105\",\"time\":1763626886938,\"type\":300}";
        ErrorEvent event = new ErrorEvent(message2,"110",null);
        Assertions.assertTrue(errorMessageSkip.match(errorEventList,event));
    }
}
