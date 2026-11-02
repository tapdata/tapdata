package com.tapdata.tm.inspect.dto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiver;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InspectAlarmReceiverContractTest {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void serializesAndReadsInspectCustomReceiverContract() throws Exception {
        InspectDto source = new InspectDto();
        source.setAlarmReceivers(List.of(
                new AlarmReceiver(AlarmReceiverType.USER, "user-id", null),
                new AlarmReceiver(AlarmReceiverType.USER_GROUP, "group-id", null),
                new AlarmReceiver(AlarmReceiverType.EMAIL, null, "ops@example.com")));
        source.setEmailReceivers(List.of("ops@example.com"));

        InspectDto roundTrip = objectMapper.readValue(objectMapper.writeValueAsBytes(source), InspectDto.class);

        assertEquals(source.getAlarmReceivers(), roundTrip.getAlarmReceivers());
        assertEquals(source.getEmailReceivers(), roundTrip.getEmailReceivers());
    }
}
