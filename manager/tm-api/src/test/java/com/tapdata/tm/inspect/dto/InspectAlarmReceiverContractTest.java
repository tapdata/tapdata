package com.tapdata.tm.inspect.dto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.alarm.dto.InspectReceiverPreviewRequest;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiver;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

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
        source.setUseSystemDefaultReceivers(true);

        byte[] response = objectMapper.writeValueAsBytes(source);
        InspectDto roundTrip = objectMapper.readValue(response, InspectDto.class);

        assertEquals(source.getAlarmReceivers(), roundTrip.getAlarmReceivers());
        assertFalse(objectMapper.readTree(response).has("emailReceivers"));
        assertFalse(objectMapper.readTree(response).has("useSystemDefaultReceivers"));
        InspectDto input = objectMapper.readValue("{\"emailReceivers\":[\"ops@example.com\"]}", InspectDto.class);
        assertEquals(List.of("ops@example.com"), input.getEmailReceivers());
        InspectDto reset = objectMapper.readValue("{\"useSystemDefaultReceivers\":true}", InspectDto.class);
        assertEquals(Boolean.TRUE, reset.getUseSystemDefaultReceivers());
    }

    @Test
    void receiverPreviewRequestPreservesDraftAndCustomEmptyModes() throws Exception {
        InspectReceiverPreviewRequest customEmpty = new InspectReceiverPreviewRequest();
        customEmpty.setInspectId("inspect-id");
        customEmpty.setFlowId("flow-id");
        customEmpty.setAlarmReceivers(List.of());
        customEmpty.setEmailReceivers(List.of());

        InspectReceiverPreviewRequest roundTrip = objectMapper.readValue(
                objectMapper.writeValueAsBytes(customEmpty), InspectReceiverPreviewRequest.class);

        assertEquals("inspect-id", roundTrip.getInspectId());
        assertEquals("flow-id", roundTrip.getFlowId());
        assertEquals(List.of(), roundTrip.getAlarmReceivers());
        assertEquals(List.of(), roundTrip.getEmailReceivers());
    }
}
