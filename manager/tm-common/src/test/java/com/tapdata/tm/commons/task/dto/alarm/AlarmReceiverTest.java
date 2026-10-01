package com.tapdata.tm.commons.task.dto.alarm;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AlarmReceiverTest {

    @Test
    void javaSerializationRoundTrip() throws Exception {
        List<AlarmReceiver> receivers = List.of(
                new AlarmReceiver(AlarmReceiverType.USER, "u1", "a@example.com"),
                new AlarmReceiver(AlarmReceiverType.USER_GROUP, "g1", null),
                new AlarmReceiver(AlarmReceiverType.EMAIL, null, "ops@example.com")
        );

        byte[] bytes;
        try (ByteArrayOutputStream output = new ByteArrayOutputStream();
             ObjectOutputStream objectOutput = new ObjectOutputStream(output)) {
            objectOutput.writeObject(receivers);
            bytes = output.toByteArray();
        }

        List<?> restored;
        try (ObjectInputStream objectInput = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            restored = (List<?>) objectInput.readObject();
        }

        assertEquals(receivers, restored);
    }
}
