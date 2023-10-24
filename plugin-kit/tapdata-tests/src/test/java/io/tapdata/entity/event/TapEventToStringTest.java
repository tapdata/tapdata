package io.tapdata.entity.event;

import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.schema.TapField;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TapEventToStringTest {
    @Test
    public void test() {
        TapNewFieldEvent newFieldEvent = new TapNewFieldEvent();
        newFieldEvent.setPdkId("mysql");
        newFieldEvent.setPdkGroup("io/tapdata");
        newFieldEvent.setPdkVersion("1.10.0");
        newFieldEvent.field(new TapField("aaa", "varchar(30)").comment("this is comment"));
        newFieldEvent.field(new TapField("bbb", "tinyint(8)"));
        System.out.println("newFieldEvent toString == " + newFieldEvent);
        assertNotNull(newFieldEvent.toString());
    }

}
