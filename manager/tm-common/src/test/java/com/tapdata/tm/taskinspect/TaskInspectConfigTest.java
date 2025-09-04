package com.tapdata.tm.taskinspect;

import com.tapdata.tm.taskinspect.cons.CustomCdcTypeEnum;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Update;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/4/14 14:11 Create
 */
class TaskInspectConfigTest {

    @Test
    void testInit() {
        TaskInspectConfig config = new TaskInspectConfig().init(-1);
        Assertions.assertNotNull(config.getEnable());
        Assertions.assertNotNull(config.getMode());
        Assertions.assertNotNull(config.getIntelligent());
        Assertions.assertNotNull(config.getCustom());
    }

    @Test
    void testInitZero() {
        TaskInspectConfig config = new TaskInspectConfig().init(0);
        Assertions.assertNotNull(config.getCustom());
        Assertions.assertNull(config.getCustom().getCdc());
    }

    @Test
    void testInitMulti() {
        TaskInspectConfig config = new TaskInspectConfig().init(1);
        Assertions.assertNotNull(config.getCustom());
        Assertions.assertNotNull(config.getCustom().getCdc());
        Assertions.assertNull(config.getCustom().getCdc().getType());
    }

    @Nested
    class QueueCapacityTest {

        @Test
        void testDefault() {
            int expectedQueueCapacity = 1000;

            TaskInspectConfig config = TaskInspectConfig.createClose();

            Assertions.assertEquals(expectedQueueCapacity, config.getQueueCapacity());
        }

        @Test
        void testDefaultOfCustom() {
            TaskInspectConfig config = new TaskInspectConfig();
            config.setMode(TaskInspectMode.CUSTOM);
            config.init(-1);

            int expectedQueueCapacity = config.getCustom().getCdc().getSample().getLimit() * 3;

            Assertions.assertNotNull(config.getCustom());
            Assertions.assertNotNull(config.getCustom().getCdc());
            Assertions.assertEquals(CustomCdcTypeEnum.SAMPLE, config.getCustom().getCdc().getType());
            Assertions.assertEquals(expectedQueueCapacity, config.getQueueCapacity());
        }

        @Test
        void testWithSet() {
            int expectedQueueCapacity = 200;

            TaskInspectConfig config = TaskInspectConfig.createClose();
            config.setQueueCapacity(expectedQueueCapacity);

            Assertions.assertEquals(expectedQueueCapacity, config.getQueueCapacity());
        }
    }

    @Nested
    class toUpdateUnsetWithNullValueTest {
        @Test
        void testEmpty() {
            TaskInspectConfig config = new TaskInspectConfig();

            Update update = config.toUpdateUnsetWithNullValue();

            Assertions.assertNotNull(update);
            Assertions.assertNotNull(update.getUpdateObject());
            Assertions.assertNull(update.getUpdateObject().get("$set", Document.class));

            Document $unset = update.getUpdateObject().get("$unset", Document.class);
            Assertions.assertNotNull($unset);
            Assertions.assertNotEquals(0, $unset.size());
        }

        @Test
        void testUnsetWithNull() {
            TaskInspectConfig config = new TaskInspectConfig();
            config.setEnable(true);

            Update update = config.toUpdateUnsetWithNullValue();

            Assertions.assertNotNull(update);
            Assertions.assertNotNull(update.getUpdateObject());
            Document $set = update.getUpdateObject().get("$set", Document.class);
            Assertions.assertNotNull($set);
            Assertions.assertTrue($set.containsKey(TaskInspectConfig.FIELD_ENABLE));

            Document $unset = update.getUpdateObject().get("$unset", Document.class);
            Assertions.assertNotNull($unset);
            Assertions.assertTrue($unset.containsKey(TaskInspectConfig.FIELD_MODE));
        }
    }

}
