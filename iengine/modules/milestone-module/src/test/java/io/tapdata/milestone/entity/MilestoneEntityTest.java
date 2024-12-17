package io.tapdata.milestone.entity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/11/1 11:46
 */
public class MilestoneEntityTest {

    @Test
    void testSetOfMap() {

        MilestoneEntity milestoneEntity = new MilestoneEntity();

        Assertions.assertDoesNotThrow(() -> {
            Map<String, Object> map = new HashMap<>();
            milestoneEntity.setOfMap(map);

            map.put("retrying", null);
            map.put("retryMetadata", null);
            map.put("retryOp", null);
            milestoneEntity.setOfMap(map);

            map.put("retrying", true);
            map.put("retryTimes", 1L);
            map.put("startRetryTs", 1L);
            map.put("endRetryTs", 1L);
            map.put("nextRetryTs", 1L);
            map.put("totalOfRetries", 1L);
            map.put("retryOp", "test");
            map.put("errorCode", "test");
            map.put("retryMetadata", new HashMap<>());
            milestoneEntity.setOfMap(map);

            Assertions.assertEquals(true, milestoneEntity.getRetrying());
            Assertions.assertEquals(1L, milestoneEntity.getRetryTimes());
            Assertions.assertEquals(1L, milestoneEntity.getStartRetryTs());
            Assertions.assertEquals(1L, milestoneEntity.getEndRetryTs());
            Assertions.assertEquals(1L, milestoneEntity.getNextRetryTs());
            Assertions.assertEquals(1L, milestoneEntity.getTotalOfRetries());
            Assertions.assertEquals("test", milestoneEntity.getRetryOp());
            Assertions.assertEquals("test", milestoneEntity.getErrorCode());
            Assertions.assertInstanceOf(HashMap.class, milestoneEntity.getRetryMetadata());

            MilestoneEntity entity = MilestoneEntity.valueOf(map);
            Assertions.assertNotNull(entity);
        });

    }

}
