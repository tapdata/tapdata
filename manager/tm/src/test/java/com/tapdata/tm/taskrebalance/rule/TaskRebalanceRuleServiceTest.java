package com.tapdata.tm.taskrebalance.rule;

import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.task.dto.Milestone;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.taskrebalance.vo.TaskRebalancePreviewVo;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Date;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskRebalanceRuleServiceTest {

    private final TaskRebalanceRuleService ruleService = new TaskRebalanceRuleService();

    private TaskDto newRunningCdcTask(String agentId) {
        TaskDto task = new TaskDto();
        task.setId(new ObjectId());
        task.setName("t-" + agentId);
        task.setStatus(TaskDto.STATUS_RUNNING);
        task.setAgentId(agentId);
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        task.setType(ParentTaskDto.TYPE_CDC);
        return task;
    }

    @Nested
    @DisplayName("evaluate movability")
    class Evaluate {
        @Test
        void cdcRunningTaskIsMovable() {
            TaskDto task = newRunningCdcTask("a1");
            TaskRebalancePreviewVo.TaskPreview item = ruleService.evaluate(task, Set.of("a1", "a2"));
            assertTrue(item.getMovable());
            assertEquals("OK", item.getSchedulableStatus());
            assertNull(item.getReason());
            assertEquals("a1", item.getSourceAgentId());
            assertEquals("a1", item.getTargetAgentId());
        }

        @Test
        void taskOnOfflineAgentIsNotMovable() {
            TaskDto task = newRunningCdcTask("offline");
            TaskRebalancePreviewVo.TaskPreview item = ruleService.evaluate(task, Set.of("a1", "a2"));
            assertFalse(item.getMovable());
            assertEquals("AGENT_OFFLINE", item.getSchedulableStatus());
            assertNotNull(item.getReason());
        }

        @Test
        void nonRunningStatusIsNotMovable() {
            TaskDto task = newRunningCdcTask("a1");
            task.setStatus(TaskDto.STATUS_SCHEDULING);
            TaskRebalancePreviewVo.TaskPreview item = ruleService.evaluate(task, Set.of("a1"));
            assertFalse(item.getMovable());
            assertEquals("STATUS_ERROR", item.getSchedulableStatus());
        }

        @Test
        void manuallySpecifiedAgentIsNotMovable() {
            TaskDto task = newRunningCdcTask("a1");
            task.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());
            TaskRebalancePreviewVo.TaskPreview item = ruleService.evaluate(task, Set.of("a1"));
            assertFalse(item.getMovable());
            assertEquals("MANUAL_AGENT", item.getSchedulableStatus());
        }

        @Test
        void manuallySpecifiedAgentGroupIsNotMovable() {
            TaskDto task = newRunningCdcTask("a1");
            task.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
            TaskRebalancePreviewVo.TaskPreview item = ruleService.evaluate(task, Set.of("a1"));
            assertFalse(item.getMovable());
            assertEquals("MANUAL_AGENT", item.getSchedulableStatus());
        }

        @Test
        void initialSyncCdcWithoutIncrementalNotMovable() {
            TaskDto task = newRunningCdcTask("a1");
            task.setType(ParentTaskDto.TYPE_INITIAL_SYNC_CDC);
            task.setMilestones(Collections.emptyList());
            TaskRebalancePreviewVo.TaskPreview item = ruleService.evaluate(task, Set.of("a1"));
            assertFalse(item.getMovable());
            assertEquals("INCREMENTAL_NOT_STARTED", item.getSchedulableStatus());
        }

        @Test
        void initialSyncCdcWithCdcMilestoneIsMovable() {
            TaskDto task = newRunningCdcTask("a1");
            task.setType(ParentTaskDto.TYPE_INITIAL_SYNC_CDC);
            Milestone m = new Milestone();
            m.setCode("READ_CDC_EVENT");
            m.setStart(System.currentTimeMillis());
            task.setMilestones(Collections.singletonList(m));
            TaskRebalancePreviewVo.TaskPreview item = ruleService.evaluate(task, Set.of("a1"));
            assertTrue(item.getMovable());
        }
    }

    @Nested
    @DisplayName("priority ordering")
    class Priority {
        @Test
        void higherSyncTypePriorityGetsHigherScore() {
            TaskDto heartbeat = newRunningCdcTask("a1");
            heartbeat.setSyncType(TaskDto.SYNC_TYPE_CONN_HEARTBEAT);
            TaskDto sync = newRunningCdcTask("a1");
            sync.setSyncType(TaskDto.SYNC_TYPE_SYNC);
            TaskDto logCollector = newRunningCdcTask("a1");
            logCollector.setSyncType(TaskDto.SYNC_TYPE_LOG_COLLECTOR);

            TaskRebalancePreviewVo.TaskPreview h = ruleService.evaluate(heartbeat, Set.of("a1"));
            TaskRebalancePreviewVo.TaskPreview s = ruleService.evaluate(sync, Set.of("a1"));
            TaskRebalancePreviewVo.TaskPreview l = ruleService.evaluate(logCollector, Set.of("a1"));

            assertTrue(h.getPriorityScore() > s.getPriorityScore());
            assertTrue(s.getPriorityScore() > l.getPriorityScore());
            assertEquals(-1, ruleService.compareMovePriority(h, s));
            assertEquals(1, ruleService.compareMovePriority(l, s));
        }

        @Test
        void scoreItemsAreExposedForFrontend() {
            TaskDto task = newRunningCdcTask("a1");
            TaskRebalancePreviewVo.TaskPreview item = ruleService.evaluate(task, Set.of("a1"));
            assertNotNull(item.getPriorityScoreItems());
            assertTrue(item.getPriorityScoreItems().containsKey("syncType"));
            assertTrue(item.getPriorityScoreItems().containsKey("nodeCount"));
            assertTrue(item.getPriorityScoreItems().containsKey("startTime"));
        }

        @Test
        void startTimePriorityUsesPreviewListOrder() {
            TaskDto earlier = newRunningCdcTask("a1");
            earlier.setStartTime(new Date(1000L));
            TaskDto later = newRunningCdcTask("a1");
            later.setStartTime(new Date(2000L));

            TaskRebalancePreviewVo.TaskPreview high = ruleService.evaluate(earlier, Set.of("a1"), 2);
            TaskRebalancePreviewVo.TaskPreview low = ruleService.evaluate(later, Set.of("a1"), 1);

            assertEquals(2, high.getPriorityScoreItems().get("startTime"));
            assertEquals(1, low.getPriorityScoreItems().get("startTime"));
            assertTrue(high.getPriorityScore() > low.getPriorityScore());
        }

        @Test
        void nullStartTimeDoesNotGetStartTimePriority() {
            TaskDto task = newRunningCdcTask("a1");

            TaskRebalancePreviewVo.TaskPreview item = ruleService.evaluate(task, Set.of("a1"), 10);

            assertEquals(0, item.getPriorityScoreItems().get("startTime"));
        }
    }

    @Nested
    @DisplayName("execution-time movability")
    class ExecutionMovability {
        @Test
        void runningCdcIsMovableAtExecution() {
            assertTrue(ruleService.isMovableAtExecution(newRunningCdcTask("a1")));
        }

        @Test
        void stoppedTaskIsNotMovableAtExecution() {
            TaskDto task = newRunningCdcTask("a1");
            task.setStatus(TaskDto.STATUS_STOP);
            assertFalse(ruleService.isMovableAtExecution(task));
        }

        @Test
        void nullTaskIsNotMovable() {
            assertFalse(ruleService.isMovableAtExecution(null));
        }
    }
}
