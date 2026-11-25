package io.tapdata.flow.engine.V2.throttle;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataShareLogEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.throttle.LogPositionStatus;
import com.tapdata.tm.commons.throttle.TaskThrottlePolicyDto;
import com.tapdata.tm.commons.throttle.TaskThrottleTelemetrySample;
import com.tapdata.tm.commons.throttle.ThrottleDirection;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import com.tapdata.tm.commons.throttle.ThrottlePhaseLimit;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastSourcePdkDataNode;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastTargetPdkShareCDCNode;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcRetentionListener;
import io.tapdata.node.pdk.processor.TapProcessorNode;
import io.tapdata.observable.metric.throttle.MonotonicClock;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ShareCdcEngineThrottleIntegrationTest {

    private TaskThrottleRuntimeRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new TaskThrottleRuntimeRegistry(
                new AgentMemoryBudgetPool(10_000_000L, 50_000L, 5_000L, 0L, MonotonicClock.SYSTEM),
                MonotonicClock.SYSTEM
        );
        registry.setAgentId("agent-share-1");
        TaskThrottleRuntimeRegistry.setInstanceForTest(registry);
    }

    @AfterEach
    void tearDown() {
        registry.resetForTest();
        TaskThrottleRuntimeRegistry.setInstanceForTest(new TaskThrottleRuntimeRegistry());
    }

    @Test
    void targetShareCdcNodePassesThroughUntouchedWithoutRuntimeAndSlicesWhenEnabled() {
        ObjectId taskId = new ObjectId();
        TaskDto taskDto = new TaskDto();
        taskDto.setId(taskId);
        taskDto.setSyncType("logCollector");
        taskDto.setType("cdc");
        TableNode node = new TableNode();
        node.setId("share-tgt-1");
        DataProcessorContext ctx = DataProcessorContext.newBuilder()
                .withTaskDto(taskDto)
                .withNode(node)
                .build();

        TapProcessorNode mockDelegate = mock(TapProcessorNode.class);
        HazelcastTargetPdkShareCDCNode targetNode = new HazelcastTargetPdkShareCDCNode(ctx, mockDelegate) {
        };
        ReflectionTestUtils.setField(targetNode, "running", new AtomicBoolean(true));
        ExternalStorageDto ext = new ExternalStorageDto();
        ext.setId(new ObjectId());
        ReflectionTestUtils.setField(targetNode, "externalStorageDto", ext);

        List<TapdataShareLogEvent> batch = buildShareLogBatch("conn-1", "orders", 5);

        // No runtime registered -> zero interference, passed directly in one batch
        ReflectionTestUtils.invokeMethod(targetNode, "processShareLog", batch);
        verify(mockDelegate, times(1)).processShareLog(batch);

        // Register runtime with QPS limit (normalBurst=2 for 20 events/sec -> 5 events split into slices)
        reset(mockDelegate);
        Map<ThrottlePhase, ThrottlePhaseLimit> limits = new EnumMap<>(ThrottlePhase.class);
        limits.put(ThrottlePhase.CDC, ThrottlePhaseLimit.builder()
                .enabled(true)
                .eventsPerSec(20L)
                .bytesPerSec(50_000_000L)
                .build());
        TaskThrottlePolicyDto policy = TaskThrottlePolicyDto.builder()
                .tenantId("t1")
                .taskKind("logCollector")
                .taskId(taskId.toHexString())
                .desiredVersion(1L)
                .phases(limits)
                .build();
        TaskThrottleRuntime runtime = registry.openRuntime(
                new TaskRunKey("t1", "logCollector", taskId.toHexString(), 1L), policy, true);

        List<Integer> observedIds = new ArrayList<>();
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            List<TapdataShareLogEvent> chunk = inv.getArgument(0);
            for (TapdataShareLogEvent ev : chunk) {
                TapInsertRecordEvent rec = (TapInsertRecordEvent) ev.getTapEvent();
                observedIds.add((Integer) rec.getAfter().get("id"));
            }
            return null;
        }).when(mockDelegate).processShareLog(anyList());

        ReflectionTestUtils.invokeMethod(targetNode, "processShareLog", batch);

        // Order strictly preserved & metered under CDC OUTPUT
        assertEquals(List.of(0, 1, 2, 3, 4), observedIds);
        verify(mockDelegate, atLeast(2)).processShareLog(anyList());
        List<TaskThrottleTelemetrySample> samples = runtime.snapshotAndRollTelemetry();
        assertTrue(samples.stream().anyMatch(s ->
                s.getPhase() == ThrottlePhase.CDC
                        && s.getDirection() == ThrottleDirection.OUTPUT
                        && s.getAdmittedEvents() == 5L));
    }

    @Test
    void multiSourceTableNormalizerIsolatesConnectionsAndRetentionListenerDrivesMonitor() {
        TapInsertRecordEvent connA = TapInsertRecordEvent.create().init().table("orders").after(Map.of("id", 1));
        connA.addInfo(TapdataEvent.CONNECTION_ID_INFO_KEY, "conn-A");
        TapUpdateRecordEvent connB = TapUpdateRecordEvent.create().init().table("orders").before(Map.of("id", 1)).after(Map.of("id", 2));
        connB.addInfo(TapdataEvent.CONNECTION_ID_INFO_KEY, "conn-B");
        TapInsertRecordEvent plain = TapInsertRecordEvent.create().init().table("orders").after(Map.of("id", 3));

        assertEquals("conn-A:orders", HazelcastSourcePdkDataNode.throttleTableNormalizer(connA));
        assertEquals("conn-B:orders", HazelcastSourcePdkDataNode.throttleTableNormalizer(connB));
        assertEquals("orders", HazelcastSourcePdkDataNode.throttleTableNormalizer(plain));

        String taskId = new ObjectId().toHexString();
        TaskThrottleRuntime runtime = registry.openRuntime(
                new TaskRunKey("t1", "sync", taskId, 1L),
                TaskThrottlePolicyDto.builder().tenantId("t1").taskKind("sync").taskId(taskId).desiredVersion(1L).build(),
                true);

        TaskDto taskDto = new TaskDto();
        taskDto.setId(new ObjectId(taskId));
        taskDto.setSyncType("sync");
        taskDto.setType("cdc");
        DataProcessorContext ctx = DataProcessorContext.newBuilder().withTaskDto(taskDto).build();
        HazelcastSourcePdkDataNode sourceNode = new HazelcastSourcePdkDataNode(ctx);

        ShareCdcRetentionListener listener = ReflectionTestUtils.invokeMethod(sourceNode, "createShareCdcRetentionListener", runtime);
        assertNotNull(listener);

        long now = System.currentTimeMillis();
        // 75% of 1 day -> WARN_70
        listener.onWindowObserved("orders", 15L, 10L, 100L, now - 64_800_000L, 1);
        assertEquals(LogPositionStatus.WARN_70, runtime.getLogRetentionMonitor().worstStatus());
        assertFalse(runtime.getLogRetentionMonitor().shouldStopAutoRetry());

        // Transient failure keeps retry allowed (UNKNOWN severity < CRITICAL)
        listener.onTransientReadFailure("users", 5L, "network glitch");
        assertFalse(runtime.getLogRetentionMonitor().shouldStopAutoRetry());

        // Sequence behind headSequence immediately triggers EXPIRED & locks auto-retry
        listener.onWindowObserved("orders", 9L, 10L, 100L, now, 1);
        assertEquals(LogPositionStatus.EXPIRED, runtime.getLogRetentionMonitor().worstStatus());
        assertTrue(runtime.getLogRetentionMonitor().shouldStopAutoRetry());
        assertNotNull(runtime.getLogRetentionMonitor().getExpiredReason());
    }

    private static List<TapdataShareLogEvent> buildShareLogBatch(String connId, String table, int count) {
        List<TapdataShareLogEvent> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            TapInsertRecordEvent tapEvent = TapInsertRecordEvent.create()
                    .init()
                    .table(table)
                    .after(Map.of("id", i, "payload", "data-" + i));
            TapdataShareLogEvent wrapper = new TapdataShareLogEvent();
            wrapper.setTapEvent(tapEvent);
            wrapper.addInfo(TapdataEvent.CONNECTION_ID_INFO_KEY, connId);
            list.add(wrapper);
        }
        return list;
    }
}
