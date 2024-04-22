package io.tapdata.milestone;

import com.tapdata.constant.BeanUtil;
import com.tapdata.entity.ResponseBody;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.TaskStartAspect;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.task.AspectTask;
import io.tapdata.aspect.taskmilestones.CDCReadBeginAspect;
import io.tapdata.aspect.taskmilestones.CDCReadErrorAspect;
import io.tapdata.aspect.taskmilestones.CDCReadStartedAspect;
import io.tapdata.aspect.taskmilestones.CDCWriteBeginAspect;
import io.tapdata.aspect.taskmilestones.Snapshot2CDCAspect;
import io.tapdata.aspect.taskmilestones.SnapshotReadBeginAspect;
import io.tapdata.aspect.taskmilestones.SnapshotReadEndAspect;
import io.tapdata.aspect.taskmilestones.SnapshotReadErrorAspect;
import io.tapdata.aspect.taskmilestones.SnapshotReadTableEndAspect;
import io.tapdata.aspect.taskmilestones.SnapshotWriteBeginAspect;
import io.tapdata.aspect.taskmilestones.SnapshotWriteEndAspect;
import io.tapdata.aspect.taskmilestones.WriteErrorAspect;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.simplify.pretty.TypeHandlers;
import io.tapdata.exception.TmUnavailableException;
import io.tapdata.milestone.constants.MilestoneStatus;
import io.tapdata.milestone.constants.SyncStatus;
import io.tapdata.milestone.entity.MilestoneEntity;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/3/11 20:55 Create
 */
class MilestoneAspectTaskTest {

	void setField(MilestoneAspectTask milestoneAspectTask, Class<?> clz, String field, Object value) {
		try {
			Field logField = clz.getDeclaredField(field);
			logField.setAccessible(true);
			logField.set(milestoneAspectTask, value);
		} catch (Exception e) {
			throw new RuntimeException("Can't mock '" + field + "' field in " + MilestoneAspectTask.class.getSimpleName(), e);
		}
	}

	@Test
	void testTmUnavailableException() {
		try (MockedStatic<TmUnavailableException> tmUnavailableExceptionMockedStatic = Mockito.mockStatic(TmUnavailableException.class, Mockito.CALLS_REAL_METHODS)) {
			TmUnavailableException ex = new TmUnavailableException("test-url", "post", null, new ResponseBody());
			tmUnavailableExceptionMockedStatic.when(() -> TmUnavailableException.notInstance(ex)).thenReturn(true, false);

			TaskDto task = new TaskDto() {
				@Override
				public String getType() {
					throw ex;
				}
			};
			task.setId(new ObjectId());
			task.setName("test-task");

			MilestoneAspectTask milestoneAspectTask = mock(MilestoneAspectTask.class, Mockito.CALLS_REAL_METHODS);
			setField(milestoneAspectTask, AspectTask.class, "log", mock(Log.class));
			setField(milestoneAspectTask, MilestoneAspectTask.class, "executorService", mock(ScheduledExecutorService.class));
			milestoneAspectTask.setTask(task);

			milestoneAspectTask.onStop(null); // available
			milestoneAspectTask.onStop(null); // unavailable
		}

	}


	@Test
	void testParams() {
		Assertions.assertEquals("TASK", MilestoneAspectTask.KPI_TASK);
		Assertions.assertEquals("DATA_NODE_INIT", MilestoneAspectTask.KPI_DATA_NODE_INIT);
		Assertions.assertEquals("NODE", MilestoneAspectTask.KPI_NODE);
		Assertions.assertEquals("SNAPSHOT", MilestoneAspectTask.KPI_SNAPSHOT);
		Assertions.assertEquals("CDC", MilestoneAspectTask.KPI_CDC);
		Assertions.assertEquals("SNAPSHOT_READ", MilestoneAspectTask.KPI_SNAPSHOT_READ);
		Assertions.assertEquals("OPEN_CDC_READ", MilestoneAspectTask.KPI_OPEN_CDC_READ);
		Assertions.assertEquals("CDC_READ", MilestoneAspectTask.KPI_CDC_READ);
		Assertions.assertEquals("SNAPSHOT_WRITE", MilestoneAspectTask.KPI_SNAPSHOT_WRITE);
		Assertions.assertEquals("CDC_WRITE", MilestoneAspectTask.KPI_CDC_WRITE);
		Assertions.assertEquals("TABLE_INIT", MilestoneAspectTask.KPI_TABLE_INIT);
	}

	@Nested
	class MilestoneAspectTaskEachTest {
		MilestoneAspectTask milestoneAspectTask;

		TypeHandlers<Aspect, Void> observerHandlers;
		TypeHandlers<Aspect, AspectInterceptResult> interceptHandlers;
		TaskDto task;
		Log log;

		Map<String, MilestoneEntity> milestones;
		Map<String, Map<String, MilestoneEntity>> nodeMilestones;
		ScheduledExecutorService executorService;
		ClientMongoOperator clientMongoOperator;
		Set<String> targetNodes;
		Map<String, MilestoneStatus> dataNodeInitMap;
		AtomicLong snapshotTableCounts;
		AtomicLong snapshotTableProgress;


		MilestoneEntity m;
		MilestoneEntity tm;
		@BeforeEach
		void init() {
			m = mock();
			tm = mock();

			observerHandlers = mock(TypeHandlers.class);
			interceptHandlers = mock(TypeHandlers.class);
			task = mock(TaskDto.class);
			log = mock(Log.class);

			milestones = new HashMap<>();
			nodeMilestones = new HashMap<>();
			executorService = mock(ScheduledExecutorService.class);
			clientMongoOperator = mock(ClientMongoOperator.class);
			targetNodes = new HashSet<>();
			dataNodeInitMap = new HashMap<>();
			snapshotTableCounts = new AtomicLong();
			snapshotTableProgress = new AtomicLong();

			milestoneAspectTask = mock(MilestoneAspectTask.class);
			ReflectionTestUtils.setField(milestoneAspectTask, "observerHandlers", observerHandlers);
			ReflectionTestUtils.setField(milestoneAspectTask, "interceptHandlers", interceptHandlers);
			ReflectionTestUtils.setField(milestoneAspectTask, "task", task);
			ReflectionTestUtils.setField(milestoneAspectTask, "log", log);
			ReflectionTestUtils.setField(milestoneAspectTask, "milestones", milestones);
			ReflectionTestUtils.setField(milestoneAspectTask, "nodeMilestones", nodeMilestones);
			ReflectionTestUtils.setField(milestoneAspectTask, "executorService", executorService);
			ReflectionTestUtils.setField(milestoneAspectTask, "clientMongoOperator", clientMongoOperator);
			ReflectionTestUtils.setField(milestoneAspectTask, "targetNodes", targetNodes);
			ReflectionTestUtils.setField(milestoneAspectTask, "dataNodeInitMap", dataNodeInitMap);
			ReflectionTestUtils.setField(milestoneAspectTask, "snapshotTableCounts", snapshotTableCounts);
			ReflectionTestUtils.setField(milestoneAspectTask, "snapshotTableProgress", snapshotTableProgress);

			when(observerHandlers.register(any(Class.class), any(Function.class))).thenReturn(observerHandlers);
			doNothing().when(milestoneAspectTask).setRunning(m);
			doNothing().when(milestoneAspectTask).setFinish(m);

			doNothing().when(tm).setProgress(anyLong());
			doAnswer(a -> {
				Consumer argument = a.getArgument(1, Consumer.class);
				argument.accept(tm);
				return null;
			}).when(milestoneAspectTask).taskMilestone(anyString(), any(Consumer.class));

			doNothing().when(milestoneAspectTask).setFinish(any(MilestoneEntity.class));
			doNothing().when(milestoneAspectTask).setError(any(SnapshotReadErrorAspect.class), any(MilestoneEntity.class));
			doNothing().when(milestoneAspectTask).setError(any(CDCReadErrorAspect.class), any(MilestoneEntity.class));
			doNothing().when(milestoneAspectTask).setError(any(WriteErrorAspect.class), any(MilestoneEntity.class));


			doAnswer(w -> {
				Consumer argument = w.getArgument(2, Consumer.class);
				argument.accept(m);
				return null;
			}).when(milestoneAspectTask).nodeKpi(anyString(), anyString(), any(Consumer.class));
		}

		@Nested
		class InitTest {
			@BeforeEach
			void init() {
				doAnswer(a -> {
					Class classType = a.getArgument(0, Class.class);
					String typeName = a.getArgument(1, String.class);
					BiConsumer consumer = a.getArgument(2, BiConsumer.class);
					mockSnapshotReadBeginAspect(classType, typeName, consumer);
					mockSnapshotReadEndAspect(classType, typeName, consumer);
					mockSnapshotReadErrorAspect(classType, typeName, consumer);
					mockSnapshotReadTableEndAspect(classType, typeName, consumer);
					mockSnapshot2CDCAspect(classType, typeName, consumer);
					mockCDCReadBeginAspect(classType, typeName, consumer);
					mockSnapshotWriteBeginAspect(classType, typeName, consumer);
					mockSnapshotWriteEndAspect(classType, typeName, consumer);
					return null;
				}).when(milestoneAspectTask).nodeRegister(any(Class.class), anyString(), any(BiConsumer.class));

				doAnswer(a -> {
					Class classType = a.getArgument(0, Class.class);
					BiConsumer consumer = a.getArgument(1, BiConsumer.class);
					mockCDCReadStartedAspect(classType, consumer);
					mockCDCReadErrorAspect(classType, consumer);
					mockCDCWriteBeginAspect(classType, consumer);
					mockWriteErrorAspect(classType, consumer);
					return null;
				}).when(milestoneAspectTask).nodeRegister(any(Class.class), any(BiConsumer.class));

				doCallRealMethod().when(milestoneAspectTask).init();
			}

			SnapshotReadBeginAspect snapshotReadBeginAspect;
			List<String> tablesForSnapshotReadBeginAspect;
			void mockSnapshotReadBeginAspect(Class classType, String typeName, BiConsumer consumer) {
				snapshotReadBeginAspect = mock(SnapshotReadBeginAspect.class);
				doNothing().when(m).setProgress(0L);
				tablesForSnapshotReadBeginAspect = mock(List.class);
				when(tablesForSnapshotReadBeginAspect.size()).thenReturn(1);
				when(snapshotReadBeginAspect.getTables()).thenReturn(tablesForSnapshotReadBeginAspect);
				doNothing().when(task).setSyncStatus(SyncStatus.SNAPSHOT_INIT.getType());
				if (classType.getName().equals(SnapshotReadBeginAspect.class.getName())
						&& MilestoneAspectTask.KPI_SNAPSHOT_READ.equals(typeName)) {
					consumer.accept(snapshotReadBeginAspect, m);
				}
			}

			SnapshotReadEndAspect snapshotReadEndAspect;
			void mockSnapshotReadEndAspect(Class classType, String typeName, BiConsumer consumer) {
				snapshotReadEndAspect = mock(SnapshotReadEndAspect.class);
				if (classType.getName().equals(SnapshotReadEndAspect.class.getName())
						&& MilestoneAspectTask.KPI_SNAPSHOT_READ.equals(typeName)) {
					consumer.accept(snapshotReadEndAspect, m);
				}
			}

			SnapshotReadErrorAspect snapshotReadErrorAspect;
			void mockSnapshotReadErrorAspect(Class classType, String typeName, BiConsumer consumer) {
				snapshotReadErrorAspect = mock();
				doNothing().when(task).setSyncStatus(SyncStatus.SNAPSHOT_FAILED.getType());
				if (classType.getName().equals(SnapshotReadErrorAspect.class.getName())
						&& MilestoneAspectTask.KPI_SNAPSHOT_READ.equals(typeName)) {
					consumer.accept(snapshotReadErrorAspect, m);
				}
			}

			SnapshotReadTableEndAspect snapshotReadTableEndAspect;
			void mockSnapshotReadTableEndAspect(Class classType, String typeName, BiConsumer consumer) {
				snapshotReadTableEndAspect = mock();
				doNothing().when(task).setSyncStatus(SyncStatus.SNAPSHOT_FAILED.getType());
				doNothing().when(m).addProgress(1);
				doNothing().when(task).setSyncStatus(SyncStatus.TABLE_INIT_COMPLETE.getType());
				if (classType.getName().equals(SnapshotReadTableEndAspect.class.getName())
						&& MilestoneAspectTask.KPI_SNAPSHOT_READ.equals(typeName)) {
					consumer.accept(snapshotReadTableEndAspect, m);
				}
			}

			Snapshot2CDCAspect snapshot2CDCAspect;
			void mockSnapshot2CDCAspect(Class classType, String typeName, BiConsumer consumer) {
				snapshot2CDCAspect = mock();
				doNothing().when(task).setSyncStatus(SyncStatus.SNAPSHOT_COMPLETED.getType());
				if (classType.getName().equals(Snapshot2CDCAspect.class.getName())
						&& MilestoneAspectTask.KPI_SNAPSHOT_READ.equals(typeName)) {
					consumer.accept(snapshot2CDCAspect, m);
				}
			}

			CDCReadBeginAspect cdcReadBeginAspect;
			void mockCDCReadBeginAspect(Class classType, String typeName, BiConsumer consumer) {
				cdcReadBeginAspect = mock();
				when(milestoneAspectTask.hasSnapshot()).thenReturn(true);
				doNothing().when(task).setSyncStatus(SyncStatus.CDC_INIT.getType());
				if (classType.getName().equals(CDCReadBeginAspect.class.getName())
						&& MilestoneAspectTask.KPI_OPEN_CDC_READ.equals(typeName)) {
					consumer.accept(cdcReadBeginAspect, m);
				}
			}

			SnapshotWriteBeginAspect snapshotWriteBeginAspect;
			void mockSnapshotWriteBeginAspect(Class classType, String typeName, BiConsumer consumer) {
				snapshotWriteBeginAspect = mock();
				doNothing().when(task).setSyncStatus(SyncStatus.DO_SNAPSHOT.getType());
				if (classType.getName().equals(SnapshotWriteBeginAspect.class.getName())
						&& MilestoneAspectTask.KPI_SNAPSHOT_WRITE.equals(typeName)) {
					consumer.accept(snapshotWriteBeginAspect, m);
				}
			}

			SnapshotWriteEndAspect snapshotWriteEndAspect;
			void mockSnapshotWriteEndAspect(Class classType, String typeName, BiConsumer consumer) {
				snapshotWriteEndAspect = mock();
				doNothing().when(task).setSyncStatus(SyncStatus.SNAPSHOT_COMPLETED.getType());
				if (classType.getName().equals(SnapshotWriteEndAspect.class.getName())
						&& MilestoneAspectTask.KPI_SNAPSHOT_WRITE.equals(typeName)) {
					consumer.accept(snapshotWriteEndAspect, m);
				}
			}

			CDCReadStartedAspect cdcReadStartedAspect;
			void mockCDCReadStartedAspect(Class classType, BiConsumer consumer) {
				cdcReadStartedAspect = mock();
				when(milestoneAspectTask.hasSnapshot()).thenReturn(true);
				doNothing().when(task).setSyncStatus(SyncStatus.DO_CDC.getType());
				if (classType.getName().equals(CDCReadStartedAspect.class.getName())) {
					consumer.accept("nodeId", cdcReadStartedAspect);
				}
			}

			CDCReadErrorAspect cdcReadErrorAspect;
			void mockCDCReadErrorAspect(Class classType, BiConsumer consumer) {
				cdcReadStartedAspect = mock();
				when(m.getEnd()).thenReturn(1L);
				when(milestoneAspectTask.hasSnapshot()).thenReturn(true);
				doNothing().when(task).setSyncStatus(SyncStatus.CDC_FAILED.getType());
				if (classType.getName().equals(CDCReadErrorAspect.class.getName())) {
					consumer.accept("nodeId", cdcReadErrorAspect);
				}
			}

			CDCWriteBeginAspect cdcWriteBeginAspect;
			void mockCDCWriteBeginAspect(Class classType, BiConsumer consumer) {
				cdcWriteBeginAspect = mock();
				when(milestoneAspectTask.hasSnapshot()).thenReturn(true);
				doNothing().when(task).setSyncStatus(SyncStatus.DO_CDC.getType());
				if (classType.getName().equals(CDCWriteBeginAspect.class.getName())) {
					consumer.accept("nodeId", cdcWriteBeginAspect);
				}
			}

			WriteErrorAspect writeErrorAspect;
			void mockWriteErrorAspect(Class classType, BiConsumer consumer) {
				writeErrorAspect = mock();
				when(m.getEnd()).thenReturn(1L);
				if (classType.getName().equals(WriteErrorAspect.class.getName())) {
					consumer.accept("nodeId", writeErrorAspect);
				}
			}

			@Test
			void testNormal() {
				Assertions.assertDoesNotThrow(() -> milestoneAspectTask.init());
			}

			@Test
			void testCDCReadBeginAspectNotHasSnapshot() {
				when(milestoneAspectTask.hasSnapshot()).thenReturn(false);
				Assertions.assertDoesNotThrow(() -> milestoneAspectTask.init());
			}
			@Test
			void testCDCReadErrorAspectGetEndIsNull() {
				when(m.getEnd()).thenReturn(null);
				Assertions.assertDoesNotThrow(() -> milestoneAspectTask.init());
			}
			@Test
			void testSnapshotWriteEndAspect() {
				snapshotTableProgress.set(-100);
				Assertions.assertDoesNotThrow(() -> milestoneAspectTask.init());
			}

			@Test
			void testSnapshotWriteEndAspectV2() {
				snapshotTableProgress.set(100);
				Assertions.assertDoesNotThrow(() -> milestoneAspectTask.init());
			}

			@Test
			void testCDCWriteBeginAspect() {
				doAnswer(a -> {
					Class classType = a.getArgument(0, Class.class);
					BiConsumer consumer = a.getArgument(1, BiConsumer.class);
					mockCDCReadStartedAspect(classType, consumer);
					mockCDCReadErrorAspect(classType, consumer);
					cdcWriteBeginAspect = mock();
					when(milestoneAspectTask.hasSnapshot()).thenReturn(false);
					doNothing().when(task).setSyncStatus(SyncStatus.DO_CDC.getType());
					if (classType.getName().equals(CDCWriteBeginAspect.class.getName())) {
						consumer.accept("nodeId", cdcWriteBeginAspect);
					}
					mockWriteErrorAspect(classType, consumer);
					return null;
				}).when(milestoneAspectTask).nodeRegister(any(Class.class), any(BiConsumer.class));
				Assertions.assertDoesNotThrow(() -> milestoneAspectTask.init());
			}

			@Test
			void testWriteErrorAspect() {
				doAnswer(a -> {
					Class classType = a.getArgument(0, Class.class);
					BiConsumer consumer = a.getArgument(1, BiConsumer.class);
					mockCDCReadStartedAspect(classType, consumer);
					mockCDCReadErrorAspect(classType, consumer);
					mockCDCWriteBeginAspect(classType, consumer);
					writeErrorAspect = mock();
					when(m.getEnd()).thenReturn(null);
					if (classType.getName().equals(WriteErrorAspect.class.getName())) {
						consumer.accept("nodeId", writeErrorAspect);
					}
					return null;
				}).when(milestoneAspectTask).nodeRegister(any(Class.class), any(BiConsumer.class));
				Assertions.assertDoesNotThrow(() -> milestoneAspectTask.init());
			}
		}

		@Nested
		class OnStartTest {
			TaskStartAspect startAspect;
			ObjectId id;
			DAG dag;
			List<Node> nodes ;
			Node node;
			@BeforeEach
			void init() {
				node = mock(Node.class);
				when(node.isDataNode()).thenReturn(true);
				when(node.getId()).thenReturn("id");
				nodes = new ArrayList<>();
				nodes.add(node);
				startAspect = mock(TaskStartAspect.class);
				id = mock(ObjectId.class);
				dag = mock(DAG.class);
				when(task.getId()).thenReturn(id);
				when(id.toHexString()).thenReturn("id");
				when(task.getName()).thenReturn("name");
				doNothing().when(task).setSyncStatus(SyncStatus.TABLE_INIT_COMPLETE.getType());
				when(task.getSyncType()).thenReturn("syncType");
				doNothing().when(log).info(anyString(), anyString(), anyString());

				when(milestoneAspectTask.hasSnapshot()).thenReturn(true);
				when(milestoneAspectTask.hasCdc()).thenReturn(true);
				when(task.getDag()).thenReturn(dag);
				when(dag.getNodes()).thenReturn(nodes);
				when(executorService.scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class))).thenReturn(mock());

				doCallRealMethod().when(milestoneAspectTask).onStart(startAspect);
			}

			void verifyAssert(int KPI_TABLE_INITTimes, int KPI_SNAPSHOT, int KPI_CDC, int getId) {
				try(MockedStatic<BeanUtil> bu = mockStatic(BeanUtil.class)) {
					bu.when(() -> BeanUtil.getBean(ClientMongoOperator.class)).thenReturn(clientMongoOperator);
					milestoneAspectTask.onStart(startAspect);
				}
				verify(task, times(1)).getId();
				verify(id, times(1)).toHexString();
				verify(task, times(1)).getName();
				verify(log).info(anyString(), anyString(), anyString());
				verify(task, times(1)).setSyncStatus(SyncStatus.TABLE_INIT_COMPLETE.getType());
				verify(milestoneAspectTask).taskMilestone(anyString(), any(Consumer.class));
				verify(task).getSyncType();
				verify(milestoneAspectTask, times(KPI_TABLE_INITTimes)).taskMilestone(MilestoneAspectTask.KPI_TABLE_INIT, null);
				verify(milestoneAspectTask).taskMilestone(MilestoneAspectTask.KPI_DATA_NODE_INIT, null);
				verify(milestoneAspectTask).hasSnapshot();
				verify(milestoneAspectTask, times(KPI_SNAPSHOT)).taskMilestone(MilestoneAspectTask.KPI_SNAPSHOT, null);
				verify(milestoneAspectTask).hasCdc();
				verify(milestoneAspectTask, times(KPI_CDC)).taskMilestone(MilestoneAspectTask.KPI_CDC, null);
				verify(task).getDag();
				verify(node).isDataNode();
				verify(node, times(getId)).getId();
				verify(executorService).scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
			}
			@Test
			void testNormal() {
				verifyAssert(1, 1, 1, 1);
			}
			@Test
			void testSYNC_TYPE_LOG_COLLECTOR() {
				when(task.getSyncType()).thenReturn(TaskDto.SYNC_TYPE_LOG_COLLECTOR);
				verifyAssert(0, 1, 1, 1);
			}
			@Test
			void testNotHasSnapshot() {
				when(milestoneAspectTask.hasSnapshot()).thenReturn(false);
				verifyAssert(1, 0, 1, 1);
			}
			@Test
			void testNotHasCdc() {
				when(milestoneAspectTask.hasCdc()).thenReturn(false);
				verifyAssert(1, 1, 0, 1);
			}
			@Test
			void testNotDataNode() {
				when(node.isDataNode()).thenReturn(false);
				verifyAssert(1, 1, 1, 0);
			}
		}
		@Nested
		class OnStopTest {
			TaskStopAspect stopAspect;
			ObjectId id;
			@BeforeEach
			void init() {
				stopAspect = mock(TaskStopAspect.class);
				id = mock(ObjectId.class);
				doNothing().when(task).setSyncStatus(SyncStatus.NORMAL.getType());
				when(task.getId()).thenReturn(id);
				when(id.toHexString()).thenReturn("id");
				when(task.getName()).thenReturn("name");
				doNothing().when(log).info(anyString(), anyString(), anyString());
				doNothing().when(executorService).shutdown();
				doNothing().when(milestoneAspectTask).storeMilestone();
				doCallRealMethod().when(milestoneAspectTask).onStop(stopAspect);
			}
			@Test
			void testNormal() {
				Assertions.assertDoesNotThrow(() -> milestoneAspectTask.onStop(stopAspect));
				verify(task).setSyncStatus(SyncStatus.NORMAL.getType());
				verify(task).getId();
				verify(id).toHexString();
				verify(task).getName();
				verify(log).info(anyString(), anyString(), anyString());
				verify(executorService).shutdown();
				verify(milestoneAspectTask).storeMilestone();
			}
		}

		@Nested
		class HandleDataNodeInitTest {

		}
		@Nested
		class HandleDataNodeCloseTest {

		}
		@Nested
		class HandleProcessNodeInitTest {

		}
		@Nested
		class HandleProcessNodeCloseTest {

		}
		@Nested
		class HandleTableInitTest {

		}
		@Nested
		class StoreMilestoneTest {

		}
	}
}
