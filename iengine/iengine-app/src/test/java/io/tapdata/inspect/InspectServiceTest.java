package io.tapdata.inspect;

import base.BaseTest;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Connections;
import com.tapdata.entity.inspect.*;
import com.tapdata.entity.inspect.InspectTask;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.SettingService;
import io.tapdata.inspect.cdc.compare.RowCountInspectCdcJob;
import io.tapdata.inspect.compare.TableRowContentInspectJob;
import io.tapdata.inspect.compare.TableRowCountInspectJob;
import io.tapdata.inspect.compare.TableRowScriptInspectJob;
import org.junit.jupiter.api.*;
import org.junit.runner.RunWith;
import org.mockito.internal.verification.Times;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
class InspectServiceTest extends BaseTest {
    private InspectService inspectService;
    private ClientMongoOperator clientMongoOperator;
    private SettingService settingService;
    @BeforeEach
    public void setInspectService() {
        clientMongoOperator = mock(ClientMongoOperator.class);
        settingService = mock(SettingService.class);
        inspectService = InspectService.getInstance(clientMongoOperator, settingService);
        ReflectionTestUtils.setField(inspectService, "clientMongoOperator", clientMongoOperator);
    }
    @Nested
    class GetInspectByIdTest{
        @Test
        void testGetInspectByIdNormal(){
            String id = "655322fa3ebad82f4b55d233";
            Inspect exceptInspect = new Inspect();
            exceptInspect.setInspectResultId(id);
            Query query = Query.query(Criteria.where("_id").is(id));
            query.fields().exclude("tasks.source.fields").exclude("tasks.target.fields");
            when(clientMongoOperator.findOne(query,ConnectorConstant.INSPECT_COLLECTION, Inspect.class)).thenReturn(exceptInspect);
            Inspect inspectById = inspectService.getInspectById(id);
            Assertions.assertEquals(exceptInspect,inspectById);
        }
        @Test
        void testGetInspectByIdWithNullOrEmpty(){
            String id = " ";
            Assertions.assertThrows(IllegalArgumentException.class,()->inspectService.getInspectById(id));
        }
    }
    @Nested
    class UpdateStatusTest{
        @Test
        void testUpdateStatusNormal(){
            inspectService = spy(inspectService);
            String id = "11111";
            InspectStatus status = InspectStatus.DONE;
            String msg = "test msg";
            Map<String, Object> queryMap = new HashMap<>();
            queryMap.put("id", id);
            Map<String, Object> updateMap = new HashMap<>();
            updateMap.put("status", status.getCode());
            updateMap.put("errorMsg", msg);
            inspectService.updateStatus(id,status,msg);
            verify(inspectService, new Times(1)).updateStatus(id,status,msg);
        }
    }
    @Nested
    class UpsertInspectResultTest{
        @Test
        void testUpsertInspectResultWithIdNotNull(){
            inspectService = spy(inspectService);
            InspectResult inspectResult = new InspectResult();
            inspectResult.setId("1111");
            boolean excludeInspect = true;
            inspectService.upsertInspectResult(inspectResult,excludeInspect);
            verify(inspectService).upsertInspectResult(inspectResult,excludeInspect);
        }
        @Test
        void testUpsertInspectResultWithIdIsNull(){
            inspectService = spy(inspectService);
            InspectResult inspectResult = mock(InspectResult.class);
            boolean excludeInspect = true;
            inspectService.upsertInspectResult(inspectResult,excludeInspect);
            verify(inspectService).upsertInspectResult(inspectResult,excludeInspect);
        }
    }
    @Nested
    class GetLastDifferenceInspectResultTest{
        @Test
        void testGetLastDifferenceInspectResultNormal(){
            String firstCheckId = "11111";
            InspectResult exceptInspectResult = new InspectResult();
            exceptInspectResult.setInspect_id(firstCheckId);
            Query query = Query.query(Criteria
                    .where("firstCheckId").regex("^" + firstCheckId + "$")
                    .and("status").is(InspectStatus.DONE.getCode())
                    .and("stats.status").is(InspectStatus.DONE.getCode())
                    .and("stats.result").is("failed")
            ).with(Sort.by(Sort.Order.desc("ttlTime"))).limit(1);
            when(clientMongoOperator.findOne(query, ConnectorConstant.INSPECT_RESULT_COLLECTION, InspectResult.class)).thenReturn(exceptInspectResult);
            InspectResult lastDifferenceInspectResult = inspectService.getLastDifferenceInspectResult(firstCheckId);
            Assertions.assertEquals(exceptInspectResult,lastDifferenceInspectResult);
        }
        @Test
        void testGetLastDifferenceInspectResultWithNullId(){
            String firstCheckId = null;
            InspectResult exceptInspectResult = null;
            Query query = Query.query(Criteria
                    .where("firstCheckId").regex("^" + firstCheckId + "$")
                    .and("status").is(InspectStatus.DONE.getCode())
                    .and("stats.status").is(InspectStatus.DONE.getCode())
                    .and("stats.result").is("failed")
            ).with(Sort.by(Sort.Order.desc("ttlTime"))).limit(1);
            when(clientMongoOperator.findOne(query, ConnectorConstant.INSPECT_RESULT_COLLECTION, InspectResult.class)).thenReturn(exceptInspectResult);
            InspectResult lastDifferenceInspectResult = inspectService.getLastDifferenceInspectResult(firstCheckId);
            Assertions.assertEquals(exceptInspectResult,lastDifferenceInspectResult);
        }
    }
    @Nested
    class GetLastInspectResultTest{
        @Test
        void testGetLastInspectResultNormal(){
            String inspectId = "11111";
            InspectResult exceptInspectResult = new InspectResult();
            exceptInspectResult.setInspect_id(inspectId);
            Query query = Query.query(Criteria
                    .where("inspect_id").regex("^" + inspectId + "$")
                    .and("status").in(InspectStatus.DONE.getCode(), InspectStatus.PAUSE.getCode(), InspectStatus.ERROR.getCode())
            ).with(Sort.by(Sort.Order.desc("ttlTime"))).limit(1);
            when(clientMongoOperator.findOne(query, ConnectorConstant.INSPECT_RESULT_COLLECTION, InspectResult.class)).thenReturn(exceptInspectResult);
            InspectResult lastDifferenceInspectResult = inspectService.getLastInspectResult(inspectId);
            Assertions.assertEquals(exceptInspectResult,lastDifferenceInspectResult);
        }
        @Test
        void testGetLastInspectResultWithNullId(){
            String inspectId = null;
            InspectResult exceptInspectResult = null;
            Query query = Query.query(Criteria
                    .where("inspect_id").regex("^" + inspectId + "$")
                    .and("status").in(InspectStatus.DONE.getCode(), InspectStatus.PAUSE.getCode(), InspectStatus.ERROR.getCode())
            ).with(Sort.by(Sort.Order.desc("ttlTime"))).limit(1);
            when(clientMongoOperator.findOne(query, ConnectorConstant.INSPECT_RESULT_COLLECTION, InspectResult.class)).thenReturn(exceptInspectResult);
            InspectResult lastDifferenceInspectResult = inspectService.getLastInspectResult(inspectId);
            Assertions.assertEquals(exceptInspectResult,lastDifferenceInspectResult);
        }
    }
    @Nested
    class GetInspectResultByIdTest{
        private String inspectResultId;
        @Test
        void testGetInspectResultByIdNormal(){
            inspectResultId = "11111";
            InspectResult exceptInspectResult = new InspectResult();
            exceptInspectResult.setInspect_id(inspectResultId);
            when(clientMongoOperator.findOne(Query.query(Criteria.where("id").is(inspectResultId)),
                    ConnectorConstant.INSPECT_RESULT_COLLECTION, InspectResult.class)).thenReturn(exceptInspectResult);
            InspectResult actual = inspectService.getInspectResultById(inspectResultId);
            Assertions.assertEquals(exceptInspectResult,actual);
        }
        @Test
        void testGetInspectResultByIdWithNullId(){
            inspectResultId = null;
            InspectResult exceptInspectResult = new InspectResult();
            when(clientMongoOperator.findOne(Query.query(Criteria.where("id").is(inspectResultId)),
                    ConnectorConstant.INSPECT_RESULT_COLLECTION, InspectResult.class)).thenReturn(exceptInspectResult);
            InspectResult actual = inspectService.getInspectResultById(inspectResultId);
            Assertions.assertEquals(exceptInspectResult,actual);
        }
    }
    @Nested
    class GetInspectConnectionsByIdTest{
        @Test
        void testGetInspectConnectionsByIdNormal(){
            List<Connections> excepted = new ArrayList<>();
            Inspect inspect = new Inspect();
            InspectTask inspectTask = new InspectTask();
            inspectTask.setTaskId("11111");
            InspectDataSource inspectDataSource = new InspectDataSource();
            inspectDataSource.setConnectionId("1");
            inspectDataSource.setTable("table1");
            inspectTask.setSource(inspectDataSource);
            inspectTask.setTarget(inspectDataSource);
            List<InspectTask> inspectTasks = new ArrayList<>();
            inspectTasks.add(inspectTask);
            inspect.setTasks(inspectTasks);
            List<Connections> actual = inspectService.getInspectConnectionsById(inspect);
            Assertions.assertEquals(excepted,actual);
        }
        @Test
        void testGetInspectConnectionsWithNullInspect(){
            Inspect inspect = null;
            Assertions.assertThrows(IllegalArgumentException.class,()->inspectService.getInspectConnectionsById(inspect));
        }
    }
    @Nested
    class InsertInspectDetailsTest{
        @Test
        void testInsertInspectDetailsNormal(){
            inspectService = spy(inspectService);
            List<InspectDetail> details = new ArrayList<>();
            InspectDetail inspectDetail = mock(InspectDetail.class);
            details.add(inspectDetail);
            inspectService.insertInspectDetails(details);
            verify(inspectService).insertInspectDetails(details);
        }
    }
    @Nested
    class StartInspectTest{
        private final ConcurrentHashMap<String, io.tapdata.inspect.InspectTask> RUNNING_INSPECT
                = new ConcurrentHashMap<String, io.tapdata.inspect.InspectTask>();
        private io.tapdata.inspect.InspectTask inspectTask;
        private Inspect inspect;
        @Test
        void testStartInspectWithNullInspect(){
            inspect = null;
            Assertions.assertThrows(IllegalArgumentException.class,()->inspectService.startInspect(inspect));
        }
        @Test
        void testStartInspectWithIsRunning(){
            inspect = mock(Inspect.class);
            inspectTask = mock(io.tapdata.inspect.InspectTask.class);
            RUNNING_INSPECT.put("11111",inspectTask);
            inspectService = mock(InspectService.class);
            doNothing().when(inspectService).startInspect(inspect);
            inspectService.startInspect(inspect);
            verify(inspectService).startInspect(inspect);
            RUNNING_INSPECT.clear();
        }
        @Test
        void testStartInspectWithInspectMethodIsNull(){
            inspect = new Inspect();
            inspect.setId("11111");
            inspect.setInspectMethod(null);
            Assertions.assertThrows(IllegalArgumentException.class,()->inspectService.startInspect(inspect));
        }
        @Test
        void testStartInspectWithFieldMethod() {
            inspect = new Inspect();
            inspect.setId("11111");
            inspect.setInspectMethod("field");
            inspectService = spy(inspectService);
            inspectTask = mock(io.tapdata.inspect.InspectTask.class);
            Future submit = mock(Future.class);
            doReturn(submit).when(inspectService).submitTask(inspectTask);
            doReturn(inspectTask).when(inspectService).executeFieldInspect(inspect);
            doReturn(new ArrayList()).when(inspectService).checkFieldInspect(inspect);
            doCallRealMethod().when(inspectService).startInspect(inspect);
            inspectService.startInspect(inspect);
            verify(inspectService, new Times(1))
                    .submitTask(any(io.tapdata.inspect.InspectTask.class));
        }
        @Test
        void testStartInspectWithCountMethod() {
            inspect = new Inspect();
            inspect.setId("11111");
            inspect.setInspectMethod("row_count");
            inspectService = spy(inspectService);
            inspectTask = mock(io.tapdata.inspect.InspectTask.class);
            Future submit = mock(Future.class);
            doReturn(submit).when(inspectService).submitTask(inspectTask);
            doReturn(inspectTask).when(inspectService).executeRowCountInspect(inspect);
            doReturn(new ArrayList()).when(inspectService).checkRowCountInspect(inspect);
            inspectService.startInspect(inspect);
            verify(inspectService, new Times(1))
                    .submitTask(any(io.tapdata.inspect.InspectTask.class));
        }@Test
        void testStartInspectWithDefaultMethod(){
            inspect = new Inspect();
            inspect.setId("11111");
            inspect.setInspectMethod("junitTest");
            inspectService = spy(inspectService);
            doNothing().when(inspectService).updateStatus(inspect.getId(), InspectStatus.ERROR, String.join(", ", "Unsupported comparison method"));
            inspectService.startInspect(inspect);
            verify(inspectService, new Times(1))
                    .updateStatus(inspect.getId(), InspectStatus.ERROR, String.join(", ", "Unsupported comparison method"));
        }
    }
    @Nested
    class onInspectStoppedTest{
        @Test
        void testOnInspectStoppedNormal(){
            Inspect inspect = new Inspect();
            inspect.setId("11111");
            inspectService = mock(InspectService.class);
            doNothing().when(inspectService).onInspectStopped(inspect);
            inspectService.onInspectStopped(inspect);
            verify(inspectService).onInspectStopped(inspect);
        }
        @Test
        void testOnInspectStoppedWithNullId(){
            Inspect inspect = null;
            Assertions.assertThrows(IllegalArgumentException.class,()->inspectService.onInspectStopped(inspect));
        }
    }
    @Nested
    class doInspectStopTest{
        @Test
        void testDoInspectStopNormal(){
            String inspectId = "11111";
            inspectService = spy(inspectService);
            inspectService.doInspectStop(inspectId);
            verify(inspectService).doInspectStop(inspectId);
        }
        @Test
        void testDoInspectStopWithNullId(){
            String inspectId = null;
            Assertions.assertThrows(IllegalArgumentException.class,()->inspectService.doInspectStop(inspectId));
        }
    }
    @Nested
    class ExecuteRowCountInspectTest{
        private Inspect inspect;
        private InspectDataSource inspectDataSource;
        @BeforeEach
        void buildInspect(){
            inspect = new Inspect();
            inspect.setStatus("scheduling");
            InspectTask inspectTask = new InspectTask();
            inspectTask.setTaskId("11111");
            inspectDataSource = new InspectDataSource();
            inspectDataSource.setConnectionId("1");
            inspectDataSource.setTable("table1");
            inspectDataSource.setDirection("ASC");
            inspectDataSource.setSortColumn("a1");
            inspectTask.setSource(inspectDataSource);
            inspectTask.setTarget(inspectDataSource);
            List<InspectTask> inspectTasks = new ArrayList<>();
            inspectTasks.add(inspectTask);
            inspect.setTasks(inspectTasks);
            inspect.setInspectMethod("row_count");
        }
        @Test
        void testExecuteRowCountInspectWithNullInspect(){
            inspect = null;
            ArrayList<String> errorMsg = new ArrayList<>();
            errorMsg.add("Inspect can not be empty.");
            inspectService = spy(InspectService.getInstance(clientMongoOperator,settingService));
            doReturn(errorMsg).when(inspectService).checkRowCountInspect(inspect);
            io.tapdata.inspect.InspectTask actual = inspectService.executeRowCountInspect(inspect);
            Assertions.assertEquals(null,actual);
        }
        @Test
        void testExecuteRowCountInspectWithErrorMsg(){
            ArrayList<String> errorMsg = new ArrayList<>();
            errorMsg.add("Inspect status must be scheduling");
            inspectService = spy(InspectService.getInstance(clientMongoOperator,settingService));
            doReturn(errorMsg).when(inspectService).checkRowCountInspect(inspect);
            doNothing().when(inspectService).updateStatus(inspect.getId(), InspectStatus.ERROR, String.join(", ", errorMsg));
            inspectService.updateStatus(inspect.getId(), InspectStatus.ERROR, String.join(", ", errorMsg));
            verify(inspectService).updateStatus(inspect.getId(), InspectStatus.ERROR, String.join(", ", errorMsg));
            io.tapdata.inspect.InspectTask actual = inspectService.executeRowCountInspect(inspect);
            Assertions.assertEquals(null,actual);
        }
        @Test
        void testExecuteRowCountInspectWithRowCountInspectCdcJob(){
            ArrayList<String> errorMsg = new ArrayList<>();
            inspectService = spy(InspectService.getInstance(clientMongoOperator,settingService));
            doReturn(errorMsg).when(inspectService).checkRowCountInspect(inspect);
            io.tapdata.inspect.InspectTask inspectTask = new io.tapdata.inspect.InspectTask(inspectService, inspect, clientMongoOperator) {
                @Override
                public Runnable createTableInspectJob(InspectTaskContext inspectTaskContext) {
                    return new RowCountInspectCdcJob(inspectTaskContext);
                }
            };
            when(inspectService.executeRowCountInspect(inspect)).thenReturn(inspectTask);
            io.tapdata.inspect.InspectTask actual = inspectService.executeRowCountInspect(inspect);
            Assertions.assertEquals(inspectTask,actual);
        }
        @Test
        void testExecuteRowCountInspectWithTableRowCountInspectJob(){
            ArrayList<String> errorMsg = new ArrayList<>();
            inspectService = spy(InspectService.getInstance(clientMongoOperator,settingService));
            doReturn(errorMsg).when(inspectService).checkRowCountInspect(inspect);
            io.tapdata.inspect.InspectTask inspectTask = new io.tapdata.inspect.InspectTask(inspectService, inspect, clientMongoOperator) {
                @Override
                public Runnable createTableInspectJob(InspectTaskContext inspectTaskContext) {
                    return new TableRowCountInspectJob(inspectTaskContext);
                }
            };
            when(inspectService.executeRowCountInspect(inspect)).thenReturn(inspectTask);
            io.tapdata.inspect.InspectTask actual = inspectService.executeRowCountInspect(inspect);
            Assertions.assertEquals(inspectTask,actual);
        }

    }
    @Nested
    class ExecuteFieldInspectTest{
        private Inspect inspect;
        private InspectDataSource inspectDataSource;
        @BeforeEach
        void buildInspect(){
            inspect = new Inspect();
            inspect.setStatus("scheduling");
            InspectTask inspectTask = new InspectTask();
            inspectTask.setTaskId("11111");
            inspectDataSource = new InspectDataSource();
            inspectDataSource.setConnectionId("1");
            inspectDataSource.setTable("table1");
            inspectDataSource.setDirection("ASC");
            inspectDataSource.setSortColumn("a1");
            inspectTask.setSource(inspectDataSource);
            inspectTask.setTarget(inspectDataSource);
            List<InspectTask> inspectTasks = new ArrayList<>();
            inspectTasks.add(inspectTask);
            inspect.setTasks(inspectTasks);
            inspect.setInspectMethod("row_count");
        }
        @Test
        void testExecuteFieldInspectWithNullInspect(){
            inspect = null;
            ArrayList<String> errorMsg = new ArrayList<>();
            errorMsg.add("Inspect can not be empty.");
            inspectService = spy(InspectService.getInstance(clientMongoOperator,settingService));
            doReturn(errorMsg).when(inspectService).checkFieldInspect(inspect);
            io.tapdata.inspect.InspectTask actual = inspectService.executeFieldInspect(inspect);
            Assertions.assertEquals(null,actual);
        }
        @Test
        void testExecuteFieldInspectWithErrorMsg(){
            ArrayList<String> errorMsg = new ArrayList<>();
            errorMsg.add("Inspect status must be scheduling");
            inspectService = spy(InspectService.getInstance(clientMongoOperator,settingService));
            doReturn(errorMsg).when(inspectService).checkFieldInspect(inspect);
            doNothing().when(inspectService).updateStatus(inspect.getId(), InspectStatus.ERROR, String.join(", ", errorMsg));
            inspectService.updateStatus(inspect.getId(), InspectStatus.ERROR, String.join(", ", errorMsg));
            verify(inspectService).updateStatus(inspect.getId(), InspectStatus.ERROR, String.join(", ", errorMsg));
            io.tapdata.inspect.InspectTask actual = inspectService.executeFieldInspect(inspect);
            Assertions.assertEquals(null,actual);
        }
        @Test
        void testExecuteFieldInspectWithContentInspectJob(){
            ArrayList<String> errorMsg = new ArrayList<>();
            inspectService = spy(InspectService.getInstance(clientMongoOperator,settingService));
            doReturn(errorMsg).when(inspectService).checkFieldInspect(inspect);
            io.tapdata.inspect.InspectTask inspectTask = new io.tapdata.inspect.InspectTask(inspectService, inspect, clientMongoOperator) {
                @Override
                public Runnable createTableInspectJob(InspectTaskContext inspectTaskContext) {
                        return new TableRowContentInspectJob(inspectTaskContext);
                }
            };
            when(inspectService.executeFieldInspect(inspect)).thenReturn(inspectTask);
            io.tapdata.inspect.InspectTask actual = inspectService.executeFieldInspect(inspect);
            Assertions.assertEquals(inspectTask,actual);
        }
        @Test
        void testExecuteFieldInspectWithScriptInspectJob(){
            ArrayList<String> errorMsg = new ArrayList<>();
            inspectService = spy(InspectService.getInstance(clientMongoOperator,settingService));
            doReturn(errorMsg).when(inspectService).checkFieldInspect(inspect);
            io.tapdata.inspect.InspectTask inspectTask = new io.tapdata.inspect.InspectTask(inspectService, inspect, clientMongoOperator) {
                @Override
                public Runnable createTableInspectJob(InspectTaskContext inspectTaskContext) {
                    return new TableRowScriptInspectJob(inspectTaskContext);
                }
            };
            when(inspectService.executeFieldInspect(inspect)).thenReturn(inspectTask);
            io.tapdata.inspect.InspectTask actual = inspectService.executeFieldInspect(inspect);
            Assertions.assertEquals(inspectTask,actual);
        }
    }
    @Nested
    class SubmitTaskTest{
        @Test
        void testSubmitTaskNormal(){
            inspectService = spy(inspectService);
            ConcurrentHashMap map = mock(ConcurrentHashMap.class);
            ReflectionTestUtils.setField(inspectService,"RUNNING_INSPECT",map);
            io.tapdata.inspect.InspectTask inspectTask = mock(io.tapdata.inspect.InspectTask.class);
            inspectService.submitTask(inspectTask);
            verify(inspectService).submitTask(inspectTask);
        }
        @Test
        void testSubmitTaskWithNullTask(){
            io.tapdata.inspect.InspectTask inspectTask = null;
            Future<?> actual = inspectService.submitTask(inspectTask);
            Assertions.assertEquals(null,actual);
        }
    }
    @Nested
    class CheckRowCountInspectTest{
        private Inspect inspect;
        private InspectDataSource inspectDataSource;
        private List<String> excepted = new ArrayList<>();
        @BeforeEach
        void buildInspect(){
            inspect = new Inspect();
            inspect.setStatus("scheduling");
            InspectTask inspectTask = new InspectTask();
            inspectTask.setTaskId("11111");
            inspectDataSource = new InspectDataSource();
            inspectDataSource.setConnectionId("1");
            inspectDataSource.setTable("table1");
            inspectDataSource.setDirection("ASC");
            inspectDataSource.setSortColumn("a1");
            inspectTask.setSource(inspectDataSource);
            inspectTask.setTarget(inspectDataSource);
            List<InspectTask> inspectTasks = new ArrayList<>();
            inspectTasks.add(inspectTask);
            inspect.setTasks(inspectTasks);
            inspect.setInspectMethod("row_count");
        }
        @Test
        void testCheckRowCountInspectNormal(){
            String exceptedInspectMethod = "row_count";
            String exceptedDirection = "ASC";
            List<String> actual = inspectService.checkFieldInspect(inspect);
            Assertions.assertEquals(excepted,actual);
            Assertions.assertEquals(exceptedInspectMethod,inspect.getInspectMethod());
            Assertions.assertEquals(exceptedDirection,inspectDataSource.getDirection());
            excepted.clear();
        }
        @Test
        void testCheckRowCountInspectWithNullInspect(){
            inspect = null;
            excepted.add("Inspect can not be empty.");
            List<String> actual = inspectService.checkRowCountInspect(inspect);
            Assertions.assertEquals(excepted,actual);
            excepted.clear();
        }
        @Test
        void testCheckRowCountInspectWithSubTaskIsEmpty(){
            inspect.setTasks(new ArrayList<>());
            excepted.add("Inspect sub-task can not be empty.");
            List<String> actual = inspectService.checkRowCountInspect(inspect);
            Assertions.assertEquals(excepted,actual);
        }
        @Test
        void testCheckRowCountInspectWithDataSourceIsNull(){
            inspect.getTasks().get(0).setSource(null);
            excepted.add("Inspect.tasks[0].source.inspectDataSource can not be null.");
            List<String> actual = inspectService.checkRowCountInspect(inspect);
            Assertions.assertEquals(excepted,actual);
            excepted.clear();
        }
    }
    @Nested
    class CheckRowCountInspectTaskDataSourceTest{
        private InspectDataSource dataSource;
        private List<String> excepted;
        private String prefix;
        @BeforeEach
        void buildDataSource() {
            dataSource = new InspectDataSource();
            dataSource.setConnectionId("111");
            dataSource.setTable("table1");
            excepted = new ArrayList<>();
            prefix = "test";
        }
        @Test
        void testCheckRowCountInspectTaskDataSourceNormal(){
            List<String> actual = inspectService.checkRowCountInspectTaskDataSource(prefix, dataSource);
            Assertions.assertEquals(excepted,actual);
        }
        @Test
        void testCheckRowCountInspectTaskDataSourceWithDataSourceIsNull() {
            dataSource = null;
            excepted.add("test.inspectDataSource can not be null.");
            List<String> actual = inspectService.checkRowCountInspectTaskDataSource(prefix, dataSource);
            Assertions.assertEquals(excepted,actual);
        }
        @Test
        void testCheckRowCountInspectTaskDataSourceWithConnAndTableIsNull() {
            dataSource.setConnectionId(null);
            dataSource.setTable(null);
            excepted.add("test.connectionId can not be empty.");
            excepted.add("test.table can not be empty.");
            List<String> actual = inspectService.checkRowCountInspectTaskDataSource(prefix, dataSource);
            Assertions.assertEquals(excepted,actual);
        }
    }
    @Nested
    class CheckFieldInspectTest{
        private Inspect inspect;
        private InspectDataSource inspectDataSource;
        private List<String> excepted = new ArrayList<>();
        @BeforeEach
        void buildInspect(){
            inspect = new Inspect();
            inspect.setStatus("scheduling");
            InspectTask inspectTask = new InspectTask();
            inspectTask.setTaskId("11111");
            inspectDataSource = new InspectDataSource();
            inspectDataSource.setConnectionId("1");
            inspectDataSource.setTable("table1");
            inspectDataSource.setDirection("ASC");
            inspectDataSource.setSortColumn("a1");
            inspectTask.setSource(inspectDataSource);
            inspectTask.setTarget(inspectDataSource);
            List<InspectTask> inspectTasks = new ArrayList<>();
            inspectTasks.add(inspectTask);
            inspect.setTasks(inspectTasks);
            inspect.setInspectMethod("field");
        }
        @Test
        void testCheckFieldInspectNormal() {
            String exceptedInspectMethod = "field";
            String exceptedDirection = "ASC";
            List<String> actual = inspectService.checkFieldInspect(inspect);
            Assertions.assertEquals(excepted,actual);
            Assertions.assertEquals(exceptedInspectMethod,inspect.getInspectMethod());
            Assertions.assertEquals(exceptedDirection,inspectDataSource.getDirection());
            excepted.clear();
        }
        @Test
        void testCheckFieldInspectWithInspectIsNull() {
            Inspect inspect = null;
            List<String> actual = inspectService.checkFieldInspect(inspect);
            excepted.add("Inspect can not be empty.");
            Assertions.assertEquals(excepted,actual);
            excepted.clear();
        }
        @Test
        void testCheckFieldInspectWithNotScheduling() {
            inspect.setStatus(null);
            List<String> actual = inspectService.checkFieldInspect(inspect);
            excepted.add("Inspect status must be scheduling");
            Assertions.assertEquals(excepted,actual);
            excepted.clear();
        }
        @Test
        void testCheckFieldInspectWithTaskIsNull() {
            inspect.setTasks(new ArrayList<>());
            List<String> actual = inspectService.checkFieldInspect(inspect);
            excepted.add("Inspect sub-task can not be empty.");
            Assertions.assertEquals(excepted,actual);
            excepted.clear();
        }
        @Test
        void testCheckFieldInspectWithDataSourceIsNull(){
            inspect.getTasks().get(0).setSource(null);
            excepted.add("Inspect.tasks[0].source.inspectDataSource can not be null.");
            List<String> actual = inspectService.checkFieldInspect(inspect);
            Assertions.assertEquals(excepted,actual);
            excepted.clear();
        }

    }
    @Nested
    class CheckFieldInspectTaskDataSourceTest{
        private List<String> excepted = new ArrayList<>();
        @Test
        void testCheckFieldInspectTaskDataSourceNormal() {
            String prefix = "test";
            InspectDataSource dataSource = new InspectDataSource();
            dataSource.setConnectionId("11111");
            dataSource.setTable("table1");
            dataSource.setDirection("ASC");
            dataSource.setSortColumn("a1");
            List<String> excepted = new ArrayList<>();
            String exceptedDirection = "ASC";
            List<String> actual = inspectService.checkFieldInspectTaskDataSource(prefix, dataSource);
            Assertions.assertEquals(excepted,actual);
            Assertions.assertEquals(exceptedDirection,dataSource.getDirection());
        }
        @Test
        void testCheckFieldInspectTaskDataSourceWithDataSourceIsNull() {
            String prefix = "test";
            InspectDataSource dataSource = null;
            List<String> actual = inspectService.checkFieldInspectTaskDataSource(prefix, dataSource);
            excepted.add(prefix + ".inspectDataSource can not be null.");
            Assertions.assertEquals(excepted,actual);
        }
        @Test
        void testCheckFieldInspectTaskDataSourceWithEmptyValue() {
            String prefix = "test";
            InspectDataSource dataSource = new InspectDataSource();
            dataSource.setConnectionId("");
            dataSource.setTable(null);
            List<String> excepted = new ArrayList<>();
            String exceptedDirection = "DESC";
            excepted.add("test.connectionId can not be empty.");
            excepted.add("test.table can not be empty.");
            excepted.add("test.sortColumn can not be empty.");
            List<String> actual = inspectService.checkFieldInspectTaskDataSource(prefix, dataSource);
            Assertions.assertEquals(excepted,actual);
            Assertions.assertEquals(exceptedDirection,dataSource.getDirection());
        }
    }
    @Nested
    class InspectHeartBeatTest{
        @Test
        void testInspectHeartBeatNormal() {
            String id = "1111";
            inspectService = spy(inspectService);
            inspectService.inspectHeartBeat(id);
            verify(inspectService).inspectHeartBeat(id);
        }
        @Test
        void testInspectHeartBeatWithIdIsNull() {
            String id = null;
            Assertions.assertThrows(IllegalArgumentException.class,()->inspectService.inspectHeartBeat(id));
        }
    }
}