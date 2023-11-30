package com.tapdata.tm.worker.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.repository.WorkerRepository;
import org.bson.BsonValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class WorkerServiceTest {
    private WorkerService workerService;
    private WorkerRepository workerRepository;
    @BeforeEach
    void buildWorkService(){
        workerRepository = mock(WorkerRepository.class);
        workerService = spy(new WorkerService(workerRepository));
    }
    @Test
    void testQueryWorkerByProcessIdWithoutId(){
        String processId = "  ";
        assertThrows(IllegalArgumentException.class,()->workerService.queryWorkerByProcessId(processId));
    }
    @Test
    void testQueryWorkerByProcessIdWithId(){
        String processId = "111";
        WorkerDto workerDto = mock(WorkerDto.class);
        Query query = Query.query(Criteria.where("process_id").is(processId).and("worker_type").is("connector"));
        doReturn(workerDto).when(workerService).findOne(query);
        WorkerDto actual = workerService.queryWorkerByProcessId(processId);
        assertEquals(workerDto,actual);
    }
    @Test
    void testQueryAllBindWorker(){
        List<Worker> excepted = new ArrayList<>();
        Query query = Query.query(Criteria.where("worker_type").is("connector").and("licenseBind").is(true));
        BaseRepository repository = mock(BaseRepository.class);
        when(repository.findAll(query)).thenReturn(excepted);
        List<Worker> actual = workerService.queryAllBindWorker();
        assertEquals(excepted,actual);
    }
    @Test
    void testBindByProcessIdWithoutId(){
        WorkerDto workerDto = mock(WorkerDto.class);
        UserDetail userDetail = mock(UserDetail.class);
        String processId = "  ";
        assertThrows(IllegalArgumentException.class,()->workerService.bindByProcessId(workerDto,processId,userDetail));
    }
    @Test
    void testBindByProcessIdWithId(){
        String processId = "111";
        WorkerDto workerDto = mock(WorkerDto.class);
        UserDetail userDetail = mock(UserDetail.class);
        Query query = Query.query(Criteria.where("process_id").is(processId).and("worker_type").is("connector"));
        Update update = Update.update("licenseBind", true);
        UpdateResult result = new UpdateResult() {
            @Override
            public boolean wasAcknowledged() {
                return false;
            }
            @Override
            public long getMatchedCount() {
                return 1;
            }
            @Override
            public long getModifiedCount() {
                return 1;
            }
            @Override
            public BsonValue getUpsertedId() {
                return null;
            }
        };
        doReturn(workerDto).when(workerService).queryWorkerByProcessId(processId);
        doReturn(result).when(workerRepository).update(query,update);
        boolean actual = workerService.bindByProcessId(workerDto, processId, userDetail);
        assertEquals(true,actual);
    }
    @Test
    void testBindByProcessIdWithNullResult(){
        String processId = "111";
        WorkerDto workerDto = mock(WorkerDto.class);
        UserDetail userDetail = mock(UserDetail.class);
        Query query = Query.query(Criteria.where("process_id").is(processId).and("worker_type").is("connector"));
        Update update = Update.update("licenseBind", true);
        doReturn(workerDto).when(workerService).queryWorkerByProcessId(processId);
        doReturn(null).when(workerRepository).update(query,update);
        boolean actual = workerService.bindByProcessId(workerDto, processId, userDetail);
        assertEquals(false,actual);
    }
    @Test
    void testBindByProcessIdWithCount(){
        String processId = "111";
        WorkerDto workerDto = mock(WorkerDto.class);
        UserDetail userDetail = mock(UserDetail.class);
        Query query = Query.query(Criteria.where("process_id").is(processId).and("worker_type").is("connector"));
        Update update = Update.update("licenseBind", true);
        UpdateResult result = new UpdateResult() {
            @Override
            public boolean wasAcknowledged() {
                return false;
            }
            @Override
            public long getMatchedCount() {
                return 1;
            }
            @Override
            public long getModifiedCount() {
                return 0;
            }
            @Override
            public BsonValue getUpsertedId() {
                return null;
            }
        };
        doReturn(workerDto).when(workerService).queryWorkerByProcessId(processId);
        doReturn(result).when(workerRepository).update(query,update);
        boolean actual = workerService.bindByProcessId(workerDto, processId, userDetail);
        assertEquals(false,actual);
    }
    @Test
    void testBindByProcessIdWithIdSave(){
        String processId = "111";
        WorkerDto workerDto = mock(WorkerDto.class);
        UserDetail userDetail = mock(UserDetail.class);
        Query query = Query.query(Criteria.where("process_id").is(processId).and("worker_type").is("connector"));
        Update update = Update.update("licenseBind", true);
        UpdateResult result = new UpdateResult() {
            @Override
            public boolean wasAcknowledged() {
                return false;
            }
            @Override
            public long getMatchedCount() {
                return 1;
            }
            @Override
            public long getModifiedCount() {
                return 1;
            }
            @Override
            public BsonValue getUpsertedId() {
                return null;
            }
        };
        doReturn(null).when(workerService).queryWorkerByProcessId(processId);
        doReturn(workerDto).when(workerService).save(workerDto,userDetail);
        doReturn(result).when(workerRepository).update(query,update);
        boolean actual = workerService.bindByProcessId(workerDto, processId, userDetail);
        assertEquals(true,actual);
    }
    @Test
    void testUnbindByProcessIdWithoutId(){
        String processId = "  ";
        assertThrows(IllegalArgumentException.class,()->workerService.unbindByProcessId(processId));
    }
    @Test
    void testUnbindByProcessIdWithId(){
        String processId = "111";
        Query query = Query.query(Criteria.where("process_id").is(processId).and("worker_type").is("connector"));
        Update update = Update.update("licenseBind", false);
        UpdateResult result = new UpdateResult() {
            @Override
            public boolean wasAcknowledged() {
                return false;
            }
            @Override
            public long getMatchedCount() {
                return 1;
            }
            @Override
            public long getModifiedCount() {
                return 1;
            }
            @Override
            public BsonValue getUpsertedId() {
                return null;
            }
        };
        when(workerRepository.update(query,update)).thenReturn(result);
        boolean actual = workerService.unbindByProcessId(processId);
        assertEquals(true,actual);
    }
}
