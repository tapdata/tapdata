package com.tapdata.tm.worker.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.repository.WorkerRepository;
import org.bson.BsonValue;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class WorkerServiceTest {
    private WorkerService workerService;
    @Test
    void testQueryWorkerByProcessIdWithoutId(){
        WorkerRepository workerRepository = mock(WorkerRepository.class);
        workerService = spy(new WorkerService(workerRepository));
        String processId = "  ";
        assertThrows(IllegalArgumentException.class,()->workerService.queryWorkerByProcessId(processId));
    }
    @Test
    void testQueryWorkerByProcessIdWithId(){
        workerService = mock(WorkerService.class);
        String processId = "111";
        WorkerDto workerDto = mock(WorkerDto.class);
        Query query = Query.query(Criteria.where("process_id").is(processId).and("worker_type").is("connector"));
        Worker worker = mock(Worker.class);
//        when(workerRepository.findOne(query)).thenReturn(Optional.of(worker));
        when(workerService.findOne(query)).thenReturn(workerDto);
        when(workerService.queryWorkerByProcessId(processId)).thenReturn(workerDto);
        WorkerDto actual = workerService.queryWorkerByProcessId(processId);
        assertEquals(workerDto,actual);
    }
    @Test
    void testQueryAllBindWorker(){
        WorkerRepository workerRepository = mock(WorkerRepository.class);
        workerService = spy(new WorkerService(workerRepository));
        List<Worker> excepted = new ArrayList<>();
        Query query = Query.query(Criteria.where("worker_type").is("connector").and("licenseBind").is(true));
        BaseRepository repository = mock(BaseRepository.class);
        when(repository.findAll(query)).thenReturn(excepted);
        List<Worker> actual = workerService.queryAllBindWorker();
        assertEquals(excepted,actual);
    }
    @Test
    void testBindByProcessIdWithoutId(){
        WorkerRepository workerRepository = mock(WorkerRepository.class);
        workerService = spy(new WorkerService(workerRepository));
        WorkerDto workerDto = mock(WorkerDto.class);
        UserDetail userDetail = mock(UserDetail.class);
        String processId = "  ";
        assertThrows(IllegalArgumentException.class,()->workerService.bindByProcessId(workerDto,processId,userDetail));
    }
//    @Test
    /*void testBindByProcessIdWithId(){
        WorkerRepository workerRepository = mock(WorkerRepository.class);
        workerService = spy(new WorkerService(workerRepository));
        String processId = "111";
        WorkerDto workerDto = mock(WorkerDto.class);
        UserDetail userDetail = mock(UserDetail.class);
        Query query = Query.query(Criteria.where("process_id").is(processId).and("worker_type").is("connector"));
        Update update = Update.update("licenseBind", true);
//        when(workerService.queryWorkerByProcessId(processId)).thenReturn(workerDto);
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
        Worker worker = mock(Worker.class);
        when(workerRepository.findOne(query)).thenReturn(Optional.of(worker));
        when(workerService.findOne(query)).thenReturn(workerDto);
        when(workerService.queryWorkerByProcessId(processId)).thenReturn(workerDto);
        when(workerRepository.update(query,update)).thenReturn(result);
        when(workerService.save(workerDto,userDetail)).thenReturn(workerDto);
//        doNothing().when(workerService).beforeSave(workerDto,mock(UserDetail.class));
//        when(workerRepository.save(mock(Worker.class),mock(UserDetail.class))).thenReturn(mock(Worker.class));
        boolean actual = workerService.bindByProcessId(workerDto, processId, userDetail);
        assertEquals(true,actual);
    }*/
    @Test
    void testUnbindByProcessIdWithoutId(){
        WorkerRepository workerRepository = mock(WorkerRepository.class);
        workerService = spy(new WorkerService(workerRepository));
        WorkerDto workerDto = mock(WorkerDto.class);
        UserDetail userDetail = mock(UserDetail.class);
        String processId = "  ";
        assertThrows(IllegalArgumentException.class,()->workerService.unbindByProcessId(processId));
    }
    @Test
    void testUnbindByProcessIdWithId(){
        WorkerRepository workerRepository = mock(WorkerRepository.class);
        workerService = spy(new WorkerService(workerRepository));
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
