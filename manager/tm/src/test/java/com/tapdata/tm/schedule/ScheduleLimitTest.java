package com.tapdata.tm.schedule;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.service.impl.TaskScheduleServiceImpl;
import com.tapdata.tm.worker.dto.TcmInfo;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.BeanUtils;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class ScheduleLimitTest {


    /**
     * test handleScheduleLimit function
     * 测试调度数量限制抛出异常
     */
    @Test
    public void testHandleScheduleLimitThrowScheduleLimitException(){
        // input param

        // use Mockito mock data
        WorkerService workerService = mock(WorkerService.class);
        TaskService taskService = mock(TaskService.class);
        UserDetail mockUserDetail = mock(UserDetail.class);

        // mock data
        Worker worker = getWorker("worker1");
        List<Worker> listWorker = new ArrayList<>();
        listWorker.add(worker);

        // use Mockito mock function   return data
        Mockito.when(workerService.findAvailableAgent(mockUserDetail)).thenReturn(listWorker);

        // input function  param
        TaskScheduleServiceImpl taskScheduleService = new TaskScheduleServiceImpl();
        setParamForObject(taskScheduleService,taskService,workerService);

        WorkerDto workerDto = new WorkerDto();
        BeanUtils.copyProperties(worker,workerDto);

        // execution method
        Exception exception = assertThrows(BizException.class, () -> {
            taskScheduleService.handleScheduleLimit(workerDto,mockUserDetail);
        });

        // actual data
        String actualData =  ((BizException) exception).getErrorCode();

        // expected data
        String expectedData = "Task.ScheduleLimit";

        // output results
        assertEquals(expectedData,actualData);
    }


    /**
     * test handleScheduleLimit function
     * 测试调度数量限制只有一个引擎且不是当前任务指定的引擎则提示替换引擎
     */
    @Test
    public void testHandleScheduleLimitThrowManuallyScheduleLimitExceptionWithOneEngine() throws BizException {
        // input param

        // use Mockito mock data
        WorkerService workerService = mock(WorkerService.class);
        TaskService taskService = mock(TaskService.class);
        UserDetail mockUserDetail = mock(UserDetail.class);

        // mock data
        Worker worker = getWorker("worker1");
        List<Worker> listWorker = new ArrayList<>();
        listWorker.add(worker);

        WorkerDto workerDto = new WorkerDto();
        Worker workerTemp = getWorker("worker2");
        BeanUtils.copyProperties(workerTemp,workerDto);

        // use Mockito mock function   return data
        Mockito.when(workerService.findAvailableAgent(mockUserDetail)).thenReturn(listWorker);

        // input function  param
        TaskScheduleServiceImpl taskScheduleService = new TaskScheduleServiceImpl();
        setParamForObject(taskScheduleService,taskService,workerService);


        // execution method
        Exception exception = assertThrows(BizException.class, () -> {
            taskScheduleService.handleScheduleLimit(workerDto,mockUserDetail);
        });

        // actual data
        String actualData =  ((BizException) exception).getErrorCode();

        // expected data
        String expectedData = "Task.ManuallyScheduleLimit";

        // output results
        assertEquals(expectedData,actualData);
    }


    /**
     * test handleScheduleLimit function
     * 测试调度数量限制.存在多个引擎且不是当前任务指定的引擎则提示替换引擎
     */
    @Test
    public void testHandleScheduleLimitThrowScheduleLimitExceptionWithManyEngine() throws BizException {
        // input param

        // use Mockito mock data
        WorkerService workerService = mock(WorkerService.class);
        TaskService taskService = mock(TaskService.class);
        UserDetail mockUserDetail = mock(UserDetail.class);

        // mock data
        Worker worker =getWorker("worker1");
        Worker workerTemp =getWorker("worker2");
        workerTemp.setProcessId("worker2");
        TcmInfo tcmInfo =  mock(TcmInfo.class);
        List<Worker> listWorker = new ArrayList<>();
        listWorker.add(worker);
        listWorker.add(workerTemp);

        // use Mockito mock function   return data
        Mockito.when(tcmInfo.getAgentName()).thenReturn("worker2");
        Mockito.when(workerService.findAvailableAgent(mockUserDetail)).thenReturn(listWorker);

        // input function  param
        TaskScheduleServiceImpl taskScheduleService = new TaskScheduleServiceImpl();
        setParamForObject(taskScheduleService,taskService,workerService);
        WorkerDto workerDto = new WorkerDto();
        BeanUtils.copyProperties(workerTemp,workerDto);

        // execution method
        Exception exception = assertThrows(BizException.class, () -> {
            taskScheduleService.handleScheduleLimit(workerDto,mockUserDetail);
        });

        // actual data
        String actualData =  ((BizException) exception).getErrorCode();

        // expected data
        String expectedData = "Task.ManuallyScheduleLimit";

        // output results
        assertEquals(expectedData,actualData);
    }


    public Worker getWorker(String processId) {
        Worker worker = new Worker();
        worker.setProcessId(processId);
        return worker;
    }


    public void  setParamForObject(TaskScheduleServiceImpl taskScheduleService,
                                   TaskService taskService,WorkerService workerService){

        taskScheduleService.setTaskService(taskService);
        taskScheduleService.setWorkerService(workerService);

    }


}
