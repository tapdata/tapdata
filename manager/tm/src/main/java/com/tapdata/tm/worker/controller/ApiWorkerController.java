package com.tapdata.tm.worker.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.worker.dto.ApiServerInfo;
import com.tapdata.tm.worker.service.ApiWorkerServer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/4 10:24 Create
 * @description
 */
@Slf4j
@RestController
@RequestMapping("/api/api-server-worker")
public class ApiWorkerController extends BaseController {
    @Autowired
    private ApiWorkerServer apiWorkerServer;

    /**
     * get worker's info by processId
     * */
    @GetMapping()
    public ResponseMessage<ApiServerInfo> getWorkers(@RequestParam(name = "processId", required = true) String processId) {
        return success(apiWorkerServer.getWorkers(processId));
    }

    /**
     * get all api-server's info (cup & memory usage)
     * */
    @GetMapping("/cpu-mem")
    public ResponseMessage<List<ApiServerInfo>> getAllWorkers() {
        return success(apiWorkerServer.getApiServerWorkerInfo());
    }

    @DeleteMapping("/delete")
    public ResponseMessage<Void> delete(@RequestParam(name = "processId", required = true) String processId) {
        apiWorkerServer.delete(processId, null);
        return success();
    }

    @DeleteMapping("/delete-worker")
    public ResponseMessage<Void> deleteWorker(@RequestParam(name = "processId", required = true) String processId,
                                           @RequestParam(name = "processId", required = true) String workerOid) {
        apiWorkerServer.delete(processId, workerOid);
        return success();
    }
}
