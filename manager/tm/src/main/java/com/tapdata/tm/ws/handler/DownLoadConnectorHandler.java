package com.tapdata.tm.ws.handler;


import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.Map;
import java.util.function.Consumer;

@WebSocketMessageHandler(type = MessageType.DOWNLOAD_PDK_FILE_FLAG)
@Slf4j
public class DownLoadConnectorHandler implements WebSocketHandler {
    private final static int MAX_SIZE = 1000000;
    private static final LinkedList cacheList;

    private final MessageQueueService messageQueueService;

    private final DataSourceService dataSourceService;

    private final DataSourceDefinitionService dataSourceDefinitionService;

    private final UserService userService;

    private final WorkerService workerService;
    static {
        cacheList = new LinkedList<Map<String,Object>>();
    }

    public DownLoadConnectorHandler(MessageQueueService messageQueueService, DataSourceService dataSourceService, UserService userService
            , WorkerService workerService, DataSourceDefinitionService dataSourceDefinitionService) {
        this.messageQueueService = messageQueueService;
        this.dataSourceService = dataSourceService;
        this.userService = userService;
        this.workerService = workerService;
        this.dataSourceDefinitionService = dataSourceDefinitionService;
    }

    @Override
    public void handleMessage(WebSocketContext context) throws Exception {
        if(null == context) return;
        synchronized (cacheList) {
            if (cacheList.size() >= MAX_SIZE) {
                cacheList.remove(0);
            }
            cacheList.add(context.getMessageInfo().getData());
        }
    }

    public static boolean handleResponse(String pingId, Consumer<Map<String, Object>> consumer) {
        Map<String, Object> cache;
        synchronized (cacheList) {
        }
        return false;
    }

//    @Override
//    public void handleMessage(WebSocketContext context) throws Exception {
//        TestConnectionHandler testConnectionHandler = new TestConnectionHandler(messageQueueService,dataSourceService,userService,workerService,dataSourceDefinitionService);
//        testConnectionHandler.handleMessage(context);
//    }
}
