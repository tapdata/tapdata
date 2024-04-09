package com.tapdata.tm.ws.handler;


import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.extern.slf4j.Slf4j;

@WebSocketMessageHandler(type = MessageType.DOWNLOAD_CONNECTOR)
@Slf4j
public class DownLoadConnectorHandler implements WebSocketHandler {

    private final MessageQueueService messageQueueService;

    private final DataSourceService dataSourceService;

    private final DataSourceDefinitionService dataSourceDefinitionService;

    private final UserService userService;

    private final WorkerService workerService;

    AgentGroupService agentGroupService;

    public DownLoadConnectorHandler(AgentGroupService agentGroupService, MessageQueueService messageQueueService, DataSourceService dataSourceService, UserService userService
            , WorkerService workerService, DataSourceDefinitionService dataSourceDefinitionService) {
        this.messageQueueService = messageQueueService;
        this.dataSourceService = dataSourceService;
        this.userService = userService;
        this.workerService = workerService;
        this.dataSourceDefinitionService = dataSourceDefinitionService;
        this.agentGroupService = agentGroupService;
    }

    @Override
    public void handleMessage(WebSocketContext context) throws Exception {
        TestConnectionHandler testConnectionHandler = new TestConnectionHandler(agentGroupService, messageQueueService,dataSourceService,userService,workerService,dataSourceDefinitionService);
        testConnectionHandler.handleMessage(context);
    }
}
