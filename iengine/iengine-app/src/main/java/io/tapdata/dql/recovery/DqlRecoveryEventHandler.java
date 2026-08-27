package io.tapdata.dql.recovery;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import io.tapdata.dql.client.DqlTmClient;
import com.tapdata.tm.dql.config.DqlRuntimeConfig;
import io.tapdata.dql.model.DqlRecoveryReport;
import io.tapdata.dql.reporter.DqlEventReporter;
import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.WebSocketEventResult;
import io.tapdata.websocket.handler.BaseEventHandler;

import java.util.Map;

/** WebSocket adapter for the TM -> Engine dqlRecovery pipe message. */
@EventHandlerAnnotation(type = "dqlRecovery")
public class DqlRecoveryEventHandler extends BaseEventHandler {
    private DqlRecoveryMessageHandler messageHandler;

    public DqlRecoveryEventHandler() {
    }

    public DqlRecoveryEventHandler(DqlRecoveryMessageHandler messageHandler) {
        this.messageHandler = messageHandler;
    }

    @Override
    public void initialize(ClientMongoOperator clientMongoOperator) {
        initialize(clientMongoOperator, null);
    }

    @Override
    public void initialize(ClientMongoOperator clientMongoOperator,
                           io.tapdata.common.SettingService settingService) {
        super.initialize(clientMongoOperator, settingService);
        if (messageHandler != null) {
            return;
        }
        ConfigurationCenter configurationCenter = BeanUtil.getBean(ConfigurationCenter.class);
        String currentAgentId = configurationCenter == null
                ? null
                : (String) configurationCenter.getConfig(ConfigurationCenter.AGENT_ID);
        DqlRecoveryCoordinator coordinator = BeanUtil.getBean(DqlRecoveryCoordinator.class);
        DqlRecoveryReportSender reportSender = recoveryReportSender(clientMongoOperator);
        DqlRuntimeConfig runtimeConfig = DqlRuntimeConfig.from(key ->
                settingService == null ? null : settingService.getString(key, null));
        messageHandler = new DqlRecoveryMessageHandler(
                coordinator,
                reportSender,
                new MongoDqlRecoveryTaskContextProvider(clientMongoOperator),
                currentAgentId,
                DqlRecoveryBatchRegistry.global(),
                runtimeConfig.getRecoveryBatchMaxSize()
        );
    }

    @Override
    public Object handle(Map event) {
        if (messageHandler == null) {
            return WebSocketEventResult.handleFailed(
                    WebSocketEventResult.Type.DQL_RECOVERY_RESULT,
                    "recovery message handler is not initialized"
            );
        }
        DqlRecoveryHandleResult result = messageHandler.handle(event);
        if (result.getOutcome() == DqlRecoveryHandleResult.Outcome.REJECTED) {
            return WebSocketEventResult.handleFailed(
                    WebSocketEventResult.Type.DQL_RECOVERY_RESULT,
                    result.getMessage()
            );
        }
        return WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.DQL_RECOVERY_RESULT, result);
    }

    private DqlRecoveryReportSender recoveryReportSender(ClientMongoOperator clientMongoOperator) {
        if (!(clientMongoOperator instanceof HttpClientMongoOperator httpClientMongoOperator)) {
            return null;
        }
        DqlEventReporter reporter = new DqlEventReporter(new DqlTmClient(httpClientMongoOperator));
        return (command, report) -> reporter.reportRecovery(command.getTaskId(), report);
    }
}
