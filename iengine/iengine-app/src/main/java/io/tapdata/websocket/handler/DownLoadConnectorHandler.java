package io.tapdata.websocket.handler;

import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.metrics.ConnectorRecordDto;
import io.tapdata.aspect.supervisor.AspectRunnableUtil;
import io.tapdata.aspect.supervisor.DisposableThreadGroupAspect;
import io.tapdata.aspect.supervisor.entity.ConnectionTestEntity;
import io.tapdata.callback.DownloadCallback;
import io.tapdata.callback.impl.DownloadCallbackImpl;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.threadgroup.DisposableThreadGroup;
import io.tapdata.threadgroup.utils.DisposableType;
import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.SendMessage;
import io.tapdata.websocket.WebSocketEventHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
@EventHandlerAnnotation(type = "downLoadConnector")
public class DownLoadConnectorHandler extends BaseEventHandler implements WebSocketEventHandler {
    private final static Logger logger = LogManager.getLogger(DownLoadConnectorHandler.class);

    @Override
    public Object handle(Map event, SendMessage sendMessage) {
        logger.info(String.format("downLoad connector, event: %s", event));
        String pskHash = (String) event.getOrDefault("pdkHash", "");
        String connName = (String) event.getOrDefault("name", "");
        String connectionId = String.valueOf(event.get("id"));
        ConnectionTestEntity entity = new ConnectionTestEntity()
                .associateId(UUID.randomUUID().toString())
                .time(System.nanoTime())
                .connectionId(connectionId)
                .type(String.valueOf(event.get("type")))
                .connectionName(connName)
                .pdkType(String.valueOf(event.get("pdkType")))
                .pdkHash(pskHash)
                .schemaVersion(String.valueOf(event.get("schemaVersion")))
                .databaseType(String.valueOf(event.get("database_type")));
        String threadName = String.format("DOWNLOAD-CONNECTOR-%s", Optional.ofNullable(event.get("name")).orElse(""));
        DisposableThreadGroup threadGroup = new DisposableThreadGroup(DisposableType.DOWNLOAD_CONNECTOR, threadName);
        DatabaseTypeEnum.DatabaseType databaseDefinition = ConnectionUtil.getDatabaseType(clientMongoOperator, pskHash);
        DownloadCallback callback=new DownloadCallbackImpl(databaseDefinition,connectionId,clientMongoOperator);
        Runnable runnable = AspectRunnableUtil.aspectRunnable(new DisposableThreadGroupAspect<>(connectionId, threadGroup, entity), () -> {
            downloadPdkFileIfNeedPrivate(event, connName, connectionId, databaseDefinition, callback);
        });
        Thread thread = new Thread(threadGroup, runnable, threadName);
        thread.start();
        return thread;
    }

    protected void downloadPdkFileIfNeedPrivate(Map event, String connName, String connectionId, DatabaseTypeEnum.DatabaseType databaseDefinition, DownloadCallback callback) {
        try{
            PdkUtil.downloadPdkFileIfNeed((HttpClientMongoOperator) clientMongoOperator, databaseDefinition.getPdkHash(), databaseDefinition.getJarFile(), databaseDefinition.getJarRid(), callback);
        } catch (Exception e){
            String errMsg = String.format("Download connector %s failed, data: %s, err: %s", connName, event, e.getMessage());
            logger.error(errMsg, e);
            ConnectorRecordDto connectorRecordDto = new ConnectorRecordDto();
            connectorRecordDto.setConnectionId(connectionId);
            connectorRecordDto.setStatus(ConnectorRecordDto.StatusEnum.FAIL.getStatus());
            connectorRecordDto.setDownFiledMessage(e.getMessage());
            connectorRecordDto.setFlag(true);
            ((DownloadCallbackImpl) callback).upsertConnectorRecord(connectorRecordDto);
        }
    }


}
