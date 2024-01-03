package io.tapdata.websocket.handler;

import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.mongo.RestTemplateOperator;
import com.tapdata.tm.commons.metrics.ConnectorRecordDto;
import io.tapdata.aspect.supervisor.AspectRunnableUtil;
import io.tapdata.aspect.supervisor.DisposableThreadGroupAspect;
import io.tapdata.aspect.supervisor.entity.ConnectionTestEntity;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.threadgroup.DisposableThreadGroup;
import io.tapdata.threadgroup.utils.DisposableType;
import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.SendMessage;
import io.tapdata.websocket.WebSocketEventHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import java.util.HashMap;
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
        RestTemplateOperator.Callback callback=new RestTemplateOperator.Callback() {
            @Override
            public void needDownloadPdkFile(boolean flag) throws Exception {
                logger.info("Whether to start downloading the pdk file {}",flag);
                ConnectorRecordDto connectorRecordDto = new ConnectorRecordDto();
                connectorRecordDto.setConnectionId(connectionId);
                connectorRecordDto.setPdkHash(databaseDefinition.getPdkHash());
                connectorRecordDto.setFlag(flag);
                upsertConnectorRecord(connectorRecordDto);
            }

            @Override
            public void onProgress(long fileSize,long progress) throws Exception{
                ConnectorRecordDto connectorRecordDto = new ConnectorRecordDto();
                connectorRecordDto.setConnectionId(connectionId);
                connectorRecordDto.setFileSize(fileSize);
                connectorRecordDto.setProgress(progress);
                connectorRecordDto.setPdkHash(databaseDefinition.getPdkHash());
                connectorRecordDto.setStatus(ConnectorRecordDto.StatusEnum.DOWNLOADING.getStatus());
                connectorRecordDto.setFlag(true);
                upsertConnectorRecord(connectorRecordDto);
            }

            @Override
            public void onFinish(String downloadSpeed) throws Exception{
                logger.info("Downloading the pdk file is completed at a speed of {}.",downloadSpeed);
                ConnectorRecordDto connectorRecordDto = new ConnectorRecordDto();
                connectorRecordDto.setConnectionId(connectionId);
                connectorRecordDto.setStatus(ConnectorRecordDto.StatusEnum.FINISH.getStatus());
                connectorRecordDto.setDownloadSpeed(downloadSpeed);
                connectorRecordDto.setFlag(false);
                upsertConnectorRecord(connectorRecordDto);
            }

            @Override
            public void onError(Exception ex) throws Exception{
                logger.error("The reason why downloading the pdk file failed is {}.",ex.getMessage());
                ConnectorRecordDto connectorRecordDto = new ConnectorRecordDto();
                connectorRecordDto.setConnectionId(connectionId);
                connectorRecordDto.setStatus(ConnectorRecordDto.StatusEnum.FAIL.getStatus());
                connectorRecordDto.setDownFiledMessage(ex.getMessage());
                connectorRecordDto.setFlag(true);
                upsertConnectorRecord(connectorRecordDto);
                throw new RuntimeException("Download connector failed ",ex);
            }
        };
        Runnable runnable = AspectRunnableUtil.aspectRunnable(new DisposableThreadGroupAspect<>(connectionId, threadGroup, entity), () -> {
            try{
                PdkUtil.downloadPdkFileIfNeed((HttpClientMongoOperator) clientMongoOperator, databaseDefinition.getPdkHash(), databaseDefinition.getJarFile(), databaseDefinition.getJarRid(), callback);
            } catch (Exception e){
                String errMsg = String.format("Download connector %s failed, data: %s, err: %s", connName, event, e.getMessage());
                logger.error(errMsg, e);
                try {
                    ConnectorRecordDto connectorRecordDto = new ConnectorRecordDto();
                    connectorRecordDto.setConnectionId(connectionId);
                    connectorRecordDto.setStatus(ConnectorRecordDto.StatusEnum.FAIL.getStatus());
                    connectorRecordDto.setDownFiledMessage(e.getMessage());
                    connectorRecordDto.setFlag(true);
                    upsertConnectorRecord(connectorRecordDto);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        });
        Thread thread = new Thread(threadGroup, runnable, threadName);
        thread.start();
        return thread;
    }

    protected void upsertConnectorRecord(ConnectorRecordDto connectorRecordDto) throws IllegalAccessException {
        HashMap<String, Object> queryMap = new HashMap<>();
        queryMap.put("connectionId", connectorRecordDto.getConnectionId());
        clientMongoOperator.upsert(queryMap, MapUtil.obj2Map(connectorRecordDto), ConnectorConstant.CONNECTORRECORD_COLLECTION);
    }

}
