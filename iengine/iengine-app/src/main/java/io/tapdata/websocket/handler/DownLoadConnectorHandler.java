package io.tapdata.websocket.handler;

import com.tapdata.constant.*;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.metrics.ConnectorRecordDto;
import io.tapdata.DownloadCallback;
import io.tapdata.aspect.supervisor.AspectRunnableUtil;
import io.tapdata.aspect.supervisor.DisposableThreadGroupAspect;
import io.tapdata.aspect.supervisor.entity.ConnectionTestEntity;

import io.tapdata.dao.ConnectorRecordFlagDto;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.threadgroup.DisposableThreadGroup;
import io.tapdata.threadgroup.utils.DisposableType;
import io.tapdata.websocket.*;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.socket.TextMessage;

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
        String pdkHash = (String) event.getOrDefault("pdkHash", "");
        String connName = (String) event.getOrDefault("name", "");
        String uId = (String) event.getOrDefault("uId", "");
        String connectionId = String.valueOf(event.get("id"));
        ConnectionTestEntity entity = new ConnectionTestEntity()
                .associateId(UUID.randomUUID().toString())
                .time(System.nanoTime())
                .connectionId(connectionId)
                .type(String.valueOf(event.get("type")))
                .connectionName(connName)
                .pdkType(String.valueOf(event.get("pdkType")))
                .pdkHash(pdkHash)
                .schemaVersion(String.valueOf(event.get("schemaVersion")))
                .databaseType(String.valueOf(event.get("database_type")));
        String threadName = String.format("DOWNLOAD-CONNECTOR-%s", Optional.ofNullable(event.get("name")).orElse(""));
        DisposableThreadGroup threadGroup = new DisposableThreadGroup(DisposableType.DOWNLOAD_CONNECTOR, threadName);
        DatabaseTypeEnum.DatabaseType databaseDefinition = ConnectionUtil.getDatabaseType(clientMongoOperator, pdkHash);
        DownloadCallback callback=new DownloadCallbackImpl(databaseDefinition,uId,clientMongoOperator);
        Runnable runnable = AspectRunnableUtil.aspectRunnable(new DisposableThreadGroupAspect<>(connectionId, threadGroup, entity), () -> {
            downloadPdkFileIfNeedPrivate(event, connName, connectionId, databaseDefinition, callback);
        });
        startThread( threadGroup, runnable,threadName);
        return null;
    }
    @Data
    class DownloadFlagDto{
        private String id;
        private String type;
        private Boolean flag;
    }

    protected Thread startThread(DisposableThreadGroup threadGroup,Runnable runnable,String threadName ) {
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
    class  DownloadCallbackImpl implements DownloadCallback{

        private final Logger logger = LogManager.getLogger(DownloadCallbackImpl.class);
        private DatabaseTypeEnum.DatabaseType databaseDefinition;
        private String connectionId;
        private ClientMongoOperator clientMongoOperator;

        private ManagementWebsocketHandler managementWebsocketHandler;

        public DatabaseTypeEnum.DatabaseType getDatabaseDefinition() {
            return databaseDefinition;
        }


        public void setDatabaseDefinition(DatabaseTypeEnum.DatabaseType databaseDefinition) {
            this.databaseDefinition = databaseDefinition;
        }

        public DownloadCallbackImpl() {
        }
        private String uid;

        public String getUid() {
            return uid;
        }

        public void setUid(String uid) {
            this.uid = uid;
        }

        public String getConnectionId() {
            return connectionId;
        }

        public void setConnectionId(String connectionId) {
            this.connectionId = connectionId;
        }

        public ClientMongoOperator getClientMongoOperator() {
            return clientMongoOperator;
        }

        public void setClientMongoOperator(ClientMongoOperator clientMongoOperator) {
            this.clientMongoOperator = clientMongoOperator;
        }

        public DownloadCallbackImpl(DatabaseTypeEnum.DatabaseType databaseDefinition, String connectionId, ClientMongoOperator clientMongoOperator) {
            this.databaseDefinition = databaseDefinition;
            this.connectionId = connectionId;
            this.clientMongoOperator = clientMongoOperator;
        }


        @Override
        public void needDownloadPdkFile(boolean flag) throws Exception {
            logger.info("Whether to start downloading the pdk file {}",flag);
            ConnectorRecordDto connectorRecordDto = new ConnectorRecordDto();
//            connectorRecordDto.setConnectionId(connectionId);
            connectorRecordDto.setPdkHash(databaseDefinition.getPdkHash());
            connectorRecordDto.setFlag(flag);
            connectorRecordDto.setProgress(0L);
            upsertConnectorRecord(connectorRecordDto);
            WebSocketEvent<ConnectorRecordFlagDto> webSocketEvent = new WebSocketEvent<>();
            webSocketEvent.setType("downloadPdkFileFlag");
            ConnectorRecordFlagDto connectorRecordFlagDto = new ConnectorRecordFlagDto();
            connectorRecordFlagDto.setFlag(flag);
            connectorRecordFlagDto.setType("downloadPdkFileFlag");
            connectorRecordFlagDto.setId(connectionId);
            ManagementWebsocketHandler managementWebsocketHandler = BeanUtil.getBean(ManagementWebsocketHandler.class);
            try{
                managementWebsocketHandler.sendMessage(new TextMessage(JSONUtil.obj2Json(webSocketEvent)));
            }catch (Exception e){

            }
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
            connectorRecordDto.setPdkHash(databaseDefinition.getPdkHash());
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
        public void upsertConnectorRecord(ConnectorRecordDto connectorRecordDto) {
            HashMap<String, Object> queryMap = new HashMap<>();
            queryMap.put("connectionId", connectorRecordDto.getConnectionId());
            try{
                clientMongoOperator.upsert(queryMap, MapUtil.obj2Map(connectorRecordDto), ConnectorConstant.CONNECTORRECORD_COLLECTION);
            }catch (IllegalAccessException e){
                logger.error("Update inspect result failed. ", e);
            }
        }
    }


}
