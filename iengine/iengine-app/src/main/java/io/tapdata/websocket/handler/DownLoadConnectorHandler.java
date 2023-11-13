package io.tapdata.websocket.handler;

import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.mongo.RestTemplateOperator;
import com.tapdata.tm.commons.metrics.ConnectorRecordDto;
import io.tapdata.aspect.supervisor.AspectRunnableUtil;
import io.tapdata.aspect.supervisor.DisposableThreadGroupAspect;
import io.tapdata.aspect.supervisor.entity.ConnectionTestEntity;
import io.tapdata.common.SettingService;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.threadgroup.DisposableThreadGroup;
import io.tapdata.threadgroup.utils.DisposableType;
import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.SendMessage;
import io.tapdata.websocket.WebSocketEventHandler;
import io.tapdata.websocket.WebSocketEventResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
@EventHandlerAnnotation(type = "downLoadConnector")
public class DownLoadConnectorHandler implements WebSocketEventHandler {
    private final static Logger logger = LogManager.getLogger(DownLoadConnectorHandler.class);
    private ClientMongoOperator clientMongoOperator;
    private SettingService settingService;
    @Override
    public void initialize(ClientMongoOperator clientMongoOperator) {

    }
    @Override
    public void initialize(ClientMongoOperator clientMongoOperator, SettingService settingService) {
        this.clientMongoOperator = clientMongoOperator;
        this.settingService = settingService;
    }

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
        Runnable runnable = AspectRunnableUtil.aspectRunnable(new DisposableThreadGroupAspect<>(connectionId, threadGroup, entity), () -> {
            try{
                PdkUtil.downloadPdkFileIfNeed((HttpClientMongoOperator) clientMongoOperator, databaseDefinition.getPdkHash(), databaseDefinition.getJarFile(), databaseDefinition.getJarRid(), new RestTemplateOperator.Callback() {
                    @Override
                    public void needDownloadPdkFile(boolean flag) throws IOException {
                        logger.info("Whether to start downloading the pdk file {}",flag);
                        sendMessage.send(WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.DOWNLOAD_PDK_FILE_FLAG,flag));
                    }

                    @Override
                    public void onProgress(long fileSize,long progress) throws IOException {
                        Map<String, Object> map = new HashMap<>();
                        map.put("fileSize",fileSize);
                        map.put("progress", progress);
                        map.put("status","downloading");
                        sendMessage.send(WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.PROGRESS_REPORTING,map));
                    }

                    @Override
                    public void onFinish(String downloadSpeed){
                        logger.info("Downloading the pdk file is completed at a speed of {}.",downloadSpeed);
                        uploadConnectorRecord(databaseDefinition.getPdkHash(), ConnectorRecordDto.statusEnum.FINISH,downloadSpeed,message->{
                            try {
                                sendMessage.send(message);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
                    }

                    @Override
                    public void onError(IOException ex){
                        logger.error("The reason why downloading the pdk file failed is {}.",ex.getMessage());
                        uploadConnectorRecord(databaseDefinition.getPdkHash(), ConnectorRecordDto.statusEnum.FAIL,ex.getMessage(),message->{
                            try {
                                sendMessage.send(message);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
                        throw new RuntimeException("Download connector failed",ex);
                    }
                });
            } catch (Exception e){
                String errMsg = String.format("Download connector %s failed, data: %s, err: %s", connName, event, e.getMessage());
                logger.error(errMsg, e);
                try {
                    sendMessage.send(WebSocketEventResult.handleFailed(WebSocketEventResult.Type.PROGRESS_REPORTING, errMsg, e));
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        });
        Thread thread = new Thread(threadGroup, runnable, threadName);
        thread.start();
        return null;
    }

    private void uploadConnectorRecord(String pdkHash, ConnectorRecordDto.statusEnum statusEnum, String message, Consumer<WebSocketEventResult> consumer){
        Map<String, Object> map = new HashMap<>();
        ConnectorRecordDto connectorRecordDto = new ConnectorRecordDto();
        connectorRecordDto.setPdkHash(pdkHash);
        if(statusEnum.equals(ConnectorRecordDto.statusEnum.FINISH)){
            map.put("status",ConnectorRecordDto.statusEnum.FINISH.getStatus());
            connectorRecordDto.setStatus(ConnectorRecordDto.statusEnum.FINISH.getStatus());
            connectorRecordDto.setDownloadSpeed(message);
            consumer.accept(WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.PROGRESS_REPORTING,map));
        }else{
            connectorRecordDto.setStatus(ConnectorRecordDto.statusEnum.FAIL.getStatus());
            connectorRecordDto.setDownFiledMessage(message);
            consumer.accept(WebSocketEventResult.handleFailed(WebSocketEventResult.Type.PROGRESS_REPORTING,message));
        }
        try {
            clientMongoOperator.insertOne(connectorRecordDto, ConnectorConstant.CONNECTORRECORD_COLLECTION);
        }catch (Exception e){
            logger.error("Failed to upload downloader metrics");
        }
    }
}
