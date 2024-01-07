package io.tapdata.callback.impl;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.metrics.ConnectorRecordDto;
import io.tapdata.callback.DownloadCallback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;

public class DownloadCallbackImpl implements DownloadCallback {
    private final static Logger logger = LogManager.getLogger(DownloadCallbackImpl.class);
    private DatabaseTypeEnum.DatabaseType databaseDefinition;
    private String connectionId;
    private ClientMongoOperator clientMongoOperator;

    public DatabaseTypeEnum.DatabaseType getDatabaseDefinition() {
        return databaseDefinition;
    }

    public void setDatabaseDefinition(DatabaseTypeEnum.DatabaseType databaseDefinition) {
        this.databaseDefinition = databaseDefinition;
    }

    public DownloadCallbackImpl() {
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
        connectorRecordDto.setConnectionId(connectionId);
        connectorRecordDto.setPdkHash(databaseDefinition.getPdkHash());
        connectorRecordDto.setFlag(flag);
        connectorRecordDto.setProgress(0L);
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
