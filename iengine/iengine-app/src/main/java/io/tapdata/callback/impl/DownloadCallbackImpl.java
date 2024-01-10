package io.tapdata.callback.impl;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.metrics.ConnectorRecordDto;
import io.tapdata.callback.DownloadCallback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DownloadCallbackImpl implements DownloadCallback {
    private final static Logger logger = LogManager.getLogger(DownloadCallbackImpl.class);
    private String connectionId;
    private ClientMongoOperator clientMongoOperator;

    private Map<String,Object> params;


    public DownloadCallbackImpl() {
    }


    public ClientMongoOperator getClientMongoOperator() {
        return clientMongoOperator;
    }

    public void setClientMongoOperator(ClientMongoOperator clientMongoOperator) {
        this.clientMongoOperator = clientMongoOperator;
    }

    public DownloadCallbackImpl(ClientMongoOperator clientMongoOperator,Map<String,Object> params) {
        this.clientMongoOperator = clientMongoOperator;
        this.params = params;
    }

    @Override
    public void needDownloadPdkFile(boolean flag) throws IOException {
        logger.info("Whether to start downloading the pdk file {}",flag);
        DatabaseTypeEnum.DatabaseType databaseDefinition =(DatabaseTypeEnum.DatabaseType)params.get("databaseDefinition");

        ConnectorRecordDto connectorRecordDto = new ConnectorRecordDto();
        connectorRecordDto.setPdkHash(databaseDefinition.getPdkHash());
        connectorRecordDto.setFlag(flag?"true":"false");
        connectorRecordDto.setProgress(0L);
        params.put("pdkHash",databaseDefinition.getPdkHash());
        params.put("flag",flag);
        upsertConnectorRecord(connectorRecordDto);
    }

    @Override
    public void onProgress(long fileSize,long progress) throws IOException{
        DatabaseTypeEnum.DatabaseType databaseDefinition =(DatabaseTypeEnum.DatabaseType)params.get("databaseDefinition");
        ConnectorRecordDto connectorRecordDto = new ConnectorRecordDto();
        connectorRecordDto.setPdkHash(databaseDefinition.getPdkHash());
        connectorRecordDto.setProcessId("123");
        connectorRecordDto.setFileSize(fileSize);
        connectorRecordDto.setProgress(progress);
        connectorRecordDto.setPdkHash(databaseDefinition.getPdkHash());
        connectorRecordDto.setStatus(ConnectorRecordDto.StatusEnum.DOWNLOADING.getStatus());
        upsertConnectorRecord(connectorRecordDto);
    }

    @Override
    public void onFinish(String downloadSpeed) throws IOException{
        logger.info("Downloading the pdk file is completed at a speed of {}.",downloadSpeed);
        DatabaseTypeEnum.DatabaseType databaseDefinition =(DatabaseTypeEnum.DatabaseType)params.get("databaseDefinition");
        ConnectorRecordDto connectorRecordDto = new ConnectorRecordDto();
        connectorRecordDto.setPdkHash(databaseDefinition.getPdkHash());
        connectorRecordDto.setProcessId("123");
        connectorRecordDto.setStatus(ConnectorRecordDto.StatusEnum.FINISH.getStatus());
        connectorRecordDto.setDownloadSpeed(downloadSpeed);
        connectorRecordDto.setPdkHash(databaseDefinition.getPdkHash());
        upsertConnectorRecord(connectorRecordDto);
    }

    @Override
    public void onError(Exception ex) throws IOException{
        logger.error("The reason why downloading the pdk file failed is {}.",ex.getMessage());
        DatabaseTypeEnum.DatabaseType databaseDefinition =(DatabaseTypeEnum.DatabaseType)params.get("databaseDefinition");
        ConnectorRecordDto connectorRecordDto = new ConnectorRecordDto();
        connectorRecordDto.setPdkHash(databaseDefinition.getPdkHash());
        connectorRecordDto.setProcessId("123");
        connectorRecordDto.setStatus(ConnectorRecordDto.StatusEnum.FAIL.getStatus());
        connectorRecordDto.setDownFiledMessage(ex.getMessage());
        upsertConnectorRecord(connectorRecordDto);
        throw new RuntimeException("Download connector failed ",ex);
    }
    public void upsertConnectorRecord(ConnectorRecordDto connectorRecordDto) {
        HashMap<String, Object> queryMap = new HashMap<>();
//        queryMap.put("pdkHash",);
//        queryMap.put();
        queryMap.put("pdkHash",connectorRecordDto.getPdkHash());
        queryMap.put("processId",connectorRecordDto.getProcessId());
        try{
            clientMongoOperator.upsert(queryMap, MapUtil.obj2Map(connectorRecordDto), ConnectorConstant.CONNECTORRECORD_COLLECTION);
        }catch (IllegalAccessException e){
            logger.error("Update inspect result failed. ", e);
        }
    }
}
