package io.tapdata.services;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.metrics.ConnectorRecordDto;
import io.tapdata.aspect.supervisor.entity.ConnectionTestEntity;
import io.tapdata.callback.DownloadCallback;
import io.tapdata.callback.impl.DownloadCallbackImpl;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.service.skeleton.annotation.RemoteService;
import io.tapdata.threadgroup.DisposableThreadGroup;
import io.tapdata.threadgroup.utils.DisposableType;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


@RemoteService
public class DownLoadService {

    private final Logger logger = LogManager.getLogger(TestRunService.class);

    public static final String TAG = TestRunService.class.getSimpleName();
    ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);

    public Object downloadPdkFileIfNeedPrivate(Map<String, Object> messageInfo) {
        Map<String, Object> resultMap = null;
        Map<String,Object> data=(Map<String,Object>) messageInfo.get("data");
        logger.info(String.format("downLoad connector, event: %s", messageInfo));
        String pskHash = (String) data.getOrDefault("pdkHash", "");
        String connName = (String) data.getOrDefault("name", "");
        String connectionId = String.valueOf(data.get("id"));
        String threadName = String.format("DOWNLOAD-CONNECTOR-%s", Optional.ofNullable(data.get("name")).orElse(""));
        DisposableThreadGroup threadGroup = new DisposableThreadGroup(DisposableType.DOWNLOAD_CONNECTOR, threadName);
        DatabaseTypeEnum.DatabaseType databaseDefinition = ConnectionUtil.getDatabaseType(clientMongoOperator, pskHash);
        DownloadCallback callback=new DownloadCallbackImpl(databaseDefinition,connectionId,clientMongoOperator);
        final Object lock = PdkUtil.pdkDownloadLock(databaseDefinition.getPdkHash());
        Boolean flag = null;
        synchronized (lock){
            try {
                flag = ifHasPdkFile(databaseDefinition.getJarFile(), databaseDefinition.getJarRid());
                callback.needDownloadPdkFile(false);
            }catch (Exception e){
            }
        }
        Runnable runnable = ()->{
            downloadPdkFileIfNeedPrivate(messageInfo,connName,connectionId,databaseDefinition,callback);
        };
        startThread( threadGroup, runnable,threadName);
        resultMap.put("flag",flag);
        resultMap.put("pdkId",databaseDefinition.getPdkId());
        resultMap.put("connectionId",connectionId);
        return resultMap;
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
    protected boolean ifHasPdkFile(String fileName,String resourceId){
        // create the dir used for storing the pdk jar file if the dir not exists
        String dir = System.getProperty("user.dir") + File.separator + "dist";
        File folder = new File(dir);
        if (!folder.exists()) {
            folder.mkdirs();
        }

        String filePrefix = fileName.split("\\.jar")[0];
        StringBuilder filePath = new StringBuilder(dir)
                .append(File.separator)
                .append(filePrefix)
                .append("__").append(resourceId).append("__");

        filePath.append(".jar");
        File theFilePath = new File(filePath.toString());
        return !theFilePath.isFile();
    }
}
