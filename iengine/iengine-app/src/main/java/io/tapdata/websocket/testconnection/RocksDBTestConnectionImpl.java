package io.tapdata.websocket.testconnection;

import com.tapdata.constant.OsUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.validator.ConnectionValidateResult;
import com.tapdata.validator.ConnectionValidateResultDetail;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class RocksDBTestConnectionImpl implements TestConnection{
    @Override
    public void testConnection(Map event, ConnectionValidateResult connectionValidateResult) {
        List<ConnectionValidateResultDetail> validateResultDetails = new ArrayList<>();
        Map config = (Map) event.get("config");
        String tapdataWorkDir = System.getenv("TAPDATA_WORK_DIR");
        if (StringUtils.isBlank(tapdataWorkDir)) {
            tapdataWorkDir = System.getProperty("user.dir");
        }
        String path = config.get("uri").toString();
        String  dir = tapdataWorkDir + path;
        if (OsUtil.isWindows()) {
            dir = dir.replace("/", "\\");
        }
        testReadPrivilege(validateResultDetails, dir);
        testWritePrivilege(validateResultDetails, dir);
        connectionValidateResult.setValidateResultDetails(validateResultDetails);
    }


    private void testReadPrivilege(List<ConnectionValidateResultDetail> validateResultDetails,String dir) {
        ConnectionValidateResultDetail connectionValidateResultDetail =  handleFileReadAndWrite(dir,"Read");
        validateResultDetails.add(connectionValidateResultDetail);
    }


    private void testWritePrivilege(List<ConnectionValidateResultDetail> validateResultDetails,String dir) {
        ConnectionValidateResultDetail connectionValidateResultDetail =  handleFileReadAndWrite(dir,"Write");
        validateResultDetails.add(connectionValidateResultDetail);
    }


    public ConnectionValidateResultDetail handleFileReadAndWrite(String dir, String mark) {
        ConnectionValidateResultDetail connectionValidateResultDetail = new ConnectionValidateResultDetail();
        connectionValidateResultDetail.setRequired(true);
        connectionValidateResultDetail.setShow_msg(mark);
        File file = new File(dir,"testConnect.txt");
        String status = "failed";
        if (file.exists()) {
            if ("Write".equals(mark)) {
                if (file.canRead()) {
                    status = "passed";
                }
            } else {
                if (file.canWrite()) {
                    status = "passed";
                }
            }
        } else {
            try {
                file.getParentFile().mkdirs();
                file.createNewFile();
                if ("Write".equals(mark)) {
                    if (file.canRead()) {
                        status = "passed";
                    }
                } else {
                    if (file.canWrite()) {
                        status = "passed";
                    }
                }
            } catch (IOException e) {
                connectionValidateResultDetail.failed(e.getMessage());
            } finally {
                file.delete();
            }
        }
        connectionValidateResultDetail.setStatus(status);
        return connectionValidateResultDetail;
    }
}
