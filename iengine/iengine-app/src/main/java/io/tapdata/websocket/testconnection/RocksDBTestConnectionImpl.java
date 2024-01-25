package io.tapdata.websocket.testconnection;

import com.tapdata.constant.OsUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.validator.ConnectionValidateResult;
import com.tapdata.validator.ConnectionValidateResultDetail;
import com.tapdata.validator.ValidatorConstant;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class RocksDBTestConnectionImpl implements TestConnection{

    private boolean connectionFail = false;

    private static String PASS ="passed";

    private static String FAILED ="failed";

    private static String WRITE = "Write";
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
        if(connectionFail){
            connectionValidateResult.setStatus(ValidatorConstant.CONNECTION_STATUS_INVALID);
        }
        connectionValidateResult.setStatus(ValidatorConstant.CONNECTION_STATUS_READY);
        connectionValidateResult.setValidateResultDetails(validateResultDetails);
    }


    private void testReadPrivilege(List<ConnectionValidateResultDetail> validateResultDetails,String dir) {
        ConnectionValidateResultDetail connectionValidateResultDetail = handleFileRead(dir);
        validateResultDetails.add(connectionValidateResultDetail);
    }


    private void testWritePrivilege(List<ConnectionValidateResultDetail> validateResultDetails,String dir){
        ConnectionValidateResultDetail connectionValidateResultDetail = handleFileWrite(dir);
        validateResultDetails.add(connectionValidateResultDetail);
    }


    public ConnectionValidateResultDetail handleFileWrite(String dir) {
        ConnectionValidateResultDetail connectionValidateResultDetail = new ConnectionValidateResultDetail();
        connectionValidateResultDetail.setRequired(true);
        File file = new File(dir, "testConnect.txt");
        connectionValidateResultDetail.setShow_msg(WRITE);
        if (file.exists()) {
            connectionValidateResultDetail.setStatus(FAILED);
            if (file.canWrite()) {
                connectionValidateResultDetail.setStatus(PASS);
            }
        } else {
            handleCreateFile(connectionValidateResultDetail, file, WRITE);
        }
        return connectionValidateResultDetail;
    }


    public ConnectionValidateResultDetail handleFileRead(String dir) {
        ConnectionValidateResultDetail connectionValidateResultDetail = new ConnectionValidateResultDetail();
        connectionValidateResultDetail.setRequired(true);
        File file = new File(dir, "testConnect.txt");
        connectionValidateResultDetail.setShow_msg("Read");
        if (file.exists()) {
            connectionValidateResultDetail.setStatus(FAILED);
            if (file.canRead()) {
                connectionValidateResultDetail.setStatus(PASS);
            }
        } else {
            handleCreateFile(connectionValidateResultDetail, file, "Read");
        }

        return connectionValidateResultDetail;
    }

    public void handleCreateFile(ConnectionValidateResultDetail connectionValidateResultDetail, File file,
                                 String mark) {
        boolean status = false;
        try {
            file.getParentFile().mkdirs();
            if (file.createNewFile()) {
                if (WRITE.equals(mark)) {
                    status = file.canWrite();
                } else {
                    status = file.canRead();
                }

            }
        } catch (IOException e) {
            connectionValidateResultDetail.failed(e.getMessage());
        } finally {
            try {
                Files.delete(file.toPath());
            } catch (Exception e) {

            }
        }
        connectionFail = status;
        String result = status ? PASS : FAILED;
        connectionValidateResultDetail.setStatus(result);
    }
}
