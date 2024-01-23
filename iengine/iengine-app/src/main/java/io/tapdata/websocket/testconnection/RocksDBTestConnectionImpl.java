package io.tapdata.websocket.testconnection;

import com.tapdata.constant.OsUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.validator.ConnectionValidateResult;
import com.tapdata.validator.ConnectionValidateResultDetail;
import com.tapdata.validator.ValidatorConstant;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class RocksDBTestConnectionImpl implements TestConnection{

    private boolean connectionFail = false;

    private static String PASS ="passed";

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


    private void testWritePrivilege(List<ConnectionValidateResultDetail> validateResultDetails,String dir) {
        ConnectionValidateResultDetail connectionValidateResultDetail = handleFileWrite(dir);
        validateResultDetails.add(connectionValidateResultDetail);
    }


    public ConnectionValidateResultDetail handleFileWrite(String dir) {
        ConnectionValidateResultDetail connectionValidateResultDetail = new ConnectionValidateResultDetail();
        connectionValidateResultDetail.setRequired(true);
        File file = new File(dir, "testConnect.txt");
        connectionValidateResultDetail.setShow_msg(WRITE);
        String status = "failed";
        if (file.exists()) {
            if (file.canWrite()) {
                status = PASS;
            }
        } else {
            try {
                file.getParentFile().mkdirs();
                file.createNewFile();
                if (file.canWrite()) {
                    status = PASS;
                }
            } catch (IOException e) {
                connectionValidateResultDetail.failed(e.getMessage());
            } finally {
                file.delete();
            }
        }
        if (PASS.equals(status)) {
            connectionFail = true;
        }
        connectionValidateResultDetail.setStatus(status);
        return connectionValidateResultDetail;
    }


    public ConnectionValidateResultDetail handleFileRead(String dir) {
        ConnectionValidateResultDetail connectionValidateResultDetail = new ConnectionValidateResultDetail();
        connectionValidateResultDetail.setRequired(true);
        File file = new File(dir, "testConnect.txt");
        connectionValidateResultDetail.setShow_msg("Read");
        String status = "failed";
        if (file.exists()) {
            if (file.canRead()) {
                status = PASS;
            }
        } else {
            try {
                file.getParentFile().mkdirs();
                file.createNewFile();
                if (file.canRead()) {
                    status = PASS;
                }
            } catch (IOException e) {
                connectionValidateResultDetail.failed(e.getMessage());
            } finally {
                file.delete();
            }
        }
        if (PASS.equals(status)) {
            connectionFail = true;
        }
        connectionValidateResultDetail.setStatus(status);
        return connectionValidateResultDetail;
    }
}
