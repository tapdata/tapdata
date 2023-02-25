package io.tapdata.connector.custom;

import io.tapdata.connector.custom.config.CustomConfig;
import io.tapdata.constant.ConnectionTypeEnum;
import io.tapdata.constant.SyncTypeEnum;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.TestItem;

import javax.script.ScriptException;

import static io.tapdata.base.ConnectorBase.testItem;

public class CustomTest {

    private final CustomConfig customConfig;
    private final static String CHECK_CUSTOM_SCRIPT = "check script for source or target";
    private final static String CHECK_CUSTOM_LOAD_SCHEMA = "check script for loading schema";

    public CustomTest(CustomConfig customConfig) {
        this.customConfig = customConfig;
    }

    public TestItem testScript() {
        if (validateScript(customConfig)) {
            return testItem(CHECK_CUSTOM_SCRIPT, TestItem.RESULT_SUCCESSFULLY);
        } else {
            return testItem(CHECK_CUSTOM_SCRIPT, TestItem.RESULT_FAILED, "some scripts needed are empty!");
        }
    }

    public TestItem testBuildSchema() {
        CustomSchema customSchema = new CustomSchema(customConfig);
        try {
            customSchema.loadSchema();
            return testItem(CHECK_CUSTOM_LOAD_SCHEMA, TestItem.RESULT_SUCCESSFULLY);
        } catch (ScriptException e) {
            return testItem(CHECK_CUSTOM_LOAD_SCHEMA, TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    private boolean validateScript(CustomConfig customConfig) {
        switch (ConnectionTypeEnum.fromValue(customConfig.get__connectionType())) {
            case SOURCE:
                return validateSourceScript(customConfig);
            case TARGET:
                return validateTargetScript(customConfig);
            case SOURCE_AND_TARGET:
                return validateSourceScript(customConfig) && validateTargetScript(customConfig);
        }
        return true;
    }

    private boolean validateSourceScript(CustomConfig customConfig) {
        switch (SyncTypeEnum.fromValue(customConfig.getSyncType())) {
            case INITIAL_SYNC:
                if (EmptyKit.isBlank(customConfig.getHistoryScript())) {
                    return false;
                }
                break;
            case CDC:
                if (EmptyKit.isBlank(customConfig.getCdcScript())) {
                    return false;
                }
                break;
            case INITIAL_SYNC_CDC:
                if (EmptyKit.isBlank(customConfig.getHistoryScript()) || EmptyKit.isBlank(customConfig.getCdcScript())) {
                    return false;
                }
                break;
        }
        return true;
    }

    private boolean validateTargetScript(CustomConfig customConfig) {
        return !EmptyKit.isBlank(customConfig.getTargetScript());
    }
}
