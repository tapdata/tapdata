package io.tapdata.connector.custom;

import io.tapdata.connector.custom.config.CustomConfig;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.TestItem;

import static io.tapdata.base.ConnectorBase.testItem;

public class CustomTest {

    private final CustomConfig customConfig;
    private final static String CHECK_CUSTOM_SCRIPT = "check script for source or target";

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
        return null;
    }

    private boolean validateScript(CustomConfig customConfig) {
        switch (customConfig.get__connectionType()) {
            case "source":
                return validateSourceScript(customConfig);
            case "target":
                return validateTargetScript(customConfig);
            case "source_and_target":
                return validateSourceScript(customConfig) && validateTargetScript(customConfig);
        }
        return true;
    }

    private boolean validateSourceScript(CustomConfig customConfig) {
        switch (customConfig.getSyncType()) {
            case "initial_sync":
                if (EmptyKit.isBlank(customConfig.getHistoryScript())) {
                    return false;
                }
                break;
            case "cdc":
                if (EmptyKit.isBlank(customConfig.getCdcScript())) {
                    return false;
                }
                break;
            case "initial_sync+cdc":
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
