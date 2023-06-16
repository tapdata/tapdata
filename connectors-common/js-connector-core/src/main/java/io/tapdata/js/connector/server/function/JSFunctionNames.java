package io.tapdata.js.connector.server.function;

import java.util.Objects;

public enum JSFunctionNames {
    BatchReadFunction("batchRead", "BatchReadFunction", ""),
    CommandV2("commandCallbackV2", "CommandCallbackFunction", ""),
    CommandV1("commandCallback", "CommandCallbackFunction", ""),
    StreamReadFunction("streamRead", "StreamReadFunction", ""),
    WriteRecordFunction("writeRecord", "WriteRecordFunction", ""),
    BatchCountFunction("batchCount", "BatchCountFunction", ""),
    WebHookFunction("webhookEvent", "RowDataCallbackFunctionV2", ""),
    CreateTableV2Function("createTableV2", "CreateTableV2Function", ""),
    TimestampToStreamOffset("timestampToStreamOffset", "TimestampToStreamOffset", ""),
    ExecuteCommandFunction("executeCommand", "ExecuteCommandFunction", ""),

    CONNECTION_TEST("connectionTest", "ConnectionTest", ""),
    DISCOVER_SCHEMA("discoverSchema", "DiscoverSchema", ""),
    TABLE_COUNT("tableCount", "TableCount", ""),
    EXPIRE_STATUS("expireStatus", "ExpireStatus", ""),
    UPDATE_TOKEN("updateToken", "UpdateToken", ""),

    CONNECTOR_WEBSITE("connectorWebsite","ConnectorWebSite",""),
    TABLE_WEBSITE("tableWebsite","TableWebSite",""),


    DeleteRecordFunction("deleteRecord", "DeleteRecordFunction", "","writeRecord"),
    InsertRecordFunction("insertRecord", "InsertRecordFunction", "","writeRecord"),
    UpdateRecordFunction("updateRecord", "UpdateRecordFunction", "","writeRecord"),

    SCANNING_CAPABILITIES_IN_JAVA_SCRIPT("_scanning_capabilities_in_java_script", "scanningCapabilitiesInJavaScript", ""),
    ;
    String jsName;
    String javaName;
    String description;
    String baseFunction;

    JSFunctionNames(String jsName, String javaName, String description,String baseFunction) {
        this.javaName = javaName;
        this.jsName = jsName;
        this.description = description;
        this.baseFunction = baseFunction;
    }
    JSFunctionNames(String jsName, String javaName, String description) {
        this.javaName = javaName;
        this.jsName = jsName;
        this.description = description;
        this.baseFunction = "base";
    }

    public String jsName() {
        return this.jsName;
    }

    public String javaName() {
        return this.javaName;
    }

    public String description() {
        return this.description;
    }

    public static JSFunctionNames isSupport(String functionName) {
        if (Objects.isNull(functionName)) return null;
        JSFunctionNames[] values = values();
        for (JSFunctionNames value : values) {
            if (functionName.equals(value.jsName())) return value;
        }
        return null;
    }

    public static boolean isSupported(String functionName) {
        return Objects.nonNull(JSFunctionNames.isSupport(functionName));
    }
}
