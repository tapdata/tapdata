package io.tapdata.pdk.run.base;

import java.util.Objects;
import java.util.StringJoiner;

public enum RunClassMap {
    BATCH_COUNT_RUN("1", "io.tapdata.pdk.run.support.BatchCountRun", "BatchCountFunction", new String[]{"batchCount"}, "BatchCountRun"),
    BATCH_READ_RUN("2", "io.tapdata.pdk.run.support.BatchReadRun", "BatchReadFunction", new String[]{"batchRead"}, "BatchReadRun"),
    COMMAND_RUN("3", "io.tapdata.pdk.run.support.CommandRun", "", new String[]{"commandCallback"}, "CommandRun"),
    CONNECTION_TEST_RUN("4", "io.tapdata.pdk.run.support.ConnectionTestRun", "", new String[]{"connectionTest"}, "ConnectionTestRun"),
    DISCOVER_SCHEMA_RUN("5", "io.tapdata.pdk.run.support.DiscoverSchemaRun", "", new String[]{"discoverSchema"}, "DiscoverSchemaRun"),
    STREAM_READ_RUN("6", "io.tapdata.pdk.run.support.StreamReadRun", "", new String[]{"streamRead"}, "StreamReadRun"),
    TABLE_COUNT_RUN("7", "io.tapdata.pdk.run.support.TableCountRun", "", new String[]{"tableCount"}, "TableCountRun"),
    TIMESTAMP_TO_STREAM_OFFSET_RUN("8", "io.tapdata.pdk.run.support.TimestampToStreamOffsetRun", "", new String[]{"timestampToStreamOffset"}, "TimestampToStreamOffsetRun"),
    WEB_HOOK_EVENT_RUN("9", "io.tapdata.pdk.run.support.WebHookEventRun", "", new String[]{"webhookEvent"}, "WebHookEventRun"),
    WRITE_RECORD_RUN("10", "io.tapdata.pdk.run.support.WriteRecordRun", "", new String[]{"writeRecord", "insertRecord", "updateRecord", "deleteRecord"}, "WriteRecordRun"),
    ;
    String classPath;
    String functionJavaName;
    String[] functionJsName;
    String runCaseName;
    String sort;

    RunClassMap(String sort, String classPath, String functionJavaName, String[] functionJsName, String runCaseName) {
        this.classPath = classPath;
        this.sort = sort;
        this.functionJavaName = functionJavaName;
        this.runCaseName = runCaseName;
        this.functionJsName = functionJsName;
    }

    public static String whichCase(String runCaseName) {
        if (Objects.nonNull(runCaseName) && !"".equals(runCaseName.trim())) {
            for (RunClassMap value : values()) {
                if (value.equalsName(runCaseName)) return value.classPath;
            }
        }
        return null;
    }

    public boolean equalsName(String runCaseName) {
        return runCaseName.equals(this.classPath)
                || runCaseName.equals(this.functionJavaName)
                || this.hasInJsName(runCaseName)
                || runCaseName.equals(sort)
                || runCaseName.equals(this.runCaseName);
    }

    public boolean hasInJsName(String runCaseName) {
        String[] jsName = this.functionJsName;
        for (String name : jsName) {
            if (name.equals(runCaseName)) return true;
        }
        return false;
    }

    public static String allCaseTable(boolean showLog) {
        RunClassMap[] values = values();
        int classPathMaxLength = 0;
        int functionSortMaxLength = 0;
        int functionJsNameMaxLength = 0;
        int runCaseNameMaxLength = 0;
        for (RunClassMap value : values) {
            String classPath = value.classPath;
            String sort = value.sort;
            String[] functionJsName = value.functionJsName;
            String runCaseName = value.runCaseName;
            int functionJsNameLength = 0;
            for (String name : functionJsName) {
                functionJsNameLength += name.length() + 3;
            }
            classPathMaxLength = Math.max(classPathMaxLength, classPath.length());
            functionSortMaxLength = Math.max(functionSortMaxLength, sort.length());
            functionJsNameMaxLength = Math.max(functionJsNameMaxLength, functionJsNameLength);
            runCaseNameMaxLength = Math.max(runCaseNameMaxLength, runCaseName.length());
        }

        functionSortMaxLength += 2;
        functionJsNameMaxLength += 2;
        runCaseNameMaxLength += 2;
        StringJoiner joiner = new StringJoiner("\n");
        int maxLineSize = classPathMaxLength
                //+ functionJavaNameMaxLength
                + functionJsNameMaxLength
                + runCaseNameMaxLength;
        if (showLog) {
            joiner.add(center("------------------------------------------------------------------------------------", maxLineSize));
            joiner.add(center("[.___________.    ___      .______    _______       ___   .___________.    ___     ]", maxLineSize));
            joiner.add(center("[|           |   /   \\     |   _  \\  |       \\     /   \\  |           |   /   \\    ]", maxLineSize));
            joiner.add(center("[`---|  |----`  /  ^  \\    |  |_)  | |  .--.  |   /  ^  \\ `---|  |----`  /  ^  \\   ]", maxLineSize));
            joiner.add(center("[    |  |      /  /_\\  \\   |   ___/  |  |  |  |  /  /_\\  \\    |  |      /  /_\\  \\  ]", maxLineSize));
            joiner.add(center("[    |  |     /  _____  \\  |  |      |  '--'  | /  _____  \\   |  |     /  _____  \\ ]", maxLineSize));
            joiner.add(center("[    |__|    /__/     \\__\\ | _|      |_______/ /__/     \\__\\  |__|    /__/     \\__\\]", maxLineSize));
            joiner.add(center("------------------------------------------------------------------------------------", maxLineSize));
        }
        joiner.add(spilt(maxLineSize));
        joiner.add(
                spilt(functionSortMaxLength, "No") +
                        spilt(functionJsNameMaxLength, "JavaScript Name") +
                        spilt(runCaseNameMaxLength, "Run/Debug Name") +
                        spilt(classPathMaxLength, "Class Path"));
        joiner.add(spilt(maxLineSize));
        for (RunClassMap value : values) {
            joiner.add(
                    spilt(functionSortMaxLength, value.sort) +
                            spilt(functionJsNameMaxLength, value.jsNameInLine()) +
                            spilt(runCaseNameMaxLength, value.runCaseName) +
                            spilt(classPathMaxLength, value.classPath));
        }
        return joiner.toString() + RunnerSummary.format("logo.tip");
    }

    public static String center(String str, int lineSize) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < (lineSize / 2 - str.length() / 2); i++) {
            builder.append(" ");
        }
        builder.append(str);
        return builder.toString();
    }

    public static String allCaseTable() {
        return allCaseTable(false);
    }

    public static String spilt(int maxLength, String str) {
        StringBuilder builder = new StringBuilder(str);
        for (int i = 0; i < maxLength - str.length(); i++) {
            builder.append(" ");
        }
        return builder.toString();
    }

    public String jsNameInLine() {
        StringJoiner builder = new StringJoiner(" | ");
        for (String name : this.functionJsName) {
            builder.add(name);
        }
        return builder.toString();
    }

    public static void main(String[] args) {
        System.out.println(allCaseTable());
    }

    public static String spilt(int maxLength) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < maxLength; i++) {
            builder.append("-");
        }
        return builder.toString();
    }

    public String jsName(int index) {
        if (index >= this.functionJsName.length || index < 0) return this.functionJsName[0];
        return this.functionJsName[index];
    }
}
