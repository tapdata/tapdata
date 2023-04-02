package io.tapdata.coding.service.schema;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Integer;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Long;

public class Iterations implements SchemaStart {
    public final Boolean use = true;

    @Override
    public Boolean use() {
        return use;
    }

    public Iterations(AtomicReference<String> accessToken) {
    }

    @Override
    public String tableName() {
        return "Iterations";
    }

    @Override
    public boolean connection(TapConnectionContext tapConnectionContext) {
        return false;
    }

    @Override
    public TapTable document(TapConnectionContext connectionContext) {
        return table(tableName())
                .add(field("Code", JAVA_Integer).isPrimaryKey(true).primaryKeyPos(1))        //事项 Code
                .add(field("CreatedAt", JAVA_Long))                                             //创建时间
                .add(field("ProjectId", JAVA_Long))                                             //创建时间
                .add(field("UpdatedAt", JAVA_Long))                                             //修改时间
                .add(field("Assignee", JAVA_Long))                                             //修改时间
                .add(field("Creator", JAVA_Long))                                             //修改时间
                .add(field("Deleter", JAVA_Long))                                             //修改时间
                .add(field("Starter", JAVA_Long))                                             //修改时间
                .add(field("Completer", JAVA_Long))                                             //修改时间
                .add(field("StartAt", JAVA_Long))                                             //修改时间
                .add(field("EndAt", JAVA_Long))                                             //修改时间
                .add(field("WaitProcessCount", JAVA_Long))                                             //修改时间
                .add(field("ProcessingCount", JAVA_Long))                                             //修改时间
                .add(field("CompletedCount", JAVA_Long))                                             //修改时间
                .add(field("CompletedPercent", JAVA_Long))                                             //修改时间
                .add(field("Name", "StringMinor"))   //项目名称
                .add(field("Status", "StringMinor"))   //项目名称
                .add(field("Goal", "StringMinor"));                                              //名称
    }

    @Override
    public Map<String, Object> autoSchema(Map<String, Object> eventData) {
        return eventMapToSchemaMap(eventData, TapSimplify.map(
                entry("Code", "code"),
                entry("ProjectId", "project_id"),
                entry("CreatedAt", "created_at"),
                entry("UpdatedAt", "updated_at"),
                entry("Assignee", "assignee.id"),
                entry("Creator", "creator.id"),
                entry("Deleter", "deleter.id"),
                entry("Starter", "starter.id"),
                entry("Completer", "completer.id"),
                entry("StartAt", "start_at"),
                entry("EndAt", "end_at"),
                entry("WaitProcessCount", "wait_process_count"),
                entry("ProcessingCount", "processing_count"),
                entry("CompletedCount", "completed_count"),
                entry("CompletedPercent", "completed_percent"),
                entry("Name", "title"),
                entry("Status", "status"),
                entry("Goal", "goal")));
    }
}
