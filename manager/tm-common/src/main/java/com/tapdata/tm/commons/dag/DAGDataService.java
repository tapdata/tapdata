package com.tapdata.tm.commons.dag;

import com.tapdata.tm.commons.dag.vo.MigrateJsResultVo;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.schema.TapTable;
import org.bson.types.ObjectId;

import java.util.List;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/11/9 下午5:22
 */
public interface DAGDataService {

    /**
     * 根据数据源ID 和 表名称加载模型
     * @param ownerId 模型拥有者用户 id
     * @param dataSourceId 数据源id
     * @param tableName 表名称
     * @return 返回指定表的模型
     */
    public Schema loadSchema(String ownerId, ObjectId dataSourceId, String tableName);

    /**
     * 根据数据源ID加载模型，可以指定包含的表名称或者不包含的表名称
     * 当同时指定 includes 和 excludes 时，取两者交集
     *
     * @param ownerId 模型拥有者用户 id
     * @param dataSourceId 数据源ID
     * @param includes 包含的表名称，为 null 时返回所有表
     * @param excludes 不包含的表名称，为 null 时返回所有表
     * @return 返回匹配到的表名称
     */
    public List<Schema> loadSchema(String ownerId, ObjectId dataSourceId, List<String> includes, List<String> excludes);


    default TapTable loadTapTable(String nodeId, String virtualId, TaskDto subTaskDto) {
        return null;
    }

    default List<MigrateJsResultVo> getJsResult(String jsNodeId, String virtualTargetId, TaskDto subTaskDto) {
        return null;
    }

    /**
     * 保存数据源
     *
     * 保存模型时，只更新 MetadataInstances: (original_name, fields)
     *
     * @param ownerId 模型拥有者用户 id
     * @param dataSourceId 数据源id
     * @param schema 模型
     * @param options 配置项
     * @return 更新后的模型
     */
    public List<Schema> createOrUpdateSchema(String ownerId, ObjectId dataSourceId, List<Schema> schema, DAG.Options options, Node node);

    /**
     * 根据连接id查询连接配置
     * @param connectionId
     * @return
     */
    public DataSourceConnectionDto getDataSource(String connectionId);


    /**
     * 模型推演进度推送
     * @param taskId 任务id
     * @param total 总节点（表）数
     * @param finished 已经推演的数量
     */
    void updateTransformRate(String taskId, int total, int finished);

    /**
     * 将推演的详情更新到推演的表中
     * @param schemaTransformerResults 推演的细节
     * @param taskId 任务id  推演表中对应的是dataflowId
     * @param nodeId 节点id  推演表中对应的是stageId
     * @param total 本节点所推演的所有的表名列表
     * @param uuid 同一批次推演的唯一标识
     */
    void upsertTransformTemp(List<SchemaTransformerResult> schemaTransformerResults, String taskId, String nodeId, int total, List<String> sourceQualifiedNames, String uuid);

//    /**
//     * 将推演的详情更新到推演的表中，与上面的那个接口是一个重写
//     * @param schemaTransformerResults 推演的细节
//     * @param taskId 任务id  推演表中对应的是dataflowId
//     * @param nodeId 节点id  推演表中对应的是stageId
//     * @param tableName 只有单个推演表的表名
//     */
//    void upsertTransformTemp(List<SchemaTransformerResult> schemaTransformerResults, String taskId, String nodeId, String tableName);
    boolean checkNewestVersion(String taskId, String nodeId, String version);

    TaskDto getTaskById(String taskId);
    ObjectId getTaskId();
}
