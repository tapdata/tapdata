package com.tapdata.tm.modules.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.base.IDataPermissionEntity;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.module.entity.ApiAlarmConfig;
import com.tapdata.tm.module.dto.PathSetting;
import com.tapdata.tm.module.entity.Path;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.List;


/**
 * Modules
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("Modules")
public class ModulesEntity extends BaseEntity implements IDataPermissionEntity {
    private String name;

    @Field("datasource")
    @JsonProperty("datasource")
    private String dataSource;

//    @Field("tablename")
    @JsonProperty("tableName")
    private String tableName;

    private String apiVersion;

    private String basePath;

    private String readPreference;

    private String readConcern;


    private String describtion;

    private String prefix;
    private String project;

//    private String path;
    private String apiType;

    private String status;

    private List<Path> paths;

    private List<com.tapdata.tm.commons.schema.Field> fields;

    private List<Tag> listtags;

    /**
     * TAP-12057 · 服务型索引（ADR-0001：存 Modules，不存 MetadataInstances.indices）。
     *
     * <p>必须与 {@code ModulesDto.servingIndexes} <b>同名同型</b>：{@code BaseService.convertToEntity} 走
     * {@code BeanUtils.copyProperties(dto, entity)} 按属性名拷贝，实体上缺这个属性即被静默丢弃——
     * 现象是「保存成功但索引没了」（2026-07-31 实机验证所见）。入库前已由 {@code ServingIndexNormalizer}
     * 归一化（按有序字段集确定性排序 + 方向显式）。</p>
     */
    private List<com.tapdata.tm.module.dto.ServingIndex> servingIndexes;

    private String createType;

    @Field("is_deleted")
    private Boolean isDeleted;

    // 总的访问行数
    @Field("res_rows")
    private Long resRows;

    //总的访问次数
    private Long visitCount;

    private ObjectId connection;

    //请求失败率
    private Number failRate;

    private Long responseTime;

    private Long latency;

    @Field("req_bytes")
    private Long reqBytes;

    private String connectionId;

    private String operationType;

    private String connectionType;

    private String connectionName;

    private String description;
    /** 访问路径方式  默认值 default  自定义 customize*/
    private String pathAccessMethod;

    /** 限制条数 */
    private Integer limit;

    /**
     * 用户可以自定义路径最后关键字，没有设置按默认值处理
     * */
    private List<PathSetting> pathSetting;

    ApiAlarmConfig apiAlarmConfig;

    private String publishStatus;
}
