package com.tapdata.tm.modules.vo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.commons.base.IDataPermissionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.module.dto.PathSetting;
import com.tapdata.tm.module.entity.Path;
import com.tapdata.tm.vo.BaseVo;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;
import java.util.List;


@Data
@EqualsAndHashCode(callSuper=false)
public class ModulesListVo extends BaseVo implements IDataPermissionDto {
    private String name;

    @JsonProperty("datasource")
    private String dataSource;

    @JsonProperty("tableName")
    private String tableName;

    private String apiVersion;

    private String basePath;

    private String readPreference;
    private String readPreferenceTag;

    private String readConcern;


    private String description;

    private String describtion;

    private String prefix;

    private String path;
    private String apiType;

    private String status;
    private String createUser;
    private List<Path> paths;
    private List<Field> fields;

    @JsonProperty("listtags")
    private List<Tag> listtags;

    /**
     * TAP-12057 · 服务型索引（ADR-0001）。列表接口是 API 编辑抽屉的数据源，少这个属性会让
     * {@code BeanUtil.deepCloneList(items, ModulesListVo.class)} 按属性名拷贝时静默丢弃，
     * 现象是「保存成功、重开抽屉索引不见了」（2026-07-31 实机验证所见）。
     * 载体齐全性由 {@code ServingIndexesCarrierTest} 钉住。
     */
    private List<com.tapdata.tm.module.dto.ServingIndex> servingIndexes;

    private String project;
    private String createType;

    private String connectionId;

    private String connection;

    private Source source;

    /**
     * 创建者
     */
    private String user;

    @JsonProperty("last_updated")
    private Date lastUpdAt;

    private String operationType;

    private String connectionType;

    private String connectionName;

    /** 访问路径方式  默认值 default  自定义 customize*/
    private String pathAccessMethod;

    /** 限制条数 */
    private Integer limit;

    /**
     * 用户自定义的路径最后关键字，没有设置按默认值处理
     * */
    private List<PathSetting> pathSetting;

    private String publishStatus;
}
