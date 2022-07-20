package com.tapdata.tm.transform.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.dag.FieldsMapping;
import com.tapdata.tm.commons.dag.vo.FieldInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.LinkedList;
import java.util.List;


/**
 * MetadataTransformerItem
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("MetadataTransformerItem")
public class MetadataTransformerItemEntity extends BaseEntity {
    private String sourceNodeId;
    private boolean invalid;                  // ": false,
    private String sourceQualifiedName;                  // ": "T_mysql_INSURANCE_AUTO_CLAIM_615524058c7c8e2665de227b",
    private String sourceDbName;                        //": "Demo MySQL",
    private String sourceDbType;
    private String sourceDataBaseType;
    private String sourceObjectName;                        //": "AUTO_CLAIM",
    private int sourceFieldCount;                        //": 9,
    private String sourceTableId;                      //   ": "61552406beeb6a899b22fe5e",

    private String sinkNodeId;
    private String sinkQulifiedName;                        //": "MC_mongodb_test_AUTO_CLAIM_615523a38c7c8e2665de2279",
    private String sinkDbName;                    //  ": "Demo MongoDB",
    private String sinkObjectName;                    //  ": "AUTO_CLAIM",
    private String sinkDbType;                    //  ": "mongodb",
    private String sinkStageId;                  // ": "70c96db4-9a39-45ae-8060-e4ef3ada5a8c",
    private int userDeletedNum;                    //  ": 0,
    private String sinkTableId;                  // ": "615523a6beeb6a899b21df14"

    private List<String> sourctTypes;
    private String dataFlowId;
    private String version;
    private List<FieldsMapping> fieldsMapping;
    private String uuid;

    private LinkedList<FieldInfo> migrateFieldsMapping;// 数据复制使用的字段映射
}