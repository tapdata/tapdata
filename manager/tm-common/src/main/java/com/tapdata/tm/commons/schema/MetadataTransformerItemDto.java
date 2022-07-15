package com.tapdata.tm.commons.schema;

import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.dag.FieldsMapping;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;


/**
 * MetadataTransformerItem
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class MetadataTransformerItemDto extends BaseDto {
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
    private String uuid;
    private List<FieldsMapping> fieldsMapping;

    /**
     * migrate pre tableName
     */
    private String previousTableName;

}