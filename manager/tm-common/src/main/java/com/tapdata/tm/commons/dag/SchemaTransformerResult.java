package com.tapdata.tm.commons.dag;

import com.tapdata.tm.commons.dag.vo.FieldInfo;
import lombok.Data;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/11/6 下午7:19
 */
@Data
public class SchemaTransformerResult implements Serializable {
    private String sourceNodeId;
    private boolean invalid;                  // ": false,
    private String sourceQualifiedName;                  // ": "T_mysql_INSURANCE_AUTO_CLAIM_615524058c7c8e2665de227b",
    private String sourceDbName;                        //": "Demo MySQL",
    private String sourceDbType;                        //": "Demo MySQL",
    private String sourceObjectName;                        //": "AUTO_CLAIM",
    private int sourceFieldCount;                        //": 9,
    private int sourceDeletedFieldCount;                        //": 9,
    private String sourceTableId;                      //   ": "61552406beeb6a899b22fe5e",

    private String sinkNodeId;
    private String sinkQulifiedName;                        //": "MC_mongodb_test_AUTO_CLAIM_615523a38c7c8e2665de2279",
    private String sinkDbName;                    //  ": "Demo MongoDB",
    private String sinkObjectName;                    //  ": "AUTO_CLAIM",
    private String sinkDbType;                    //  ": "mongodb",
    private String sinkStageId;                  // ": "70c96db4-9a39-45ae-8060-e4ef3ada5a8c",
    private int userDeletedNum;                    //  ": 0,
    private String sinkTableId;                  // ": "615523a6beeb6a899b21df14"
    private List<FieldsMapping> fieldsMapping;
    private int sinkAvailableFieldCount;
    private int sinkInvalidFieldCount;

    private LinkedList<FieldInfo> migrateFieldsMapping;
}
