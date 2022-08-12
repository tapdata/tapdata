package com.tapdata.tm.commons.task.dto.progress;

import lombok.Data;


/**
 * @Author: Zed
 * @Date: 2022/3/14
 * @Description:
 */
@Data
public class BatchOperationDto {
    //where 的json   eg: {"name":"sam"}
    private String where;
    private TaskSnapshotProgress document;
    /** op 操作类型， insert插入， update更新， delete 删除 */
    private BatchOp op;

    public enum BatchOp {
        insert,
        update,
        delete,
        upsert,
        ;
    }
}
