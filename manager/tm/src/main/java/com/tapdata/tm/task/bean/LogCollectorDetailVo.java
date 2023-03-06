package com.tapdata.tm.task.bean;

import com.tapdata.tm.externalStorage.vo.ExternalStorageVo;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;
import java.util.List;

/**
 * @Author: Zed
 * @Date: 2022/2/18
 * @Description:
 */
@Data
@EqualsAndHashCode(callSuper=true)
public class LogCollectorDetailVo extends LogCollectorVo {

    private List<SyncTaskVo> taskList;
    private String taskId;
    private ExternalStorageVo ExternalStorage;
}
