package com.tapdata.tm.task.bean;

import com.tapdata.tm.commons.task.dto.Milestone;
import lombok.Data;

import java.util.List;

/**
 * @Author: Zed
 * @Date: 2021/12/2
 * @Description:
 */
@Data
public class RunTimeInfo {
    /** 结构迁移 */
//    private StructureMigrateVO structureMigrate;
//    /** 全量同步 */
//    private FullSyncVO fullSync;
//    /** 增量同步 */
//    private List<IncreaSyncVO> increaSync;
    /** 里程碑 */
    private List<Milestone> milestones;
}
