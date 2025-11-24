package com.tapdata.tm.commons.task.dto;

import lombok.Data;

import java.util.List;

@Data
public class MergeTablePropertiesInfo {
   private List<CacheStatistics> cacheStatisticsList;
   private CacheRebuildStatus cacheRebuildStatus;
   private String mergeTablePropertiesId;
   private String tableName;
   private String mergeNodeId;
   private boolean needRebuild = false;

}