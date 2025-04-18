package com.tapdata.tm.task.service;

import com.tapdata.tm.base.dto.MutiResponseMessage;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.bean.LdpFuzzySearchVo;
import com.tapdata.tm.task.bean.MultiSearchDto;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface LdpService {

    TaskDto createFdmTask(TaskDto task, boolean start, UserDetail user);


    TaskDto createMdmTask(TaskDto task, String tagId, UserDetail user, boolean confirmTable, boolean start);

    void afterLdpTask(String taskId, UserDetail user);

    Tag getMdmTag(UserDetail user);
    Map<String, List<TaskDto>> queryFdmTaskByTags(List<String> tagIds, UserDetail user);

    List<LdpFuzzySearchVo> fuzzySearch(String key, List<String> connectType, UserDetail loginUser);
    List<LdpFuzzySearchVo> multiSearch(List<MultiSearchDto> multiSearchDto, UserDetail loginUser);

    void addLdpDirectory(UserDetail user);
    void addLdpDirectory(UserDetail user, Map<String, String> oldLdpMap);

    void generateLdpTaskByOld();

    boolean queryTagBelongMdm(String tagId, UserDetail user, String mdmTags);

    Map<String, String> ldpTableStatus(String connectionId, List<String> tableNames, String ldpType, UserDetail user);

    boolean checkFdmTaskStatus(String tagId, UserDetail loginUser);

    List<MutiResponseMessage> fdmBatchStart(String tagId, List<String> taskIds, UserDetail loginUser, HttpServletRequest request,
                                            HttpServletResponse response);

    void deleteMdmTable(String id, UserDetail loginUser);

    Set<String> belongLdpIds(String connectionId, List<MetadataInstancesDto> metas, UserDetail user);
}
