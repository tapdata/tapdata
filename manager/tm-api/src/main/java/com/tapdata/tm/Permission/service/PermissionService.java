package com.tapdata.tm.Permission.service;

import com.tapdata.tm.Permission.dto.PermissionDto;
import com.tapdata.tm.Permission.dto.Status;
import com.tapdata.tm.Permission.entity.PermissionEntity;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;

import java.util.List;
import java.util.Set;

public interface PermissionService {
    List<PermissionDto> getCurrentPermission(String userId);

    List<PermissionDto> getByNames(List<String> names, Status status);

    List<PermissionEntity> find(Filter filter);

    long count(Where where);

    List<String> getAllParentIds();

    List<PermissionEntity> getTopPermissionAndNoChild(Set<String> codes);
}
