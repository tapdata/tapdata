package com.tapdata.tm.fieldorigin.service;

import com.tapdata.tm.fieldorigin.entity.FieldOriginEntity;
import com.tapdata.tm.fieldorigin.entity.FieldOriginVersionEntity;

import java.util.Collection;
import java.util.List;

/**
 * Storage boundary of field origin versions and records.
 */
public interface FieldOriginStore {
    long nextVersion(String taskId);

    FieldOriginVersionEntity findActive(String taskId);

    FieldOriginVersionEntity findVersion(String taskId, long version);

    FieldOriginVersionEntity findLatestBuild(String taskId);

    void saveVersion(FieldOriginVersionEntity version);

    void insertRecords(List<FieldOriginEntity> records);

    void deleteRecords(String taskId, long version);

    /** Marks the version active and every other version of the task inactive. */
    void activate(String taskId, long version);

    /**
     * @param normalizedPaths null for all records; otherwise exact paths, children of the paths are matched as well
     */
    List<FieldOriginEntity> findRecords(String taskId, long version, String targetConnectionId, String targetTable, Collection<String> normalizedPaths);

    /** Versions of the task except the newest {@code keep} successful ones and the active one, oldest first. */
    List<FieldOriginVersionEntity> findPrunable(String taskId, int keep);

    void deleteVersion(String taskId, long version);
}
