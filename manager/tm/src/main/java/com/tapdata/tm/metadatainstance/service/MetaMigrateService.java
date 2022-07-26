package com.tapdata.tm.metadatainstance.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatainstance.dto.MigrateResetTableDto;
import com.tapdata.tm.metadatainstance.dto.MigrateTableInfoDto;

public interface MetaMigrateService {
    void saveMigrateTableInfo(MigrateTableInfoDto tableInfo, UserDetail userDetail);

    void migrateResetAllTable(MigrateResetTableDto dto, UserDetail userDetail);
}
