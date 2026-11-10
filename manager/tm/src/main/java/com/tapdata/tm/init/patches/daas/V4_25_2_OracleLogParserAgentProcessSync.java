package com.tapdata.tm.init.patches.daas;

import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.proxyprocess.service.impl.OracleLogParserAgentProcessProjectionService;
import com.tapdata.tm.utils.SpringContextHelper;
import io.tapdata.utils.AppType;
import lombok.extern.slf4j.Slf4j;

/** Synchronizes the latest historical OracleLogParser states into AgentProcess. */
@Slf4j
@PatchAnnotation(appType = AppType.DAAS, version = "4.25-2")
public class V4_25_2_OracleLogParserAgentProcessSync extends AbsPatch {
    public V4_25_2_OracleLogParserAgentProcessSync(PatchType type, PatchVersion version) {
        super(type, version);
    }

    @Override
    public void run() {
        log.info("Synchronize historical OracleLogParser states into AgentProcess");
        OracleLogParserAgentProcessProjectionService projectionService = SpringContextHelper
                .getBean(OracleLogParserAgentProcessProjectionService.class);
        int projected = projectionService.initializeLatestReports();
        log.info("Synchronized {} OracleLogParser states into AgentProcess", projected);
    }
}
