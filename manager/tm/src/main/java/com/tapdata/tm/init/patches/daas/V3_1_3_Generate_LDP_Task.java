package com.tapdata.tm.init.patches.daas;

import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.sdk.util.AppType;
import com.tapdata.tm.task.service.LdpService;
import com.tapdata.tm.utils.SpringContextHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


@PatchAnnotation(appType = AppType.DAAS, version = "3.1-3")
public class V3_1_3_Generate_LDP_Task extends AbsPatch {
    private static final Logger logger = LogManager.getLogger(V3_1_3_Generate_LDP_Task.class);

    public V3_1_3_Generate_LDP_Task(PatchType type, PatchVersion version) {
        super(type, version);
    }

    @Override
    public void run() {
        logger.info("Execute java patch: {}...", getClass().getName());
        LdpService ldpService = SpringContextHelper.getBean(LdpService.class);
        ldpService.generateLdpTaskByOld();
    }
}
