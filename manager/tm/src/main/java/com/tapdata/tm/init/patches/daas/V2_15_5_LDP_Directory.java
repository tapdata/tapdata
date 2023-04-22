package com.tapdata.tm.init.patches.daas;


import com.google.common.collect.Lists;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.sdk.util.AppType;
import com.tapdata.tm.task.constant.LdpDirEnum;
import com.tapdata.tm.task.service.LdpService;
import com.tapdata.tm.user.dto.UserDto;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.SpringContextHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@PatchAnnotation(appType = AppType.DAAS, version = "2.15-5")
public class V2_15_5_LDP_Directory extends AbsPatch {
    public V2_15_5_LDP_Directory(PatchType type, PatchVersion version) {
        super(type, version);
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        log.info("Execute java patch: {}...", getClass().getName());
        UserService userService = SpringContextHelper.getBean(UserService.class);
        LdpService ldpService = SpringContextHelper.getBean(LdpService.class);
        MetadataDefinitionService metadataDefinitionService = SpringContextHelper.getBean(MetadataDefinitionService.class);

        Map<String, String> oldLdpMap = metadataDefinitionService.ldpDirKvs();


        List<UserDetail> userDetails = userService.loadAllUser();
        for (UserDetail userDetail : userDetails) {
            ldpService.addLdpDirectory(userDetail, oldLdpMap);
        }

        metadataDefinitionService.dellOldLdpDirs();
    }
}
