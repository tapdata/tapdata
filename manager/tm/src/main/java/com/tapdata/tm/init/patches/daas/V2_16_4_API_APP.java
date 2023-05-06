package com.tapdata.tm.init.patches.daas;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.sdk.util.AppType;
import com.tapdata.tm.task.constant.LdpDirEnum;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.SpringContextHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.List;


@PatchAnnotation(appType = AppType.DAAS, version = "2.16-4")
public class V2_16_4_API_APP extends AbsPatch {
    private static final Logger logger = LogManager.getLogger(V2_16_4_API_APP.class);

    public V2_16_4_API_APP(PatchType patchType, PatchVersion version) {
        super(patchType, version);
    }

    @Override
    public void run() {
        logger.info("Execute java patch: {}...", getClass().getName());
        UserService userService = SpringContextHelper.getBean(UserService.class);
        MetadataDefinitionService definitionService = SpringContextHelper.getBean(MetadataDefinitionService.class);

        Criteria criteria = Criteria.where("value").is(LdpDirEnum.LDP_DIR_TARGET.getValue())
                .and("item_type").is(LdpDirEnum.LDP_DIR_TARGET.getItemType());

        List<UserDetail> userDetails = userService.loadAllUser();
        for (UserDetail userDetail : userDetails) {
            try {
                MetadataDefinitionDto target = definitionService.findOne(new Query(criteria), userDetail);

                Criteria criteria1 = Criteria.where("value").is(LdpDirEnum.LDP_DIR_API.getValue())
                        .and("item_type").is(LdpDirEnum.LDP_DIR_API.getItemType())
                        .and("parent_id").is(target.getId().toHexString());
                MetadataDefinitionDto service = definitionService.findOne(new Query(criteria1), userDetail);
                if (service == null) {
                    MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
                    metadataDefinitionDto.setValue(LdpDirEnum.LDP_DIR_API.getValue());
                    metadataDefinitionDto.setItemType(Lists.of(LdpDirEnum.LDP_DIR_API.getItemType()));
                    metadataDefinitionDto.setParent_id(target.getId().toHexString());
                    service = definitionService.save(metadataDefinitionDto, userDetail);
                }

                Criteria exists = Criteria.where("item_type").is("app").and("parent_id").exists(false);
                definitionService.update(new Query(exists), Update.update("parent_id", service.getId().toHexString()), userDetail);
            } catch (Exception e) {
                logger.info("create target service directory failed. user name = {}", userDetail.getUsername() == null
                        ? userDetail.getEmail() : userDetail.getUsername());
            }
        }
    }
}
