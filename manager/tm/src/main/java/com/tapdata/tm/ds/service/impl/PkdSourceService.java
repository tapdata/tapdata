package com.tapdata.tm.ds.service.impl;

import com.google.common.collect.Maps;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.ds.dto.PdkSourceDto;
import com.tapdata.tm.ds.vo.PdkFileTypeEnum;
import com.tapdata.tm.file.service.FileService;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author: Zed
 * @Date: 2022/2/23
 * @Description: pdk相关的业务处理
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class PkdSourceService {

    private DataSourceDefinitionService dataSourceDefinitionService;
    private FileService fileService;

    @SuppressWarnings (value="unchecked")
    public void uploadPdk(CommonsMultipartFile [] files, List<PdkSourceDto> pdkSourceDtos, boolean latest, UserDetail user) {
        Map<String, CommonsMultipartFile> iconMap = new HashMap<>();
        Map<String, CommonsMultipartFile> docMap = new HashMap<>();
        CommonsMultipartFile jarFile = null;
        for (CommonsMultipartFile multipartFile : files) {
            if (multipartFile.getOriginalFilename() != null && multipartFile.getOriginalFilename().endsWith(".jar")) {
                jarFile = multipartFile;
            } else if (multipartFile.getOriginalFilename() != null && multipartFile.getOriginalFilename().endsWith(".md")) {
                docMap.put(multipartFile.getFileItem().getName(), multipartFile);
            } else {
                iconMap.put(multipartFile.getFileItem().getName(), multipartFile);
            }
        }

        if (jarFile == null) {
            throw new BizException("Invalid jar file, please upload a valid jar file.");
        }

        ObjectId jarObjectId = null;
        for(PdkSourceDto pdkSourceDto : pdkSourceDtos) {
            // try to verify the version
            String version  = pdkSourceDto.getVersion();

            // 只有 admin 用户的为 public 的 scope
            String scope = "customer";
            //云版没有admin所以采用这种方式
            if ("admin@admin.com".equals(user.getEmail()) || "18973231732".equals(user.getUsername())) {
                scope = "public";
            }
            Criteria criteria = Criteria.where("scope").is(scope)
                    .and("group").is(pdkSourceDto.getGroup())
                    .and("version").is(version)
                    .and("pdkId").is(pdkSourceDto.getId())
                    .and("is_deleted").is(false);
            if ("customer".equals(scope)) {
                criteria.and("customId").is(user.getCustomerId());
            }
            DataSourceDefinitionDto oldDefinitionDto = dataSourceDefinitionService.findOne(new Query(criteria));
            if (!version.endsWith("-SNAPSHOT") && oldDefinitionDto != null) {
                throw new BizException("Only SNAPSHOT version of PDK can be overwritten, please make sure you've updated the version in your pom.");
            }

            // upload the associated files(jar/icons)
            ObjectId iconObjectId = null;
            Map<String, String> langMap = Maps.newHashMap();
            try {
                // 1. upload jar file, only update once
                if (jarObjectId == null) {
                    jarObjectId = fileService.storeFile(jarFile.getInputStream(), jarFile.getOriginalFilename(), null, new HashMap<>());
                }
                // 2. upload the associated icon
                CommonsMultipartFile icon = iconMap.getOrDefault(pdkSourceDto.getIcon(), null);
                if (icon != null) {
                    iconObjectId = fileService.storeFile(icon.getInputStream(), icon.getOriginalFilename(), null, new HashMap<>());
                }
                // 3. upload readeMe doc
                if (!docMap.isEmpty()) {
                    if (Objects.nonNull(pdkSourceDto.getMessages())) {
                        pdkSourceDto.getMessages().forEach((k, v) -> {
                            if (v instanceof Map && Objects.nonNull(((Map<?, ?>)v).get("doc"))) {
                                langMap.put(k, ((Map<?, ?>)v).get("doc").toString());
                            }
                        });
                    }
                    if (!langMap.isEmpty()) {
                        List<String> pathList = langMap.values().stream().distinct().collect(Collectors.toList());

                        Map<String, ObjectId> pathMap = new HashMap<>();
                        pathList.forEach(path -> {
                            if (docMap.containsKey(path)) {
                                CommonsMultipartFile doc = docMap.getOrDefault(path, null);
                                try {
                                    ObjectId docId = fileService.storeFile(doc.getInputStream(), doc.getOriginalFilename(), null, new HashMap<>());
                                    pathMap.put(path, docId);
                                } catch (IOException e) {
                                    throw new BizException(e);
                                }
                            }
                        });

                        pdkSourceDto.getMessages().forEach((k, v) -> {
                            if (v instanceof Map && Objects.nonNull(((Map<?, ?>)v).get("doc"))) {
                                String path = ((Map<?, ?>)v).get("doc").toString();
                                if (pathMap.containsKey(path)) {
                                    ((Map<String, Object>) v).put("doc", pathMap.get(path));
                                }
                            }
                        });
                    }
                }

            } catch (IOException e) {
                throw new BizException("SystemError");
            }

            DataSourceDefinitionDto definitionDto = new DataSourceDefinitionDto();
            BeanUtils.copyProperties(pdkSourceDto, definitionDto);
            definitionDto.setId(null);
            definitionDto.setConnectionType(pdkSourceDto.getType());
            definitionDto.setType(pdkSourceDto.getName());
            definitionDto.setPdkType("pdk");
            definitionDto.setPdkId(pdkSourceDto.getId());
            definitionDto.setJarFile(jarFile.getOriginalFilename());
            definitionDto.setJarRid(jarObjectId.toHexString());
            definitionDto.setJarTime(System.currentTimeMillis());
            definitionDto.setProperties(pdkSourceDto.getConfigOptions());
            definitionDto.setScope(scope);
            if (iconObjectId != null) {
                definitionDto.setIcon(iconObjectId.toHexString());
            }
            String pdkHash = definitionDto.calculatePdkHash(user.getCustomerId());
            definitionDto.setPdkHash(pdkHash);

            if (latest) {
                definitionDto.setLatest(true);
                // set last latest to false
                Criteria criteriaLatest = Criteria.where("scope").is(scope)
                        .and("pdkId").is(pdkSourceDto.getId())
                        .and("group").is(pdkSourceDto.getGroup())
                        .and("latest").is(true)
                        .and("is_deleted").is(false);
                if ("customer".equals(scope)) {
                    criteriaLatest.and("customId").is(user.getCustomerId());
                }
                Update removeLatest = Update.update("latest", false);
                dataSourceDefinitionService.update(new Query(criteriaLatest), removeLatest);
            }
            dataSourceDefinitionService.save(definitionDto, user);

            // remove snapshot overwritten file(jar/icons)
            if (oldDefinitionDto != null) {
                fileService.deleteFileById(MongoUtils.toObjectId(oldDefinitionDto.getJarRid()));
                if (oldDefinitionDto.getIcon() != null) {
                    fileService.deleteFileById(MongoUtils.toObjectId(oldDefinitionDto.getIcon()));
                }
                dataSourceDefinitionService.deleteById(oldDefinitionDto.getId());
            }
        }
    }

    public void uploadAndView(String pdkHash, UserDetail user, PdkFileTypeEnum type, HttpServletResponse response) {
        Criteria criteria = Criteria.where("pdkHash").is(pdkHash);
        Query query = new Query(criteria);

        switch (type) {
            case JAR:
                query.fields().include("jarRid");
                break;
            case IMAGE:
            query.fields().include("icon");
                break;
            case MARKDOWN:
            query.fields().include("messages");
                break;
            default:
        }
        query.with(Sort.by("createTime").descending());

        DataSourceDefinitionDto one = dataSourceDefinitionService.findOne(query);

        if (one == null) {
            log.error("pdkHash is error pdkHash:{}", pdkHash);
            try {
                response.sendError(404);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return;
        }

        if ("customer".equals(one.getScope()) && !user.getCustomerId().equals(one.getCustomId())) {
            throw new BizException("PDK.DOWNLOAD.SOURCE.FAILED");
        }

        String resourceId;
        switch (type) {
            case JAR:
                resourceId = one.getJarRid();
                break;
            case IMAGE:
                resourceId = one.getIcon();
                break;
            case MARKDOWN:
                String language = MessageUtil.getLanguage();
                LinkedHashMap<String, Object> messages = one.getMessages();
                if(messages != null) {
                    Object lan = messages.get(language);
                    if (Objects.nonNull(lan) && Objects.nonNull(((Map<?, ?>) lan).get("doc"))) {
                        Object docId = ((Map<?, ?>) lan).get("doc");
                        resourceId = docId.toString();
                    } else {
                        resourceId = "";
                    }
                } else {
                    resourceId = "";
                }
                break;
            default:
                resourceId = "";
        }

        if (StringUtils.isBlank(resourceId)) {
//            throw new BizException("SystemError");
            try {
                response.sendError(404);
                return;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        fileService.viewImg(MongoUtils.toObjectId(resourceId), response);
    }
}
