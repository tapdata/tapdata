package com.tapdata.tm.ds.service.impl;

import cn.hutool.core.date.DateUtil;
import com.google.common.collect.Maps;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.dto.PdkSourceDto;
import com.tapdata.tm.ds.dto.PdkVersionCheckDto;
import com.tapdata.tm.ds.repository.PdkSourceRepository;
import com.tapdata.tm.ds.vo.PdkFileTypeEnum;
import com.tapdata.tm.file.service.FileService;
import com.tapdata.tm.tcm.service.TcmService;
import com.tapdata.tm.utils.*;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.temporal.ChronoUnit;
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

	public static final String METADATA_PDK_APIBUILD_NUMBER = "metadata.pdkAPIBuildNumber";
	public static final String METADATA_PDK_HASH = "metadata.pdkHash";
	private DataSourceDefinitionService dataSourceDefinitionService;
	private FileService fileService;
	private TcmService tcmService;
	private SettingsService settingsService;
	private PdkSourceRepository repository;

	@SuppressWarnings(value = "unchecked")
	public void uploadPdk(MultipartFile[] files, List<PdkSourceDto> pdkSourceDtos, boolean latest, UserDetail user) {
		Map<String, MultipartFile> iconMap = new HashMap<>();
		Map<String, MultipartFile> docMap = new HashMap<>();
		MultipartFile jarFile = null;
		for (MultipartFile multipartFile : files) {
			if (log.isDebugEnabled()) {
				log.debug("multipartFile name: {}, file size: {}", multipartFile.getOriginalFilename(), multipartFile.getSize());
			}
			if (multipartFile.getOriginalFilename() != null && multipartFile.getOriginalFilename().endsWith(".jar")) {
				jarFile = multipartFile;
			} else if (multipartFile.getOriginalFilename() != null && multipartFile.getOriginalFilename().endsWith(".md")) {
				docMap.put(multipartFile.getOriginalFilename(), multipartFile);
			} else {
				iconMap.put(multipartFile.getOriginalFilename(), multipartFile);
			}
		}

		if (jarFile == null) {
			throw new BizException("Invalid jar file, please upload a valid jar file.");
		}

        Map<String, Object> oemConfig = OEMReplaceUtil.getOEMConfigMap("connector/replace.json");
        for(PdkSourceDto pdkSourceDto : pdkSourceDtos) {
            // try to verify the version
            String version  = pdkSourceDto.getVersion();
            Integer pdkAPIBuildNumber = pdkSourceDto.getPdkAPIBuildNumber();

			// 只有 admin 用户的为 public 的 scope
			String scope = "customer";
			//云版没有admin所以采用这种方式
			if ("admin@admin.com".equals(user.getEmail()) || "18973231732".equals(user.getUsername())) {
				scope = "public";
			}
			Criteria criteria = Criteria.where("scope").is(scope)
					.and("group").is(pdkSourceDto.getGroup())
					.and("version").is(version)
					.and("pdkAPIBuildNumber").is(pdkAPIBuildNumber)
					.and("pdkId").is(pdkSourceDto.getId())
					.and("is_deleted").is(false);
			if ("customer".equals(scope)) {
				criteria.and("customId").is(user.getCustomerId());
			}
			DataSourceDefinitionDto oldDefinitionDto = dataSourceDefinitionService.findOne(new Query(criteria));
			if (!version.endsWith("-SNAPSHOT") && oldDefinitionDto != null) {
				throw new BizException("Only SNAPSHOT version of PDK can be overwritten, please make sure you've updated the version in your pom.");
			}

			DataSourceDefinitionDto definitionDto = new DataSourceDefinitionDto();
			BeanUtils.copyProperties(pdkSourceDto, definitionDto);
			//definitionDto.setId(Objects.nonNull(oldDefinitionDto) ? oldDefinitionDto.getId() : null);
			definitionDto.setConnectionType(pdkSourceDto.getType());
			definitionDto.setType(pdkSourceDto.getName());
			definitionDto.setPdkType("pdk");
			definitionDto.setPdkId(pdkSourceDto.getId());
			definitionDto.setJarFile(jarFile.getOriginalFilename());
			definitionDto.setJarTime(System.currentTimeMillis());
			definitionDto.setProperties(pdkSourceDto.getConfigOptions());
			definitionDto.setScope(scope);
			String pdkHash = definitionDto.calculatePdkHash(user.getCustomerId());
			definitionDto.setPdkHash(pdkHash);

			// remove snapshot overwritten file(jar/icons)
			if (oldDefinitionDto != null) {
				// change to async delete
				List<ObjectId> fileIds = new ArrayList<>();
				fileIds.add(MongoUtils.toObjectId(oldDefinitionDto.getJarRid()));
				if (oldDefinitionDto.getIcon() != null) {
					fileIds.add(MongoUtils.toObjectId(oldDefinitionDto.getIcon()));
				}
				Query query = Query.query(Criteria.where(METADATA_PDK_HASH).is(pdkHash)
						.and(METADATA_PDK_APIBUILD_NUMBER).is(pdkAPIBuildNumber));
				GridFSFindIterable result = fileService.find(query);
				result.forEach(gridFSFile ->
					fileIds.add(gridFSFile.getObjectId())
				);

				fileService.scheduledDeleteFiles(fileIds, "Upload new connector", "DatabaseTypes",
						oldDefinitionDto.getId(), user);
				log.debug("Delete original source {}", oldDefinitionDto.getId());
			}

			// upload the associated files(jar/icons)
			ObjectId jarObjectId = null;
			ObjectId iconObjectId = null;
			try {
				Map<String, Object> fileInfo = Maps.newHashMap();
				String md5 = PdkSourceUtils.getFileMD5(jarFile);
				fileInfo.put("pdkHash", pdkHash);
				fileInfo.put("pdkAPIBuildNumber", pdkAPIBuildNumber);
				fileInfo.put("md5", md5);

				// 1. upload jar file, only update once
				jarObjectId = fileService.storeFile(jarFile.getInputStream(), jarFile.getOriginalFilename(), null, fileInfo);

				// 2. upload the associated icon
				MultipartFile icon = iconMap.getOrDefault(pdkSourceDto.getIcon(), null);
				if (icon != null) {
					iconObjectId = fileService.storeFile(icon.getInputStream(), icon.getOriginalFilename(), null, fileInfo);
				}
				// 3. upload readeMe doc
                uploadDocs(docMap, pdkSourceDto.getMessages(), fileInfo, oemConfig);
                log.debug("Upload file to GridFS success");
			} catch (IOException e) {
				throw new BizException("SystemError", e);
			}

			definitionDto.setJarRid(jarObjectId.toHexString());
			if (iconObjectId != null) {
				definitionDto.setIcon(iconObjectId.toHexString());
			}

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
			definitionDto.setId(null);
			if (Objects.isNull(oldDefinitionDto)) {
				dataSourceDefinitionService.save(definitionDto, user);
			} else {
				dataSourceDefinitionService.upsert(Query.query(Criteria.where("_id").is(oldDefinitionDto.getId())), definitionDto, user);
			}
			log.debug("Upsert data source definition success");
		}
		log.debug("Upload pdk done.");
	}
	public String checkJarMD5(String pdkHash, int pdkBuildNumber, String fileName){
		String md5 = null;
		Criteria criteria = Criteria.where(METADATA_PDK_HASH).is(pdkHash);
		Query query = new Query(criteria);
		criteria.and(METADATA_PDK_APIBUILD_NUMBER).lte(pdkBuildNumber);
		criteria.and("filename").is(fileName);
		query.with(Sort.by(METADATA_PDK_APIBUILD_NUMBER).descending().and(Sort.by("uploadDate").descending()));
		GridFSFile gridFSFile = fileService.findOne(query);
		if(null != gridFSFile && null != gridFSFile.getMetadata()){
			md5 = (String) gridFSFile.getMetadata().get("md5");
		}
		return md5;
	}
	public String checkJarMD5(String pdkHash, int pdkBuildNumber){
		String md5 = null;
		Criteria criteria = Criteria.where(METADATA_PDK_HASH).is(pdkHash);
		Query query = new Query(criteria);
		criteria.and(METADATA_PDK_APIBUILD_NUMBER).lte(pdkBuildNumber);
		query.with(Sort.by(METADATA_PDK_APIBUILD_NUMBER).descending());
		GridFSFile gridFSFile = fileService.findOne(query);
		if(null != gridFSFile && null != gridFSFile.getMetadata()){
			md5 = (String) gridFSFile.getMetadata().get("md5");
		}
		return md5;
	}
	public String checkJarMD5(String pdkHash, String fileName){
		String md5 = null;
		Criteria criteria = Criteria.where(METADATA_PDK_HASH).is(pdkHash).and("filename").is(fileName);
		Query query = new Query(criteria);
		GridFSFile gridFSFile = fileService.findOne(query);
		if(null != gridFSFile && null != gridFSFile.getMetadata()){
			md5 = (String) gridFSFile.getMetadata().get("md5");
		}
		return md5;
	}

	public void uploadAndView(String pdkHash, Integer pdkBuildNumber, UserDetail user, PdkFileTypeEnum type, HttpServletResponse response) {
		Criteria criteria = Criteria.where("pdkHash").is(pdkHash);
		Query query = new Query(criteria);

		switch (type) {
			case JAR:
				query.fields().include("jarRid");
				criteria.and("pdkAPIBuildNumber").lte(pdkBuildNumber);
				break;
			case IMAGE:
				query.fields().include("icon");
				break;
			case MARKDOWN:
				query.fields().include("messages");
				break;
			default:
		}
		query.with(Sort.by("pdkAPIBuildNumber").descending());

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
				if (messages != null) {
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

    protected void uploadDocs(Map<String, MultipartFile> docMap, LinkedHashMap<String, Object> messages, Map<String, Object> fileInfo, Map<String, Object> oemConfig) throws IOException {
        if (docMap == null || docMap.isEmpty() || null == messages) return;

        Map<String, ObjectId> deduplicatioMap = new HashMap<>();
        for (Object v : messages.values()) {
            if (!(v instanceof Map)) continue;

            for (Map.Entry<String, Object> langEntry : ((Map<String, Object>) v).entrySet()) {
                if (null == langEntry.getValue()
                    || !("doc".equals(langEntry.getKey()) || langEntry.getKey().startsWith("doc:"))) {
                    continue;
                }

                String path = langEntry.getValue().toString();
                if (deduplicatioMap.containsKey(path)) {
                    langEntry.setValue(deduplicatioMap.get(path));
                    continue;
                }
				MultipartFile doc = docMap.getOrDefault(path, null);

                ObjectId docId = fileService.storeFile(
                    OEMReplaceUtil.replace(doc.getInputStream(), oemConfig),
                    doc.getOriginalFilename(),
                    null,
                    fileInfo);
                langEntry.setValue(docId);
                deduplicatioMap.put(path, docId);
            }
        }
    }

    public void downloadDoc(String pdkHash, Integer pdkBuildNumber, String filename, UserDetail user, HttpServletResponse response) throws IOException {
        Criteria criteria = Criteria.where("pdkHash").is(pdkHash);
        if (null != pdkBuildNumber) {
            criteria.and("pdkAPIBuildNumber").is(pdkBuildNumber);
        }

        Query query = new Query(criteria);
        query.fields().include("messages");
        query.with(Sort.by("pdkAPIBuildNumber").descending());

        DataSourceDefinitionDto one = dataSourceDefinitionService.findOne(query);
        if (one == null) {
            log.warn("Not found any datasource: {}", query.getQueryObject().toJson());
            response.sendError(404);
            return;
        }

        if ("customer".equals(one.getScope()) && !user.getCustomerId().equals(one.getCustomId())) {
            throw new BizException("PDK.DOWNLOAD.SOURCE.FAILED");
        }

        String language = MessageUtil.getLanguage();
        String resourceId = Optional.ofNullable(one.getMessages())
            .map(messages -> messages.get(language))
            .map(langMap-> ((Map<?,?>)langMap).get(filename))
            .map(Object::toString)
            .orElse("");

        if (StringUtils.isBlank(resourceId)) {
            response.sendError(404);
            return;
        }

        fileService.viewImg(MongoUtils.toObjectId(resourceId), response);
    }

	public List<PdkVersionCheckDto> versionCheck(int days) {
		List<PdkVersionCheckDto> result = Lists.newArrayList();

		Query query = Query.query(Criteria.where("is_deleted").is(false).and("scope").is("public"));
		List<DataSourceDefinitionDto> all = dataSourceDefinitionService.findAll(query);
		if (CollectionUtils.isNotEmpty(all)) {
//             get tcm build info
			String tcmReleaseTemp = tcmService.getLatestProductReleaseCreateTime();
//            String tcmReleaseTemp = "2023-04-28T03:27:04.856+00:00";
			if (StringUtils.isBlank(tcmReleaseTemp)) {
				tcmReleaseTemp = DateUtil.now();
			}

			List<DataSourceDefinitionDto> list = all.stream()
					.collect(Collectors.groupingBy(DataSourceDefinitionDto::getPdkId))
					.values().stream()
					.map(group -> group.stream()
							.max(Comparator.comparing(DataSourceDefinitionDto::getPdkAPIBuildNumber))
							.orElse(null))
					.collect(Collectors.toList());

			String finalTcmReleaseTemp = tcmReleaseTemp;
			list.forEach(info -> {
				PdkVersionCheckDto checkDto = PdkVersionCheckDto.builder().pdkId(info.getPdkId()).pdkVersion(info.getPdkAPIVersion()).pdkHash(info.getPdkHash()).isLatest(info.isLatest()).build();

				Date buildDate;
				Map<String, String> manifest = info.getManifest();
				if (Objects.nonNull(manifest) && Objects.nonNull(manifest.get("Git-Build-Time"))) {
					String buildTime = manifest.get("Git-Build-Time");
					buildDate = DateUtil.parse(buildTime, "yyyy-MM-dd'T'HH:mm:ssZ");

					checkDto.setGitBuildUserName(manifest.get("Git-Build-User-Name"));
					checkDto.setGitBranch(manifest.get("Git-Branch"));
					checkDto.setGitCommitId(manifest.get("Git-Commit-Id"));

				} else {
					buildDate = info.getLastUpdAt();
				}
				checkDto.setGitBuildTime(DateUtil.formatDateTime(buildDate));

				Date tcmReleaseDate = DateUtil.parseDate(finalTcmReleaseTemp);
				boolean isLatest = tcmReleaseDate.before(buildDate) || ChronoUnit.DAYS.between(buildDate.toInstant(), tcmReleaseDate.toInstant()) <= days;
				// compare with tcm build info
				checkDto.setLatest(isLatest);
				result.add(checkDto);
			});
		}

		// sort by isLatest asc
		result.sort(Comparator.comparing(PdkVersionCheckDto::isLatest));
		return result;
	}
}
