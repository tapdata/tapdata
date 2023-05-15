package com.tapdata.tm.function.service;

import cn.hutool.core.date.DateUtil;
import cn.hutool.extra.cglib.CglibUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.file.service.FileService;
import com.tapdata.tm.function.dto.JsFunctionDto;
import com.tapdata.tm.function.entity.JsFunctionEntity;
import com.tapdata.tm.function.repository.JsFunctionRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.bean.TaskUpAndLoadDto;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.utils.FunctionUtils;
import com.tapdata.tm.utils.GZIPUtil;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @Author:
 * @Date: 2022/04/07
 * @Description:
 */
@Service
@Slf4j
public class JsFunctionService extends BaseService<JsFunctionDto, JsFunctionEntity, ObjectId, JsFunctionRepository> {

	@Autowired
	private FileService fileService;
	public JsFunctionService(@NonNull JsFunctionRepository repository) {
        super(repository, JsFunctionDto.class, JsFunctionEntity.class);
    }

    protected void beforeSave(JsFunctionDto jsFunction, UserDetail user) {
        String function_name = jsFunction.getFunction_name();
        if (StringUtils.isNotBlank(function_name)) {
            checkName(jsFunction.getId(), function_name, user);
        }
    }


    public void checkName(ObjectId id, String name, UserDetail user) {
        Criteria criteriaSystem = Criteria.where("function_name").is(name).and("type").is("system");

        long count = count(new Query(criteriaSystem));
        if (count != 0) {
            throw new BizException("Function.Name.Exist");
        }


        Criteria criteriaCustom = Criteria.where("function_name").is(name).and("type").ne("system");
        if (id != null) {
            criteriaCustom.and("_id").ne(id);
        }

        Query query = new Query(criteriaCustom);
        count = count(query, user);
        if (count != 0) {
            throw new BizException("Function.Name.Exist");
        }
    }

    public void batchLoadTask(HttpServletResponse response, List<String> ids, UserDetail loginUser) {
			List<JsFunctionDto> allJsFunctions = findAllJsFunctionsByIds(ids);
			String json = JsonUtil.toJsonUseJackson(allJsFunctions);
			AtomicReference<String> fileName = new AtomicReference<>("");
			String yyyymmdd = DateUtil.today().replaceAll("-", "");
			FunctionUtils.isTureOrFalse(ids.size() > 1).trueOrFalseHandle(
							() -> fileName.set("JsFunction_batch" + "-" + yyyymmdd),
							() -> fileName.set(allJsFunctions.get(0).getFunction_name() + "-" + yyyymmdd)
			);
			fileService.viewImg1(json, response, fileName.get() + ".json.gz");
		}

    public void batchUpTask(MultipartFile multipartFile, UserDetail user, boolean cover) {
			if (!Objects.requireNonNull(multipartFile.getOriginalFilename()).endsWith("json.gz")) {
				//不支持其他的格式文件
				throw new BizException("JsFunction.ImportFormatError");
			}
			try {
				byte[] bytes = GZIPUtil.unGzip(multipartFile.getBytes());

				String json = new String(bytes, StandardCharsets.UTF_8);

				List<JsFunctionDto> jsFunctionDtos = JsonUtil.parseJsonUseJackson(json, new TypeReference<List<JsFunctionDto>>() {
				});
				if (jsFunctionDtos == null) {
					//不支持其他的格式文件
					throw new BizException("JsFunction.ImportFormatError");
				}

				for (JsFunctionDto jsFunctionDto : jsFunctionDtos) {
					Query query = new Query(Criteria.where("_id").is(jsFunctionDto.getId()).and("is_deleted").ne(true));
					query.fields().include("_id", "user_id");
					JsFunctionDto one = findOne(query, user);
					if (one == null) {
						JsFunctionDto one1 = findOne(new Query(Criteria.where("_id").is(jsFunctionDto.getId()).and("is_deleted").ne(true)));
						if (one1 != null) {
							jsFunctionDto.setId(null);
						}
					}
					if (one == null || cover) {
						ObjectId objectId = null;
						if (one != null) {
							objectId = one.getId();
						}

						if (one == null) {
							if (jsFunctionDto.getId() == null) {
								jsFunctionDto.setId(new ObjectId());
							}
							JsFunctionEntity importEntity = repository.importEntity(convertToEntity(JsFunctionEntity.class, jsFunctionDto), user);
							log.info("import js function {}", importEntity);
//							jsFunctionDto = convertToDto(importEntity, JsFunctionDto.class);
						} else {
							updateByWhere(new Query(Criteria.where("_id").is(objectId)), jsFunctionDto, user);
						}
					}
				}

			} catch (Exception e) {
				//e.printStackTrace();
				//不支持其他的格式文件
				throw new BizException("JsFunction.ImportFormatError");
			}
    }

	public List<JsFunctionDto> findAllJsFunctionsByIds(List<String> list) {
		List<ObjectId> ids = list.stream().map(ObjectId::new).collect(Collectors.toList());

		Query query = new Query(Criteria.where("_id").in(ids));
		List<JsFunctionEntity> entityList = findAllEntity(query);
		return CglibUtil.copyList(entityList, JsFunctionDto::new);
	}
}