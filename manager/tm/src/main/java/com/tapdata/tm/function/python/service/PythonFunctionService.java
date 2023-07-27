package com.tapdata.tm.function.python.service;

import cn.hutool.core.date.DateUtil;
import cn.hutool.extra.cglib.CglibUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.file.service.FileService;
import com.tapdata.tm.function.python.dto.PythonFunctionDto;
import com.tapdata.tm.function.python.entity.PythonFunctionEntity;
import com.tapdata.tm.function.python.repository.PythonFunctionRepository;
import com.tapdata.tm.utils.FunctionUtils;
import com.tapdata.tm.utils.GZIPUtil;
import lombok.NonNull;
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
 * @Author: Gavin
 * @Date: 2022/04/07
 * @Description:
 */
@Service
@Slf4j
public class PythonFunctionService extends BaseService<PythonFunctionDto, PythonFunctionEntity, ObjectId, PythonFunctionRepository> {

	@Autowired
	private FileService fileService;
	public PythonFunctionService(@NonNull PythonFunctionRepository repository) {
        super(repository, PythonFunctionDto.class, PythonFunctionEntity.class);
    }

	protected void beforeSave(PythonFunctionDto pythonFunction, UserDetail user) {
        String function_name = pythonFunction.getFunction_name();
        if (StringUtils.isNotBlank(function_name)) {
            checkName(pythonFunction.getId(), function_name, user);
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
			List<PythonFunctionDto> allJsFunctions = findAllJsFunctionsByIds(ids);
			String json = JsonUtil.toJsonUseJackson(allJsFunctions);
			AtomicReference<String> fileName = new AtomicReference<>("");
			String yyyymmdd = DateUtil.today().replaceAll("-", "");
			FunctionUtils.isTureOrFalse(ids.size() > 1).trueOrFalseHandle(
							() -> fileName.set("PythonFunction_batch" + "-" + yyyymmdd),
							() -> fileName.set(allJsFunctions.get(0).getFunction_name() + "-" + yyyymmdd)
			);
			fileService.viewImg1(json, response, fileName.get() + ".json.gz");
		}

    public void batchUpTask(MultipartFile multipartFile, UserDetail user, boolean cover) {
			if (!Objects.requireNonNull(multipartFile.getOriginalFilename()).endsWith("json.gz")) {
				//不支持其他的格式文件
				throw new BizException("PythonFunction.ImportFormatError");
			}
			try {
				byte[] bytes = GZIPUtil.unGzip(multipartFile.getBytes());

				String json = new String(bytes, StandardCharsets.UTF_8);

				List<PythonFunctionDto> pythonFunctionDtos = JsonUtil.parseJsonUseJackson(json, new TypeReference<List<PythonFunctionDto>>() {
				});
				if (pythonFunctionDtos == null) {
					//不支持其他的格式文件
					throw new BizException("PythonFunction.ImportFormatError");
				}

				for (PythonFunctionDto pythonFunctionDto : pythonFunctionDtos) {
					if (check(pythonFunctionDto)) {
						log.info("Check passed");
					}
					Query query = new Query(Criteria.where("_id").is(pythonFunctionDto.getId()).and("is_deleted").ne(true));
					query.fields().include("_id", "user_id");
					PythonFunctionDto one = findOne(query, user);
					if (one == null) {
						PythonFunctionDto one1 = findOne(new Query(Criteria.where("_id").is(pythonFunctionDto.getId()).and("is_deleted").ne(true)));
						if (one1 != null) {
							pythonFunctionDto.setId(null);
						}
					}
					if (one == null || cover) {
						ObjectId objectId = null;
						if (one != null) {
							objectId = one.getId();
						}

						if (one == null) {
							if (pythonFunctionDto.getId() == null) {
								pythonFunctionDto.setId(new ObjectId());
							}
							PythonFunctionEntity importEntity = repository.importEntity(convertToEntity(PythonFunctionEntity.class, pythonFunctionDto), user);
							log.info("import js function {}", importEntity);
//							jsFunctionDto = convertToDto(importEntity, JsFunctionDto.class);
						} else {
							updateByWhere(new Query(Criteria.where("_id").is(objectId)), pythonFunctionDto, user);
						}
					}
				}

			} catch (Exception e) {
				//e.printStackTrace();
				//不支持其他的格式文件
				if (e instanceof BizException) {
					throw (BizException) e;
				} else {
					throw new BizException("PythonFunction.ImportFormatError");
				}
			}
    }

	private boolean check(PythonFunctionDto pythonFunctionDto) {
		if (StringUtils.isEmpty(pythonFunctionDto.getFunction_name())) {
			throw new BizException("PythonFunction.ImportFormatError.function.name.empty");
		}
		if (StringUtils.isEmpty(pythonFunctionDto.getFunction_body())) {
			throw new BizException("PythonFunction.ImportFormatError.function.body.empty");
		}
		return true;
	}

	public List<PythonFunctionDto> findAllJsFunctionsByIds(List<String> list) {
		List<ObjectId> ids = list.stream().map(ObjectId::new).collect(Collectors.toList());

		Query query = new Query(Criteria.where("_id").in(ids));
		List<PythonFunctionEntity> entityList = findAllEntity(query);
		return CglibUtil.copyList(entityList, PythonFunctionDto::new);
	}
}