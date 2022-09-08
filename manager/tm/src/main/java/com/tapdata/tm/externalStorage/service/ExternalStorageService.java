package com.tapdata.tm.externalStorage.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.externalStorage.dto.ExternalStorageDto;
import com.tapdata.tm.externalStorage.entity.ExternalStorageEntity;
import com.tapdata.tm.externalStorage.entity.ExternalStorageType;
import com.tapdata.tm.externalStorage.repository.ExternalStorageRepository;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

/**
 * @Author: sam
 * @Date: 2022/09/07
 * @Description:
 */
@Service
@Slf4j
public class ExternalStorageService extends BaseService<ExternalStorageDto, ExternalStorageEntity, ObjectId, ExternalStorageRepository> {

	public static final int DEFAULT_TTL_DAY = 3;

	public ExternalStorageService(@NonNull ExternalStorageRepository repository) {
		super(repository, ExternalStorageDto.class, ExternalStorageEntity.class);
	}

	protected void beforeSave(ExternalStorageDto externalStorage, UserDetail user) {
		if (StringUtils.isBlank(externalStorage.getName())) {
			throw new BizException("External.Storage.Name.Blank");
		}
		ExternalStorageDto exists = findOne(Query.query(Criteria.where("name").is(externalStorage.getName())));
		if (null != exists) {
			throw new BizException("External.Storage.Name.Exists", externalStorage.getName());
		}
		if (StringUtils.isBlank(externalStorage.getType())) {
			throw new BizException("External.Storage.Type.Blank");
		}
		ExternalStorageType externalStorageType;
		try {
			externalStorageType = ExternalStorageType.valueOf(externalStorage.getType());
		} catch (Throwable e) {
			throw new BizException("External.Storage.Type.Invalid", e, externalStorage.getType());
		}
		switch (externalStorageType) {
			case mongodb:
				if (StringUtils.isBlank(externalStorage.getUri())) {
					throw new BizException("External.Storage.MongoDB.Uri.Blank");
				}
				if (StringUtils.isBlank(externalStorage.getTable())) {
					throw new BizException("External.Storage.MongoDB.Table.Blank");
				}
				break;
			case rocksdb:
				if (StringUtils.isBlank(externalStorage.getUri())) {
					throw new BizException("External.Storage.RocksDB.Path.Blank");
				}
				break;
		}
		Integer ttlDay = externalStorage.getTtlDay();
		if (null == ttlDay || ttlDay.compareTo(0) <= 0) {
			externalStorage.setTtlDay(DEFAULT_TTL_DAY);
		}
	}
}