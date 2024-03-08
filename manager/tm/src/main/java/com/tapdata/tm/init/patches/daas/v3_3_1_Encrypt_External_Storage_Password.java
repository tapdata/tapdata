package com.tapdata.tm.init.patches.daas;

import com.mongodb.ConnectionString;
import com.tapdata.tm.commons.externalStorage.ExternalStorageType;
import com.tapdata.tm.externalStorage.entity.ExternalStorageEntity;
import com.tapdata.tm.externalStorage.repository.ExternalStorageRepository;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import io.tapdata.utils.AppType;
import com.tapdata.tm.utils.AES256Util;
import com.tapdata.tm.utils.SpringContextHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2023-07-06 17:33
 **/
@PatchAnnotation(appType = AppType.DAAS, version = "3.3-1")
public class v3_3_1_Encrypt_External_Storage_Password extends AbsPatch {
	private static final Logger logger = LogManager.getLogger(v3_3_1_Encrypt_External_Storage_Password.class);
	public v3_3_1_Encrypt_External_Storage_Password(PatchType type, PatchVersion version) {
		super(type, version);
	}

	@Override
	public void run() {
		ExternalStorageRepository externalStorageRepository = SpringContextHelper.getBean(ExternalStorageRepository.class);
		Query query = Query.query(Criteria.where("type").is(ExternalStorageType.mongodb.name()));
		List<ExternalStorageEntity> mongodbExternals = externalStorageRepository.findAll(query);
		for (ExternalStorageEntity mongodbExternal : mongodbExternals) {
			String uri = mongodbExternal.getUri();
			if (StringUtils.isBlank(uri)) {
				continue;
			}
			// Check whether it is not encrypted by new ConnectionString
			try {
				new ConnectionString(uri);
			} catch (Exception ignored) {
				continue;
			}
			String encryptUri = AES256Util.Aes256Encode(uri);
			externalStorageRepository.update(Query.query(Criteria.where("_id").is(mongodbExternal.getId()).and("uri").is(uri)), Update.update("uri", encryptUri));
		}
	}
}
