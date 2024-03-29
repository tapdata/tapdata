package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.MongoCollection;
import com.tapdata.tm.ds.repository.DataSourceRepository;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import io.tapdata.utils.AppType;
import com.tapdata.tm.shareCdcTableMapping.ShareCdcTableMappingDto;
import com.tapdata.tm.shareCdcTableMapping.repository.ShareCdcTableMappingRepository;
import com.tapdata.tm.shareCdcTableMapping.service.ShareCdcTableMappingService;
import com.tapdata.tm.utils.SpringContextHelper;
import org.apache.commons.collections.CollectionUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2023-07-06 17:33
 **/
@PatchAnnotation(appType = AppType.DAAS, version = "3.3-6")
public class v3_3_6_Share_Cdc_Table_Mapping_Fix extends AbsPatch {
	public static final String TAG = v3_3_6_Share_Cdc_Table_Mapping_Fix.class.getSimpleName();

	public v3_3_6_Share_Cdc_Table_Mapping_Fix(PatchType type, PatchVersion version) {
		super(type, version);
	}

	@Override
	public void run() {
		ShareCdcTableMappingRepository shareCdcTableMappingRepository = SpringContextHelper.getBean(ShareCdcTableMappingRepository.class);
		DataSourceRepository dataSourceRepository = SpringContextHelper.getBean(DataSourceRepository.class);

		MongoTemplate mongoTemplate = shareCdcTableMappingRepository.getMongoOperations();
		MongoCollection<Document> collection = mongoTemplate.getCollection("ShareCdcTableMapping");
		for (Document document : collection.find().sort(new Document("_id", -1))) {
			ObjectId id = document.getObjectId("_id");
			String version = document.getString("version");
			String tableName = document.getString("tableName");
			String connectionId = document.getString("connectionId");
			Document filter = new Document("_id", id);
			Document updateSet = new Document();

			dataSourceRepository.findById(connectionId).ifPresent(dataSourceEntity -> {
				updateSet.append("sign", ShareCdcTableMappingDto.genSign(connectionId, tableName));
				List<String> namespace = dataSourceEntity.getNamespace();
				String connNs = "";
				if (CollectionUtils.isNotEmpty(namespace)) {
					connNs = String.join(".", namespace);
				}
				if (ShareCdcTableMappingDto.VERSION_V2.equals(version)) {
					String externalStorageTableName = ShareCdcTableMappingService.genExternalStorageTableName(connectionId, connNs, tableName);
					updateSet.append("externalStorageTableName", externalStorageTableName);
				}
			});

			try {
				collection.updateOne(filter, new Document("$set", updateSet));
			} catch (Exception e) {
				collection.deleteOne(filter);
			}
		}
	}
}
