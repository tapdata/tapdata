package io.tapdata.common;

import com.tapdata.entity.DatabaseTypeEnum;
import io.tapdata.TapInterface;
import io.tapdata.entity.Lib;
import io.tapdata.entity.LibSupported;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SupportUtil {

	private List<LibSupported> supporteds = new ArrayList<>();

	public SupportUtil(String supportedStr, List<Lib> libs, List<DatabaseTypeEnum.DatabaseType> databaseTypes) {
		Map<String, Boolean> supportedAllFalse = new HashMap<>();
		Map<String, Boolean> supportedAllTrue = new HashMap<>();
		Map<String, Lib> libsMap = new HashMap<>();

		if (StringUtils.isNotBlank(supportedStr)) {
			String[] supportedStrs = supportedStr.split(",");
			if (supportedStrs.length > 0) {
				for (String str : supportedStrs) {
					supportedAllTrue.put(str, true);
					supportedAllFalse.put(str, false);
				}

				for (Lib lib : libs) {
					List<String> libDatabaseTypes = lib.getDatabaseTypes();
					for (String libDatabaseType : libDatabaseTypes) {
						libsMap.put(libDatabaseType, lib);
					}
				}

				for (DatabaseTypeEnum.DatabaseType databaseType : databaseTypes) {
					String type = databaseType.getType();
					if (libsMap.containsKey(type)
							&& !StringUtils.equalsAnyIgnoreCase(type, DatabaseTypeEnum.MONGODB.getType(), DatabaseTypeEnum.ALIYUN_MONGODB.getType())) {
						Lib lib = libsMap.get(type);
						try {
							TapInterface tapInterface = (TapInterface) lib.getClazz().newInstance();

							Map<String, Boolean> supported = tapInterface.getSupported(supportedStrs);

							if (MapUtils.isNotEmpty(supported)) {

//                                if (!lib.getSource()) {
//                                    supported.put(SupportConstant.INITIAL_SYNC, false);
//                                    supported.put(SupportConstant.INCREAMENTAL_SYNC, false);
//                                }

								if (!lib.getTarget()) {
									supported.put(SupportConstant.ON_DATA, false);
//                                    supported.put(SupportConstant.SYNC_PROGRESS, false);
									supported.put(SupportConstant.IS_MERGE, false);
								}

								if (!lib.getSource() && !lib.getTarget()) {
									supported.put(SupportConstant.STATS, false);
									supported.put(SupportConstant.DATA_VALIDATE, false);
								}

								if (!supported.containsKey(SupportConstant.CUSTOM_MAPPING)) {
									supported.put(SupportConstant.CUSTOM_MAPPING, false);
								}

								supporteds.add(new LibSupported(type, supported));
							} else {
								supporteds.add(new LibSupported(type, supportedAllFalse));
							}


						} catch (InstantiationException | IllegalAccessException e) {
							throw new RuntimeException("Instantiation TapInterface error: " + e.getMessage(), e);
						}
					} else {
						if (StringUtils.equalsAnyIgnoreCase(type, DatabaseTypeEnum.MONGODB.getType(), DatabaseTypeEnum.ALIYUN_MONGODB.getType())) {
							supportedAllTrue.put(SupportConstant.IS_MERGE, true);
						} else {
							supportedAllTrue.put(SupportConstant.IS_MERGE, false);
						}
						supporteds.add(new LibSupported(type, supportedAllTrue));
					}
				}
			}
		} else {
			throw new RuntimeException("Handle lib's supported list error, because libSupported is missing in Settings.");
		}
	}

	public List<LibSupported> getSupporteds() {
		return supporteds;
	}
}
