package io.tapdata.flow.engine.V2.sharecdc.impl;

import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.hazelcast.PersistenceStorageConfig;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcContext;
import io.tapdata.flow.engine.V2.sharecdc.exception.ShareCdcUnsupportedException;

/**
 * @author samuel
 * @Description
 * @create 2022-02-22 21:12
 **/
public class ShareCdcHZReader extends ShareCdcBaseReader {

	@Override
	public void init(ShareCdcContext shareCdcContext) throws ShareCdcUnsupportedException {
		super.init(shareCdcContext);
	}
}
