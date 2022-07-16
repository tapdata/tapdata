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
    // Check hazelcast persistence storage setting
    if (!PersistenceStorageConfig.getInstance().isEnable()) {
      String err = "Hazelcast persistence storage setting is invalid; ";
      ShareCdcUnsupportedException shareCdcUnsupportedException;
      if (PersistenceStorageConfig.getInstance().getThrowable() != null) {
        err += "Error: " + PersistenceStorageConfig.getInstance().getThrowable().getMessage() + "\n" + Log4jUtil.getStackString(PersistenceStorageConfig.getInstance().getThrowable());
        shareCdcUnsupportedException = new ShareCdcUnsupportedException(err, PersistenceStorageConfig.getInstance().getThrowable(), true);
      } else {
        shareCdcUnsupportedException = new ShareCdcUnsupportedException(err, true);
      }
      throw shareCdcUnsupportedException;
    }
  }
}
