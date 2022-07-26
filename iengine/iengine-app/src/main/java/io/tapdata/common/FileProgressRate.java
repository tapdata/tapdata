package io.tapdata.common;

import com.mongodb.MongoClient;
import com.tapdata.entity.Connections;
import com.tapdata.entity.Job;
import com.tapdata.entity.ProgressRateStats;
import io.tapdata.Target;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2021-06-22 22:05
 **/
public class FileProgressRate implements DatabaseProgressRate {

	private Logger logger = LogManager.getLogger(FileProgressRate.class);

	@Override
	public ProgressRateStats progressRateInfo(Job job, Connections sourceConn, Connections targetConn, MongoClient targetMongoClient, List<Target> targets) {
		return null;
	}

	@Override
	public long getCdcLastTime(Connections sourceConn, Job job) {
//    Object offset = job.getOffset();
//    FileOffset fileOffset;
//    if (offset instanceof TapdataOffset) {
//      Object jobOffset = ((TapdataOffset) offset).getOffset();
//      if (jobOffset instanceof String) {
//        try {
//          if (jobOffset.toString().startsWith(StringCompression.COMPRESSION_PREFIX)) {
//            jobOffset = StringUtils.removeStart(jobOffset.toString(), StringCompression.COMPRESSION_PREFIX);
//            jobOffset = StringCompression.uncompress(jobOffset.toString());
//          }
//          fileOffset = JSONUtil.json2POJO(jobOffset.toString(), new TypeReference<FileOffset>() {
//          });
//        } catch (Exception e) {
//          String err = "Get file last cdc time failed, can't read offset: " + offset + ", err: " + e.getMessage() + ", stack: " + Log4jUtil.getStackString(e);
//          logger.warn(err);
//          return 0;
//        }
//
//        if (fileOffset == null) {
//          return 0;
//        }
//
//        return fileOffset.getTimestamp() != null ? fileOffset.getTimestamp() : 0;
//      }
//    }
		return 0;
	}
}
