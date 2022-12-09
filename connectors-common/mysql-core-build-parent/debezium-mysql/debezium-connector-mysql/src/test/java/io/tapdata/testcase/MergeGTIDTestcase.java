package io.tapdata.testcase;

import io.debezium.connector.mysql.GtidSet;
import io.debezium.connector.mysql.utils.MergeGTIDUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * merger GTID testcase
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/11/28 10:58 Create
 */
public class MergeGTIDTestcase {

    @Test
    public void testOverMaxIgnore() {
        MergeGTIDUtils.Mode mode = MergeGTIDUtils.Mode.OverMaxIgnore;
        String serverId = UUID.randomUUID().toString();

        MergeGTIDUtils merger = new MergeGTIDUtils();
        merger.add(serverId, 30, 40, mode);
        Assert.assertEquals("前置", serverId + ":10-20:30-40", merger.add(serverId, 10, 20, mode).toString());
        Assert.assertEquals("后置1", serverId + ":10-20:30-40:50-60", merger.add(serverId, 50, 60, mode).toString());
        Assert.assertEquals("后置2", serverId + ":10-20:30-40:50-60:70-80", merger.add(serverId, 70, 80, mode).toString());
        Assert.assertEquals("后置3", serverId + ":10-20:30-40:50-60:70-80:90-99", merger.add(serverId, 90, 99, mode).toString());
        Assert.assertEquals("连接两段", serverId + ":10-20:30-60:70-80:90-99", merger.add(serverId, 38, 55, mode).toString());
        Assert.assertEquals("包含多段", serverId + ":10-20:25-85:90-99", merger.add(serverId, 25, 85, mode).toString());
        Assert.assertEquals("被包含", serverId + ":10-20:25-85:90-99", merger.add(serverId, 28, 33, mode).toString());
        Assert.assertEquals("边界1", serverId + ":10-22:25-85:90-99", merger.add(serverId, 20, 22, mode).toString());
        Assert.assertEquals("边界2", serverId + ":10-22:24-85:90-99", merger.add(serverId, 24, 25, mode).toString());
        Assert.assertEquals("边界3", serverId + ":10-85:90-99", merger.add(serverId, 22, 24, mode).toString());

        GtidSet gtidSet = new GtidSet(merger.toString());
        Assert.assertEquals("转换格式验证", serverId + ":10-85:90-99", gtidSet.toString());

    }
}
