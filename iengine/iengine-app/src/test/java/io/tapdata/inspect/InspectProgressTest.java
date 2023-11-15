package inspect;

import com.tapdata.entity.Connections;
import com.tapdata.entity.inspect.InspectDataSource;
import com.tapdata.entity.inspect.InspectTask;
import io.tapdata.inspect.InspectTaskContext;
import io.tapdata.inspect.compare.TableRowContentInspectJob;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

public class InspectProgressTest {



    /**
     * 测试数据校验的保留4位小数最大是0.9999
     * test getProgress function
     */
    @Test
    public void testGetProgress(){
        // input param
        InspectTask inspectTask = new InspectTask();
        InspectDataSource inspectSource = new InspectDataSource();
        InspectDataSource inspectTarget = new InspectDataSource();
        inspectTask.setSource(inspectSource);
        inspectTask.setTarget(inspectTarget);


        inspectTask.setTaskId("test");
        Connections source = new Connections();
        source.setName("testSource");
        Connections target = new Connections();
        target.setName("targetSource");
        InspectTaskContext inspectTaskContext = new InspectTaskContext("test", inspectTask,
                source, target, null, null, null, null, null, null);
        TableRowContentInspectJob tableRowContentInspectJob = new TableRowContentInspectJob(inspectTaskContext);

        // input query data
        ReflectionTestUtils.setField(tableRowContentInspectJob, "current", 99999999999L);
        ReflectionTestUtils.setField(tableRowContentInspectJob, "max", 999);

        // execution method
        double actualData = ReflectionTestUtils.invokeMethod(tableRowContentInspectJob, "getProgress");

        // expected data
        double expectedData = 0.9999;

        // output results
        Assert.assertTrue(expectedData >= actualData);
    }
}
