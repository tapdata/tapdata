package io.tapdata.aspect;

import io.tapdata.schema.TapTableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/10/16 09:50
 */
public class TableInitFuncAspectTest {

    @Test
    public void test() {
        TableInitFuncAspect aspect = new TableInitFuncAspect();
        aspect.totals(10);
        Assertions.assertEquals(10, aspect.getTotals());

        aspect.tapTableMap(TapTableMap.create("nodeId"));
        Assertions.assertNotNull(aspect.getTapTableMap());

        aspect.completed("test", true);
        Assertions.assertEquals(1, aspect.getCompletedCounts());
        Assertions.assertNotNull(aspect.getCompletedMap());
    }

}
