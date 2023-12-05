package comm.utils;

import io.tapdata.common.utils.NumberUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class NumberUtilsTest {


    @Test
    public void testAddNumbersWithInteger() {
        // init param
        Number init = 1112347086;
        Number addNum = 1112347086;

        for (int i = 0; i < 4; i++) {
            // execution method
            init = NumberUtils.addNumbers(init, addNum);
        }

        Number actualData = init;

        int exceptedData = Integer.MAX_VALUE;

        assertTrue(exceptedData < actualData.longValue());
    }




}
