package com.tapdata.tm;

import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/4/11 下午3:59
 */
public class TestPattern {

    @Test
    public void testPattern() {
        String str = "^INTERVAL YEAR.* TO MONTH.*$";
        System.out.println(str);
        Pattern pattern = Pattern.compile(str, Pattern.CASE_INSENSITIVE);

        Assert.assertTrue(pattern.matcher("INTERVAL YEAR TO MONTH").matches());
        Assert.assertTrue(pattern.matcher("interval year to month").matches());
        Assert.assertTrue(pattern.matcher("interval YEAR TO month").matches());
        Assert.assertTrue(pattern.matcher("INTERVAL YEAR TO MONTH(2)").matches());
        Assert.assertTrue(pattern.matcher("INTERVAL YEAR(4) TO MONTH(2)").matches());
        Assert.assertTrue(pattern.matcher("interval year(4) to month(2)").matches());
    }
}
