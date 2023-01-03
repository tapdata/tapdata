package connector;

import org.junit.Assert;
import org.junit.Test;

public class MatcherTest {
    @Test
    public void testMatch(){
        String functionInvoker = "(3)[\"A\",\"B\",\"C\"]";
        if (functionInvoker.matches("\\(([0-9]+)\\)\\[.*]")){
            functionInvoker = functionInvoker.replaceFirst("\\(([0-9]+)\\)","");
        }
        Assert.assertEquals("+",functionInvoker,"[\"A\",\"B\",\"C\"]");

        functionInvoker = "(3)[\"(3)A\",\"B\",\"C\"]";
        if (functionInvoker.matches("\\(([0-9]+)\\)\\[.*]")){
            functionInvoker = functionInvoker.replaceFirst("\\(([0-9]+)\\)","");
        }
        Assert.assertEquals("+",functionInvoker,"[\"(3)A\",\"B\",\"C\"]");

        functionInvoker = "[\"(3)A\",\"B\",\"C\"]";
        if (functionInvoker.matches("\\(([0-9]+)\\)\\[.*]")){
            functionInvoker = functionInvoker.replaceFirst("\\(([0-9]+)\\)","");
        }
        Assert.assertEquals("+",functionInvoker,"[\"(3)A\",\"B\",\"C\"]");

        functionInvoker = "[\"A\",\"B\",\"C\"]";
        if (functionInvoker.matches("\\(([0-9]+)\\)\\[.*]")){
            functionInvoker = functionInvoker.replaceFirst("\\(([0-9]+)\\)","");
        }
        Assert.assertEquals("+",functionInvoker,"[\"A\",\"B\",\"C\"]");

    }
}
