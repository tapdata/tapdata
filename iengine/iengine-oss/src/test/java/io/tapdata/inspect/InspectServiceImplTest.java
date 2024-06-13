package io.tapdata.inspect;

import com.tapdata.entity.inspect.Inspect;
import io.tapdata.exception.TapOssNonsupportFunctionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class InspectServiceImplTest {
   @Test
    void testHandleDiffInspect(){
       InspectServiceImpl inspectService = new InspectServiceImpl();
       try {
           inspectService.handleDiffInspect(new Inspect(), 10);
       }catch (Exception e) {
           Assertions.assertTrue(e instanceof TapOssNonsupportFunctionException);
       }
   }


}
