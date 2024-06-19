package io.tapdata.inspect;

import com.tapdata.entity.inspect.Inspect;
import com.tapdata.entity.inspect.InspectResult;
import io.tapdata.exception.TapOssNonsupportFunctionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class InspectServiceImplTest {
   @Test
    void testHandleDiffInspect(){
       InspectServiceImpl inspectService = new InspectServiceImpl();
       try {
           inspectService.handleDiffInspect(new Inspect(), 10,new InspectResult());
       }catch (Exception e) {
           Assertions.assertTrue(e instanceof TapOssNonsupportFunctionException);
       }
   }


}
