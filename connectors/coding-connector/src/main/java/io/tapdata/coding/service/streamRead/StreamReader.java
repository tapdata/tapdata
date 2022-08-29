package io.tapdata.coding.service.streamRead;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/***
 *
 *
 * 增量读线程
 */

public class StreamReader implements Callable<List<Map<String,Object>>> {

    public static StreamReader create(){

        return new StreamReader();
    }
    private StreamReader(){

    }
    @Override
    public List<Map<String, Object>> call() throws Exception {
        return null;
    }
}
