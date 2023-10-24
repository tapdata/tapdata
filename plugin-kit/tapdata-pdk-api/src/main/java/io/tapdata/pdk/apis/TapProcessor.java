package io.tapdata.pdk.apis;

import io.tapdata.pdk.apis.functions.ProcessorFunctions;

public interface TapProcessor extends TapNode {
    void processorFunctions(ProcessorFunctions processorFunctions);
}
