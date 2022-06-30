package com.tapdata.tm.commons.schema;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
public class PdkSupportDDL {
    private List<String> events;
    private boolean disableDDLSync;
}
