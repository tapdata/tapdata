package com.tapdata.tm.commons.schema;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
public class PdkConfigOptions {
    private List<String> capabilities;
    private PdkSupportDDL supportDDL;
}
