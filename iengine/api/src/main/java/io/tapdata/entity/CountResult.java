package io.tapdata.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CountResult {
    private Long count;
    private Boolean done;
}
