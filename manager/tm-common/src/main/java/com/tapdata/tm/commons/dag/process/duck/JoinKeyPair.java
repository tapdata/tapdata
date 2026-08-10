package com.tapdata.tm.commons.dag.process.duck;

import lombok.Data;

@Data
public class JoinKeyPair {
    private JoinField left;
    private JoinField right;
}