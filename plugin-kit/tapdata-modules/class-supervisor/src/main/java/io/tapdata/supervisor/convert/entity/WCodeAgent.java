package io.tapdata.supervisor.convert.entity;

import javassist.CannotCompileException;
import javassist.NotFoundException;

public interface WCodeAgent<Agent> {
    public void agent(Agent agentTarget) throws CannotCompileException, NotFoundException;
}
