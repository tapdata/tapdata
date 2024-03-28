package com.tapdata.tm.agent.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.worker.dto.WorkerDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * @author Gavin
 *
 * */
@EqualsAndHashCode(callSuper = true)
@Data
public class AgentGroupDto extends GroupDto {
    List<WorkerDto> agents;
}
