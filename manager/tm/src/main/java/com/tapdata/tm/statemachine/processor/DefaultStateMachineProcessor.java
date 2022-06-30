/**
 * @title: DefaultStateMachineProcessor
 * @description:
 * @author lk
 * @date 2021/8/5
 */
package com.tapdata.tm.statemachine.processor;

import com.tapdata.tm.statemachine.model.StateContext;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class DefaultStateMachineProcessor extends AbstractStateMachineProcessor {


	@Override
	public StateMachineResult execute(StateContext context) {
		return StateMachineResult.ok();
	}
}
