/**
 * @title: JobStatusService
 * @description:
 * @author lk
 * @date 2021/7/19
 */
package com.tapdata.job;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class JobStatusService {

	@Autowired
	private JobStatusConverter jobStatusConverter;

	public void xxx(String id, String target) throws Exception {
		jobStatusConverter.handle(id, target);
	}
}
