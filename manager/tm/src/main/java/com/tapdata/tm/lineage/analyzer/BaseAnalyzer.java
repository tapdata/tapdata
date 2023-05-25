package com.tapdata.tm.lineage.analyzer;

import com.tapdata.tm.ds.repository.DataSourceRepository;
import com.tapdata.tm.modules.repository.ModulesRepository;
import com.tapdata.tm.task.repository.TaskRepository;
import org.springframework.stereotype.Service;

/**
 * @author samuel
 * @Description
 * @create 2023-05-22 15:51
 **/
@Service
public abstract class BaseAnalyzer implements AnalyzerService {
	protected final TaskRepository taskRepository;
	protected final DataSourceRepository dataSourceRepository;
	protected final ModulesRepository modulesRepository;

	public BaseAnalyzer(TaskRepository taskRepository, DataSourceRepository dataSourceRepository, ModulesRepository modulesRepository) {
		this.taskRepository = taskRepository;
		this.dataSourceRepository = dataSourceRepository;
		this.modulesRepository = modulesRepository;
	}
}
