package io.tapdata.inspect;

import com.tapdata.entity.inspect.InspectDetail;
import com.tapdata.entity.inspect.InspectResultStats;
import com.tapdata.entity.inspect.InspectTask;

import java.util.List;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/20 2:52 下午
 * @description
 */
@FunctionalInterface
public interface ProgressUpdate {

	void progress(InspectTask inspectTask, InspectResultStats inspectResultStats, List<InspectDetail> inspectDetails);

}
