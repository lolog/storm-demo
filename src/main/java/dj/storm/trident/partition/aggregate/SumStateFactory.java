package dj.storm.trident.partition.aggregate;

import java.util.Map;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;

@SuppressWarnings("rawtypes")
public class SumStateFactory implements StateFactory {
	private static final long serialVersionUID = 4720954237805491706L;
	
	@Override
	public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
		return new SumState();
	}

}
