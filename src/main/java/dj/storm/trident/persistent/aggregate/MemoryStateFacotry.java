package dj.storm.trident.persistent.aggregate;

import java.util.Map;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.state.TransactionalValue;
import org.apache.storm.trident.state.map.NonTransactionalMap;
import org.apache.storm.trident.state.map.OpaqueMap;
import org.apache.storm.trident.state.map.TransactionalMap;

/**
 * <pre>
 *   OpaqueMap's会用OpaqueValue的value来调用multiPut方法,
 *   TransactionalMap's会提供TransactionalValue中的value,
 *   而NonTransactionalMaps只是简单的把从Topology获取的object传递给multiPut。
 * </pre>
 * @author Adolf.Felix
 */
@SuppressWarnings("rawtypes") 
public class MemoryStateFacotry implements StateFactory {
	private static final long serialVersionUID = 7735682421536211614L;
	
	private StateType stateType;
	
	public MemoryStateFacotry(StateType stateType) {
		this.stateType = stateType;
	}
	
	@Override
	public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
		if (this.stateType == StateType.TRANSACTIONAL) {
			return TransactionalMap.build(new MemoryStateIBackingMap<TransactionalValue>());
		}
		else if (this.stateType == StateType.OPAQUE) {
			return OpaqueMap.build(new MemoryStateIBackingMap<OpaqueValue>());
		}
		else if (this.stateType == StateType.NON_TRANSACTIONAL) {
			return NonTransactionalMap.build(new MemoryStateIBackingMap<Object>());
		}
		else {
			return null;
		}
	}

}
