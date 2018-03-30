package dj.storm.trident.partition.aggregate;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class CountAggregate implements Aggregator<Map<String, Integer>>{
	private static final long serialVersionUID = 5279088463609893133L;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TridentOperationContext context) {
	}

	@Override
	public void cleanup() {
	}
	
	/**
	 * 在处理batch之前被调用。init的返回值是一个表示聚合状态的对象,该对象会被传递到aggregate和complete方法。
	 * @param batchId 批次Id
	 * @param collector 发射器
	 */
	@Override
	public Map<String, Integer> init(Object batchId, TridentCollector collector) {
		return new HashMap<String, Integer> ();
	}
	
	/**
	 * 为每个在batch分区的输入元组所调用,更新状态 
	 * @param val 方法init的返回值
	 * @param tuple tuple元组
	 * @param collector 发射器
	 */
	@Override
	public void aggregate(Map<String, Integer> val, TridentTuple tuple, TridentCollector collector) {
		String word = tuple.getString(0);
		val.put(word, val.getOrDefault(word, 0) + 1);
	}
	
	
	@Override
	public void complete(Map<String, Integer> val, TridentCollector collector) {
		for (Entry<String, Integer> entry: val.entrySet()) {
			collector.emit(new Values(entry.getKey(), entry.getValue()));
		}
		val.clear();
	}

}
