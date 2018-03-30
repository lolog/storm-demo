package dj.storm.spout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class SentenceBatchSpout implements IBatchSpout {
	private static final long serialVersionUID = -3030510417951584017L;
	
	private Map<Long, List<Values>> caches = new HashMap<Long, List<Values>> ();
	
	private List<Values> outputs = new ArrayList<Values> () {
		private static final long serialVersionUID = 3956956945569518234L;
		{
			add(new Values("org apache storm tuple values"));
			add(new Values("org apache storm tuple fields"));
		}
	};
	
	private int maxBatch;
	private AtomicInteger index;
	
	private Fields fields = new Fields("sentence");
	
	public SentenceBatchSpout() {
		
	}
	
	public SentenceBatchSpout(Fields fields, Integer maxBatch, List<Values> outputs) {
		this.fields = fields;
		this.outputs = outputs;
		this.maxBatch = maxBatch;
	}
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {
		if (this.maxBatch < 1) {
			this.maxBatch = 10;
			this.index = new AtomicInteger(1);
		}
	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		if (index.getAndIncrement() < this.maxBatch) {
			List<Values> cache = this.caches.getOrDefault(batchId, outputs);
			caches.put(batchId, cache);
			for (Values value: cache) {
				collector.emit(value);
			}
		}
	}

	@Override
	public void ack(long batchId) {
		this.caches.remove(batchId);
	}

	@Override
	public void close() {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
	}

	@Override
	public Fields getOutputFields() {
		return this.fields;
	}

}
