package dj.storm.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import dj.storm.utils.FileStream;

/**
 * 通常,需要避免将信息存储在bolt中,因为bolt执行异常或者重新指派时,数据会丢失。一种解决方法是：定期对存储的信息快照放在持久性存储中,<br>
 * 这样,如果task被重新指派就可以恢复数据。
 * @author adolf.felix <b>jonguo@yeah.net</b>
 */
public class ReportBolt extends BaseRichBolt {
	private static final long serialVersionUID = -1884386794721577194L;

	private String outFile;
	private Map<String, Integer> counts;
	
	private OutputCollector collector;
	
	public ReportBolt(String outFile) {
		this.outFile = outFile;
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.counts = new HashMap<String, Integer>();
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		Integer count = input.getIntegerByField("count");
		if (count > counts.getOrDefault(word, 0)) {
			counts.put(word, count);
		}
		// 对spout进行响应确认
		this.collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	/**
	 * 当IBolt将要关闭时调用。如果采用-9杀死集群上工作进程，则不被调用。<br>
	 * topology#shutdown时输出最终结果
	 */
	@Override
	public void cleanup() {
		if (outFile == null) {
			
		}
		else {
			FileStream fileStream = new FileStream();
			fileStream.initalOutput(outFile);
			for (Entry<String, Integer> count: counts.entrySet()) {
				fileStream.write(count.getKey() + "=" + count.getValue());
			}
			fileStream.close();
		}
	}
}
