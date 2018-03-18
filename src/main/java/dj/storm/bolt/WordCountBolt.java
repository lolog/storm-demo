package dj.storm.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * BaseRickBolt是IComponent和IBolt接口 的一个简单实现。<br>
 * 通常,需要避免将信息存储在bolt中,因为bolt执行异常或者重新指派时,数据会丢失。一种解决方法是：定期对存储的信息快照放在持久性存储中,<br>
 * 这样,如果task被重新指派就可以恢复数据。
 * @author adolf.felix <b>jonguo@net.yeah</b>
 */
public class WordCountBolt extends BaseRichBolt {
	private static final long serialVersionUID = -2801738245452044818L;
	
	private OutputCollector collector;
	private Map<String, Integer> counts;
	
	/**
	 * 大部分实例变量通常是在prepare方法中继续实例化的,这个设计模式是有topology的部署方式决定的。<br>
	 * 当topology发布时,所有的bolt和spout组件首先会进行序列化,然后通过网络发送到集群中。<br>
	 * 如果spout和bolt在序列化之前(比如在构造函数中生成)实例化了任何无法序列化的实例变量,在进行序列化时会抛出NotSerializableException异常,topology将会部署失败.<br>
	 * 通常情况下,最好在构造函数中对基本数据类型和可序列化的对象进行复制和实例化,在prepare方法中对不可序列化的对象进行实例化。
	 */
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.counts = new HashMap<String, Integer>();
	}
	
	@Override
	public void execute(Tuple input) {
		String word = input.getStringByField("split-word");
		Integer count = counts.getOrDefault(word, 0);
		count++;
		collector.emit(new Values(word, count));
		counts.put(word, count);
		
		// 对spout进行响应确认
		this.collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}
}
