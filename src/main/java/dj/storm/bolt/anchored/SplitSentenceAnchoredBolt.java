package dj.storm.bolt.anchored;

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
 * Spout/Bolt不同的任务数,Spout/Bolt具有不同的实例。
 * @author adolf.felix <b>jonguo@net.yeah</b>
 */	
public class SplitSentenceAnchoredBolt extends BaseRichBolt {
	private static final long serialVersionUID = 911627389791344439L;
	
	private OutputCollector collector;
	
	/**
	 * prepare方法在IBolt中定义的,雷同ISpout接口中定义的open方法。<br>
	 * 这个方法在bolt初始化时调用,可以用于准备bolt用到的资源,如数据库连接。<br>
	 * @param stormConf Storm拓扑的配置信息
	 * @param context 提供了topology中的组件信息,可用于获取此任务在拓扑中的位置的信息,包括此任务的任务ID和组件ID,输入和输出信息等。
	 * @param collector 提供了发射/打开/关闭tuple的方法。 collector是线程安全的，应该保存为此Bolt对象的实例变量。
	 */
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}
	
	/**
	 * 每当从订阅的数据流中接收一个tuple,都会调用该方法。<br>
	 * Tuple对象包含关于它来自哪个组件/流/任务的元数据,可以使用Tuple＃getValue来访问。<br>
	 * IBolt不必立即处理Tuple,挂在元组上并稍后处理它是完全正确的,例如:执行聚合或连接。<br> 
	 * 需要使用OutputCollector#ack或fail，确认tuple是否响应成功/失败。
	 * @param tuple 要处理的输入元组
	 */
	@Override
	public void execute(Tuple input) {
		String sentence = input.getStringByField("sentence");
		String[] words = sentence.split(" ");
		for (String word: words) {
			/**
			 * 对处理消息成功/失败时,分别确认应答或者报错
			 * 锚定tuple: 简历读入tuple和衍生的tuple之间的对应关系,这样下游的Bolt就可以通过应答确认/报错/超时来加入到tuple树结构中。
			 * 下面将input和发射的tuple锚定起来,下游的Bolt就需要对输出的tuple进行确认应答或者报错。下游Bolt可以通过OutputCollector#ack/fail应答。
			 */
			this.collector.emit(input, new Values(word));
		}
		// 对spout进行响应确认
		this.collector.ack(input);
	}
	
	/**
	 * declareOutputFields方法是在IComponent接口中定义的,所有Storm组件(spout和bolt)都必须实现该方法。<br>
	 * 通过declareOutputFields方法告诉Storm该组件会发射哪些数据流,每个数据流的tuple包含哪些字段。
	 * @param declarer 声明输出流ID,输出字段以及每个输出流是否是直接流
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("split-word"));
	}

}
