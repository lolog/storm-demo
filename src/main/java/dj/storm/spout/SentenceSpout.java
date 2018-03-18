package dj.storm.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * BaseRichSpout是ISpout和IComponent的简单实现。<br>
 * Spout/Bolt不同的任务数,Spout/Bolt具有不同的实例。
 * @author adolf.felix <b>jonguo@net.yeah</b>
 */
public class SentenceSpout extends BaseRichSpout {
	private static final long serialVersionUID = 5807159029267461791L;
	
	private int counts = 1;
	private AtomicInteger index = new AtomicInteger(1);
	
	private SpoutOutputCollector collecter;
	
	private static List<Object> outputs = new ArrayList<Object>() {
		private static final long serialVersionUID = 1653360095410484559L;
		{
			add("the dog has fleas");
		}
	};
	
	public SentenceSpout() {
	}
	
	public SentenceSpout(List<Object> params) {
		outputs = params;
	}
	
	public SentenceSpout(List<Object> params, int size) {
		counts = size;
		outputs = params;
	}
	
	/**
	 * open是在ISpout接口中定义的,所有Spout组件在初始化时都调用这个方法。
	 * @param conf Storm拓扑的配置信息
	 * @param context 提供了topology中的组件信息,可用于获取此任务在拓扑中的位置的信息,包括此任务的任务ID和组件ID,输入和输出信息等。
	 * @param collector 提供了发射/打开/关闭tuple的方法。 collector是线程安全的，应该保存为此Bolt对象的实例变量。
	 */
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collecter = collector;
	}

	/**
	 * nextTuple方法是Spout实现的核心,Storm通过调用这个方法向SpoutOutputCollector发射tuple。<br>
	 * 这个方法应该是非阻塞的,所以如果Spout没有tuple发射时,这个方法应该有返回。<br>
	 * nextTuple/ack/fail都在Spout任务的单个线程中紧密循环中调用。<br>
	 * 当没有tuple发射时,为了不浪费太多的CPU，在下一次发射之前睡眠双倍的时间（如一毫秒）。
	 */
	@Override
	public void nextTuple() {
		if(index.getAndAdd(1) > counts) {
			return;
		}
		for(Object value: outputs) {
			/**
			 * 为实现可靠的消息处理,首先要给每个发出的tuple带上唯一的ID,并且将ID作为参数传递给SpoutOutputCollector的emit方法。<br>
			 * 给tuple指定的ID告诉Storm系统,无论执行成功与否,spout都要接受到tuple树上所有节点返回的通知。<br>
			 * 如果成功,spout的ack方法将会对编号是ID的消息应答确认,如果失败或者超时,会调用fail方法
			 */
			collecter.emit(new Values(value), outputs.indexOf(value));
		}
	}
	
	@Override
	public void fail(Object msgId) {
		int id = Integer.parseInt(msgId.toString());
		collecter.emit(new Values(outputs.get(id)), id);
	}
	
	/**
	 * declareOutputFields方法是在IComponent接口中定义的,所有Storm组件(spout和bolt)都必须实现该方法。<br>
	 * 通过declareOutputFields方法告诉Storm该组件会发射哪些数据流,每个数据流的tuple包含哪些字段。
	 * @param declarer 声明输出流ID,输出字段以及每个输出流是否是直接流
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}
	
}
