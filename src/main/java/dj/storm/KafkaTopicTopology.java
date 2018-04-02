package dj.storm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Tuple;

import dj.storm.utils.FileStream;

public class KafkaTopicTopology {
	private static String HOST = "guojl";
	
	@SuppressWarnings("rawtypes")
	private static class KafkaBolt implements IRichBolt {
		private static final long serialVersionUID = 1244488432136721278L;
		
		private OutputCollector collector;
		private FileStream fileStream = new FileStream();
		private static AtomicInteger Index = new AtomicInteger(1);
		
		@Override
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
			this.collector = collector;
			fileStream.initalOutput("kafka-bolt-" + Index.getAndIncrement() + ".out");
		}

		@Override
		public void execute(Tuple input) {
			fileStream.write(input.getValue(0));
			this.collector.ack(input);
		}

		@Override
		public void cleanup() {
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			return null;
		}
	}
	
	public static void main(String[] args) {
		List<String> zkServers = new ArrayList<String>();
		zkServers.add(HOST);
		
		ZkHosts zkHosts = new ZkHosts(HOST + ":2181");
		// 如何确定SpoutConfig中的zkRoot,查看kafka中的server.properties文件,
		// 如果zookeeper.connect后面没有跟/path-directory,那么为"";
		// 否则zkRoot为/path-directory,zookeeper.connect=host1:2181,host2:2181/path-directory
		SpoutConfig spoutConfig = new SpoutConfig(zkHosts, "test", "storm-kafka-client", "", "kafka-topic");
					spoutConfig.zkServers = zkServers;
					spoutConfig.zkPort = 2181;
					spoutConfig.socketTimeoutMs = 60 * 1000;
					spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new KafkaSpout(spoutConfig), 1);
		builder.setBolt("bolt", new KafkaBolt(), 1).shuffleGrouping("spout");

		Config stormConfig = new Config();
		stormConfig.setDebug(false);
		
		LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("kafka-topic-topology", stormConfig, builder.createTopology());
	}
}
