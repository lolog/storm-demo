package dj.storm.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import dj.storm.trident.function.Split2Function;
import dj.storm.trident.partition.aggregate.CountAggregate;
import dj.storm.trident.partition.aggregate.SumStateFactory;
import dj.storm.trident.partition.aggregate.SumUpdater;

/**
 * partitionAggregate聚合方法,以及partitionPersist持久化
 * @author Adolf.Felix
 */
@SuppressWarnings("unchecked")
public class PartitionAggregatorTopology {
	private static FixedBatchSpout spout = new FixedBatchSpout(new Fields(
			"sentence"), 3, new Values("the cow jumped over the moon"),
			new Values("the man went to the store and bought some candy"),
			new Values("four score and seven years ago"), new Values(
					"how many apples can you eat"));

	public static void main(String[] args) {
		TridentTopology topology = new TridentTopology();
		spout.setCycle(false);

		Stream stream = topology
				.newStream("spout", spout)
				.shuffle()
				.parallelismHint(3)
				.each(new Fields("sentence"), new Split2Function(), new Fields("word", "index")).parallelismHint(3)
				.project(new Fields("word")).parallelismHint(3)
				.partitionBy(new Fields("word")); // 根据word的值作分区
		
		// partitionAggregate：对每个Partition中的tuple进行聚合,partitionAggregate的输出会直接替换掉输入的tuple,即只有其发射出去的tuple。
		stream.partitionAggregate(new Fields("word"), new CountAggregate(), new Fields("sumKey", "sumValue"))
			  // 对每个Partition的Batch解析持久化
			  .partitionPersist(new SumStateFactory(), new Fields("sumKey", "sumValue"), new SumUpdater());

		Config stormConfig = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("partition-aggregator", stormConfig, topology.build());

		Utils.sleep(15000);
		cluster.shutdown();
	}
}
