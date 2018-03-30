package dj.storm.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import dj.storm.trident.function.SplitFunction;
import dj.storm.trident.persistent.aggregate.MemoryStateFacotry;
import dj.storm.utils.FileStream;

@SuppressWarnings("unchecked")
public class PersistentAggregateTopology {
	private static FixedBatchSpout spout = new FixedBatchSpout(
			  new Fields("sentence")
			, 3
			, new Values("the cow jumped over the moon")
			, new Values("the man went to the store and bought some candy")
			, new Values("four score and seven years ago")
			, new Values("how many apples can you eat")
		);

	public static void main(String[] args) {
		FileStream fileStream = new FileStream();
		fileStream.initalOutput("persistent-aggregate.out");

		spout.setCycle(false);

		LocalDRPC drpc = new LocalDRPC();
		TridentTopology topology = new TridentTopology();

		TridentState wordCounts = topology
				.newStream("spout", spout)
				.each(new Fields("sentence"), new SplitFunction(), new Fields("word"))
				// group返回的Stream,要求Trident提供的State实现MapState接口。
				// 用来进行group的字段会以key的形式存在于State当中,聚合后的结果会以value的形式存储在State当中。
				.groupBy(new Fields("word"))
				// persistentAggregate是在partitionPersist之上的另外一层抽象。它知道怎么去使用一个Trident聚合器来更新State。
				.persistentAggregate(new MemoryStateFacotry(StateType.TRANSACTIONAL), new Count(), new Fields("count"));

		// 这个流程用于查询上面的统计结果
		topology.newDRPCStream("drpc", drpc)
				.each(new Fields("args"), new SplitFunction(), new Fields("word"))
				.groupBy(new Fields("word"))
				// MapGet方法batchRetrieve的ReadOnlyMapState对象类型为MemoryStateIBackingMap
				.stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
				.each(new Fields("count"), new FilterNull());

		Config sotrmConf = new Config();
		sotrmConf.setMaxSpoutPending(20);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("persistent-aggregate", sotrmConf, topology.build());
		
		// sleep等待执行wordCounts
		Utils.sleep(20000);
		fileStream.write(drpc.execute("drpc", "cat the dog jumped"));
	}
}
