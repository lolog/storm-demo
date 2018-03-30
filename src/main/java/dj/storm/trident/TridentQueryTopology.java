package dj.storm.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.TupleCollectionGet;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import dj.storm.trident.function.SplitFunction;
import dj.storm.utils.FileStream;

@SuppressWarnings("unchecked")
public class TridentQueryTopology {
	private static FixedBatchSpout spout = new FixedBatchSpout(
			new Fields("sentence"),  
            3, 
            new Values("the cow jumped over the moon"),
            new Values("the man went to the store and bought some candy"),
            new Values("four score and seven years ago"),
            new Values("how many apples can you eat")
		);
	private static FileStream fileStream = new FileStream(); 
	static {
		fileStream.initalOutput("trident-query.out");
	}
	
	static class PrintFunction extends BaseFunction {
		private static final long serialVersionUID = 1668902196018212156L;
		
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String word = tuple.getStringByField("word");
			String count = tuple.getStringByField("count");
			
			fileStream.write("{word ="+ word + ", count="+count+"}");
			
			collector.emit(new Values(word, count));
		}
	}
	
	public static void main(String[] args) {
		spout.setCycle(false);
		
		Config stormConfig = new Config();
		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		TridentTopology topology = new TridentTopology();
		
		TridentState state = topology.newStream("spout", spout)
								.parallelismHint(6)
								.each(new Fields("sentence"), new SplitFunction(), new Fields("word"))
								.groupBy(new Fields("word"))
								.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
								.parallelismHint(6);
		
		topology.newDRPCStream("print", drpc)
				.stateQuery(state, new TupleCollectionGet(), new Fields("word", "count"))
				.each(new Fields("word", "count"), new PrintFunction(), new Fields("wrd", "sum"));
		
		cluster.submitTopology(TridentQueryTopology.class.getSimpleName(), stormConfig, topology.build());
		fileStream.write("=========================" + drpc.execute("print", ""));
	}
}
