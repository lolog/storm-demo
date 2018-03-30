package dj.storm.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import dj.storm.utils.FileStream;

@SuppressWarnings("unchecked")
public class TridentStreamJoinTopology {
	private static FixedBatchSpout userInfoSpout = new FixedBatchSpout(
			new Fields("userId","name", "sexId","tel"),  
            3, 
            new Values("1", "Jack", "1", "18610712303"), 
            new Values("2", "Tome", "2", "15146971532"), 
            new Values("3", "Lay",  "2", "18674505647"), 
            new Values("4", "Lucy", "1", "13964780123")
		);
	private static FixedBatchSpout userSexSpout = new FixedBatchSpout(
    		new Fields("sexId","sex"),  
            3, 
            new Values("1", "Boy"),
            new Values("2", "Gril") 
    	);
	
	private static FileStream fileStream = new FileStream(); 
	static {
		fileStream.initalOutput("trident-stream-join.out");
	}
	
	public static class TelFilter extends BaseFilter {
		private static final long serialVersionUID = 4065625628130722535L;
		/**
		 * 对tuple进行过滤
		 * @param tuple 需要过滤的数据
		 * @return 返回true,tuple将发射到下游,反之不会发射到下游
		 */
		@Override
		public boolean isKeep(TridentTuple tuple) {
			String telphone = tuple.getStringByField("tel");
			return telphone.startsWith("186");
		}
	}
	public static class ExportFunction extends BaseFunction {
		private static final long serialVersionUID = -2669910637932740990L;
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String sexId = tuple.getStringByField("sexId");
			String userId = tuple.getStringByField("userId");
			String name = tuple.getStringByField("name");
			String tel = tuple.getStringByField("tel");
			String sex = tuple.getStringByField("sex");
			
			fileStream.write("{sexId ="+ sexId + ", userId="+userId+", name="+name+", tel="+tel+", sex="+sex+"}");
			collector.emit(new Values("1000"));
		}
	}
	
	public static void main(String[] args) {
		userInfoSpout.setCycle(false);
	    userSexSpout.setCycle(false);
	    
	    TridentTopology topology = new TridentTopology();
	    
	    Stream userInfoStream = topology.newStream("userInfoSpout", userInfoSpout)
	    						.each(new Fields("userId","name", "sexId","tel"),new TelFilter());
	    Stream userSexStream = topology.newStream("userSexSpout", userSexSpout);
	    
	    // 
	    topology.join(userInfoStream, new Fields("sexId"), userSexStream, new Fields("sexId"), new Fields("sexId", "userId", "name", "tel", "sex"))
	    		.each(new Fields("sexId", "userId", "name", "tel", "sex"), new ExportFunction(), new Fields("money"));
	    
	    Config stormConfig = new Config();  
			   stormConfig.setNumWorkers(2);  
			   stormConfig.setNumAckers(0);  
			   stormConfig.setDebug(false); 
        	  
       LocalCluster localCluster = new LocalCluster();  
       localCluster.submitTopology(TridentStreamJoinTopology.class.getSimpleName(), stormConfig, topology.build()); 
	}
}
