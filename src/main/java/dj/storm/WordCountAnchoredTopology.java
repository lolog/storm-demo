package dj.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import dj.storm.bolt.ReportBolt;
import dj.storm.bolt.anchored.SplitSentenceAnchoredBolt;
import dj.storm.bolt.anchored.WordCountAnchoredBolt;
import dj.storm.spout.SentenceSpout;

/**
 * 备注: Spout/Bolt不同的任务数,Spout/Bolt具有不同的实例,即不同的executor运行不同的Spout和Bolt。<br>
 * 通常,需要避免将信息存储在bolt中,因为bolt执行异常或者重新指派时,数据会丢失。一种解决方法是：定期对存储的信息快照放在持久性存储中,<br>
 * 这样,如果task被重新指派就可以恢复数据。
 */
public class WordCountAnchoredTopology 
{
    public static void main( String[] args) throws Exception {
    	String outFileName = "word-count-anchored-topology.out";
    	
        TopologyBuilder builder = new TopologyBuilder();
        /**
         * 指定Spout源数量=3
         */
        builder.setSpout("spout", new SentenceSpout(), 3);
        /**
         * SentenceSpout的ID赋值给shuffleGrouping方法确立订阅关系,
         * shuffleGrouping方法告诉Storm,要将类SentenceSpout发射的tuple随机均匀
         * 的发射给SplitSentenceBolt的实例
         */
        builder.setBolt("split", new SplitSentenceAnchoredBolt(), 6).shuffleGrouping("spout");
        /**
         * 有时候需要将含有特定数据的tuple路由到特殊的bolt实例中。由此我们使用BoltDeclarer的fieldsGrouping方法
         * 来保证所有的word字段值相同的tuple会被路由到同一个WordCountBolt实例中.
         */
        builder.setBolt("count", new WordCountAnchoredBolt(), 3).fieldsGrouping("split", new Fields("split-word"));
        /**
         * globalGrouping可以保证所有的tuple路由到唯一的ReportBolt任务中
         */
        builder.setBolt("report", new ReportBolt(outFileName), 1).globalGrouping("count");
        
        /**
         * Storm的Confif类是一个HashMap<String,Object>的子类,并定义了一些Storm特有的常量和简便的方法,用来配置topology运行时的行为。
         * 当一个topology提交时,Storm会将默认配置和Config实例中的配置合并后作为参数传递给submitTopology()方法。
         * 合并后的配置被分发给各个spout的open、bolt的prepare分发。
         */
        Config stormConf = new Config();
        stormConf.setDebug(false);
        // 指定topology的worker数量
        stormConf.setNumWorkers(2);
        stormConf.setMaxTaskParallelism(3);
        
        LocalCluster cluster = new LocalCluster();  	
        cluster.submitTopology(WordCountAnchoredTopology.class.getSimpleName(), stormConf, builder.createTopology());
        
        Thread.sleep(20000);
        
        cluster.killTopology(WordCountAnchoredTopology.class.getSimpleName());
        cluster.shutdown();
    }
}
