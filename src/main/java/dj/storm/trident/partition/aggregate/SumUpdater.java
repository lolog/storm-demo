package dj.storm.trident.partition.aggregate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import dj.storm.utils.FileStream;

public class SumUpdater extends BaseStateUpdater<SumState>{
	private FileStream fileStream = new FileStream();
	
	private AtomicBoolean flag = new AtomicBoolean(false);
	private static AtomicInteger Index = new AtomicInteger(0);
	
	private static final long serialVersionUID = 7118390578208555140L;

	@Override
	public void updateState(SumState state, List<TridentTuple> tuples, TridentCollector collector) {
		if (flag.compareAndSet(false, true)) {
			fileStream.initalOutput("sum-updater-" + Index.getAndAdd(1) + ".out");
		}
		fileStream.write("++++ updateState(time=" + System.currentTimeMillis() + ", txid=" + state.getCurTxid() + ") ++++");
		fileStream.write("++++ updateState(tuples size=" + tuples.size() + ") ++++");
		Map<String,Integer> map=new HashMap<String,Integer>();
		for (TridentTuple tuple: tuples) {
			map.put(tuple.getString(0), tuple.getInteger(1));
			fileStream.write(tuple.getString(0) + " = " + tuple.getInteger(1));
		}
		fileStream.write(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
		state.setIsUpdate(true);
	}

}
