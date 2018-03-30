package dj.storm.trident.function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class SplitFunction extends BaseFunction {
	private static final long serialVersionUID = 1668902196018212156L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String sentence = tuple.getStringByField("sentence");
		for (String word: sentence.split(" ")) {
			collector.emit(new Values(word));
		}
	}
}