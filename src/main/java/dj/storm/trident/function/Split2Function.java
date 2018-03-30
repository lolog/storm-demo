package dj.storm.trident.function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class Split2Function extends BaseFunction {
	private static final long serialVersionUID = 1668902196018212156L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String sentence = tuple.getStringByField("sentence");
		String[] words = sentence.split(" ");
		for (Integer index=0; index<words.length; index++) {
			collector.emit(new Values(words[index], index));
		}
	}
}