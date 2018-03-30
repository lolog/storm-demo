package dj.storm.trident.persistent.aggregate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.shade.com.google.common.collect.Maps;
import org.apache.storm.trident.state.map.IBackingMap;

/**
 * <pre>
 *   在Trident中实现MapState是非常简单的,它几乎帮你做了所有的事情。
 *   OpaqueMap,TransactionalMap和 NonTransactionalMap类实现了所有相关的逻辑,包括容错的逻辑。
 *   只需要将一个IBackingMap的实现提供给这些类就可以了。
 *   
 *   OpaqueMap's会用OpaqueValue的value来调用multiPut方法,
 *   TransactionalMap's会提供TransactionalValue中的value,
 *   而NonTransactionalMaps只是简单的把从Topology获取的object传递给multiPut。
 *   
 *   Trident还提供了一种CachedMap类来进行自动的LRU cache。
 *   另外,Trident提供了 SnapshottableMap类将一个MapState 转换成一个 Snapshottable 对象。
 * </pre>
 * @author Adolf.Felix <b>jonguo@yeah.net</b>
 */
@SuppressWarnings("unchecked")
public class MemoryStateIBackingMap<T> implements IBackingMap<T> {
	private static Map<List<Object>, Object> caches = Maps.newHashMap();

	@Override
	public List<T> multiGet(List<List<Object>> keys) {
		List<T> ret = new ArrayList<T>();
		for (List<Object> key : keys) {
			T val = caches.getOrDefault(key, null) == null ? null : (T) caches.getOrDefault(key, null);
			ret.add(val);
		}
		return ret;
	}

	@Override
	public void multiPut(List<List<Object>> keys, List<T> vals) {
		for (int i = 0; i < keys.size(); i++) {
			List<Object> key = keys.get(i);
			T val = vals.get(i);
			caches.put(key, val);
		}
	}
}
