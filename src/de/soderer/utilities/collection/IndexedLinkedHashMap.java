package de.soderer.utilities.collection;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class IndexedLinkedHashMap<K, V> extends LinkedHashMap<K, V> {
	private static final long serialVersionUID = -1676269301623131177L;

	private List<K> keyList = null;

	@Override
	public V remove(Object key) {
		keyList = null;
		return super.remove(key);
	}

	@Override
	public V put(K key, V value) {
		keyList = null;
		return super.put(key, value);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> map) {
		keyList = null;
		super.putAll(map);
	}

	public List<K> getKeyList() {
		if (keyList == null) {
			keyList = new ArrayList<K>(keySet());
		}

		return keyList;
	}

	public K getKey(int i) {
		return getKeyList().get(i);
	}

	public V getValue(int i) {
		return get(getKey(i));
	}
}
