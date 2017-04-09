package de.soderer.utilities.collection;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractHashMap<K, V> extends HashMap<K, V> {
	private static final long serialVersionUID = 868647429993685054L;

	public AbstractHashMap() {
		super();
	}

	public AbstractHashMap(int initialCapacity, float loadFactor) {
		super(initialCapacity, loadFactor);
	}

	public AbstractHashMap(int initialCapacity) {
		super(initialCapacity);
	}

	public AbstractHashMap(Map<? extends K, ? extends V> map) {
		super(map.size());
		putAll(map);
	}

	@Override
	public boolean containsKey(Object key) {
		return super.containsKey(convertKey(key));
	}

	@Override
	public V get(Object key) {
		return super.get(convertKey(key));
	}

	@Override
	public V put(K key, V value) {
		return super.put(convertKey(key), value);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> map) {
		for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}
	}

	@Override
	public V remove(Object key) {
		return super.remove(convertKey(key));
	}

	protected abstract K convertKey(Object key);
}
