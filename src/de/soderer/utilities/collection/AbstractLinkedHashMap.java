package de.soderer.utilities.collection;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

public abstract class AbstractLinkedHashMap<K, V> extends LinkedHashMap<K, V> {
	private static final long serialVersionUID = -7227298515931195921L;

	public AbstractLinkedHashMap() {
		super();
	}

	public AbstractLinkedHashMap(int initialCapacity, float loadFactor, boolean accessOrder) {
		super(initialCapacity, loadFactor, accessOrder);
	}

	public AbstractLinkedHashMap(int initialCapacity, float loadFactor) {
		super(initialCapacity, loadFactor);
	}

	public AbstractLinkedHashMap(int initialCapacity) {
		super(initialCapacity);
	}

	public AbstractLinkedHashMap(Map<? extends K, ? extends V> map) {
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
