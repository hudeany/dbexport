package de.soderer.utilities;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A thread-safe LRU cache implementation based on internal LinkedHashMap.
 *
 * @param <K>
 *            Entry Key Type
 * @param <V>
 *            Entry Value Type
 */
public class LRUCache<K, V> {
	public static final int DEFAULT_MAX_SIZE = 1000;

	private final Map<K, V> internalMap;

	public LRUCache() {
		this(DEFAULT_MAX_SIZE);
	}

	public LRUCache(final int maxSize) {
		internalMap = Collections.synchronizedMap(new LinkedHashMap<K, V>(maxSize + 1, .75F, true) {
			private static final long serialVersionUID = 6658309225064456992L;

			@Override
			protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
				return size() > maxSize;
			}
		});
	}

	public V put(K key, V value) {
		return internalMap.put(key, value);
	}

	public V get(K key) {
		return internalMap.get(key);
	}
}