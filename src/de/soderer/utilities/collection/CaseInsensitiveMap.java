package de.soderer.utilities.collection;

import java.util.Map;

/**
 * Generic String keyed Map that ignores the String case
 *
 * @author Andreas
 *
 * @param <V>
 */
public class CaseInsensitiveMap<V> extends AbstractHashMap<String, V> {
	private static final long serialVersionUID = -528027610172636779L;

	public static <V> CaseInsensitiveMap<V> create() {
		return new CaseInsensitiveMap<V>();
	}

	public CaseInsensitiveMap() {
		super();
	}

	public CaseInsensitiveMap(int initialCapacity, float loadFactor) {
		super(initialCapacity, loadFactor);
	}

	public CaseInsensitiveMap(int initialCapacity) {
		super(initialCapacity);
	}

	public CaseInsensitiveMap(Map<? extends String, ? extends V> map) {
		super(map.size());
		putAll(map);
	}

	@Override
	protected String convertKey(Object key) {
		return key == null ? null : key.toString().toLowerCase();
	}
}
