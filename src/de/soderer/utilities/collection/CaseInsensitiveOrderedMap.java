package de.soderer.utilities.collection;

import java.util.Map;

/**
 * Generic String keyed Map that ignores the String case
 *
 * @author Andreas
 *
 * @param <V>
 */
public class CaseInsensitiveOrderedMap<V> extends AbstractLinkedHashMap<String, V> {
	private static final long serialVersionUID = 7467218114617744333L;

	public static <V> CaseInsensitiveOrderedMap<V> create() {
		return new CaseInsensitiveOrderedMap<V>();
	}

	public CaseInsensitiveOrderedMap() {
		super();
	}

	public CaseInsensitiveOrderedMap(int initialCapacity, float loadFactor, boolean accessOrder) {
		super(initialCapacity, loadFactor, accessOrder);
	}

	public CaseInsensitiveOrderedMap(int initialCapacity, float loadFactor) {
		super(initialCapacity, loadFactor);
	}

	public CaseInsensitiveOrderedMap(int initialCapacity) {
		super(initialCapacity);
	}

	public CaseInsensitiveOrderedMap(Map<? extends String, ? extends V> map) {
		super(map.size());
		putAll(map);
	}

	@Override
	protected String convertKey(Object key) {
		return key == null ? null : key.toString().toLowerCase();
	}
}
