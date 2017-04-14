package de.soderer.utilities.collection;

import java.util.Collection;

/**
 * Generic String Set that ignores the String case
 */
public class CaseInsensitiveSet extends AbstractHashSet<String> {
	private static final long serialVersionUID = 9146520978777587374L;

	public CaseInsensitiveSet() {
		super();
	}

	public CaseInsensitiveSet(int initialCapacity, float loadFactor) {
		super(initialCapacity, loadFactor);
	}

	public CaseInsensitiveSet(int initialCapacity) {
		super(initialCapacity);
	}

	public CaseInsensitiveSet(Collection<? extends String> collection) {
		super(collection);
		addAll(collection);
	}

	@Override
	protected String convertItem(Object item) {
		return item == null ? null : item.toString().toLowerCase();
	}
}
