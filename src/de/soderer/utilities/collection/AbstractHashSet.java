package de.soderer.utilities.collection;

import java.util.Collection;
import java.util.HashSet;

public abstract class AbstractHashSet<V> extends HashSet<V> {
	private static final long serialVersionUID = -8774751629113337123L;

	public AbstractHashSet() {
		super();
	}

	public AbstractHashSet(int initialCapacity, float loadFactor) {
		super(initialCapacity, loadFactor);
	}

	public AbstractHashSet(int initialCapacity) {
		super(initialCapacity);
	}

	public AbstractHashSet(Collection<? extends V> collection) {
		super(collection.size());
		addAll(collection);
	}

	@Override
	public boolean contains(Object item) {
		return super.contains(convertItem(item));
	}

	@Override
	public boolean add(V item) {
		return super.add(convertItem(item));
	}

	@Override
	public boolean addAll(Collection<? extends V> collection) {
		boolean result = false;
		for (V item : collection) {
			if (add(item)) {
				result = true;
			}
		}
		return result;
	}

	@Override
	public boolean remove(Object item) {
		return super.remove(convertItem(item));
	}

	protected abstract V convertItem(Object item);
}
