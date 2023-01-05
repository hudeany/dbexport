package de.soderer.utilities.json;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JsonArray implements Iterable<Object> {
	private final List<Object> items = new ArrayList<>();

	public JsonArray add(final Object value) {
		items.add(value);
		return this;
	}

	public Object remove(final Object value) {
		return items.remove(value);
	}

	public Object get(final int index) {
		return items.get(index);
	}

	public int size() {
		return items.size();
	}

	@Override
	public Iterator<Object> iterator() {
		return items.iterator();
	}

	@Override
	public String toString() {
		try (ByteArrayOutputStream output = new ByteArrayOutputStream();
				JsonWriter writer = new JsonWriter(output, StandardCharsets.UTF_8);) {
			writer.add(this);
			writer.flush();
			return new String(output.toByteArray(), StandardCharsets.UTF_8);
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean equals(final Object otherObject) {
		if (this == otherObject) {
			return true;
		} else if (otherObject != null && otherObject instanceof JsonArray) {
			final JsonArray otherArray = (JsonArray) otherObject;
			if (size() != otherArray.size()) {
				return false;
			} else {
				for (int i = 0; i < size(); i++) {
					final Object thisValue = get(i);
					final Object otherValue = otherArray.get(i);
					if ((thisValue != otherValue)
							&& (thisValue != null && !thisValue.equals(otherValue))) {
						return false;
					}
				}
				return true;
			}
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((items == null) ? 0 : items.hashCode());
		return result;
	}
}
