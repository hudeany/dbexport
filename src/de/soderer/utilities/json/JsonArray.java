package de.soderer.utilities.json;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import de.soderer.utilities.Utilities;

public class JsonArray implements Iterable<Object> {
	private List<Object> items = new ArrayList<Object>();

	public void add(Object value) {
		items.add(value);
	}

	public Object remove(Object value) {
		return items.remove(value);
	}

	public Object get(int index) {
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
		JsonWriter writer = null;
		ByteArrayOutputStream output = null;
		try {
			output = new ByteArrayOutputStream();
			writer = new JsonWriter(output, "UTF-8");
			writer.add(this);
			writer.close();
			return new String(output.toByteArray(), "UTF-8");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		} finally {
			Utilities.closeQuietly(output);
			Utilities.closeQuietly(writer);
		}
	}

	@Override
	public boolean equals(Object otherObject) {
		if (this == otherObject) {
			return true;
		} else if (otherObject != null && otherObject instanceof JsonArray) {
			JsonArray otherArray = (JsonArray) otherObject;
			if (this.size() != otherArray.size()) {
				return false;
			} else {
				for (int i = 0; i < this.size(); i++) {
					Object thisValue = this.get(i);
					Object otherValue = otherArray.get(i);
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
}
