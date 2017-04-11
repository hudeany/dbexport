package de.soderer.utilities.json;

import java.io.ByteArrayOutputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import de.soderer.utilities.Utilities;

public class JsonObject implements Iterable<Map.Entry<String, Object>> {
	private Map<String, Object> properties = new LinkedHashMap<String, Object>();

	/**
	 * When using the same key multiple times only the last value will be stored
	 * 
	 * @param key
	 * @param object
	 */
	public void add(String key, Object object) {
		properties.put(key, object);
	}

	public Object remove(String key) {
		return properties.remove(key);
	}

	public Object get(String key) {
		return properties.get(key);
	}
	
	public boolean containsPropertyKey(String propertyKey) {
		return properties.containsKey(propertyKey);
	}

	public Set<String> keySet() {
		return properties.keySet();
	}

	public Set<Entry<String, Object>> entrySet() {
		return properties.entrySet();
	}

	public int size() {
		return properties.size();
	}

	@Override
	public Iterator<Entry<String, Object>> iterator() {
		return properties.entrySet().iterator();
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
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		} else if (other != null && other instanceof JsonObject) {
			JsonObject otherObject = (JsonObject) other;
			if (this.size() != otherObject.size()) {
				return false;
			} else {
				for (Entry<String, Object> propertyEntry : entrySet()) {
					Object thisValue = propertyEntry.getValue();
					Object otherValue = otherObject.get(propertyEntry.getKey());
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
