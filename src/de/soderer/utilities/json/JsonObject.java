package de.soderer.utilities.json;

import java.io.ByteArrayOutputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import de.soderer.utilities.Utilities;

public class JsonObject implements Iterable<Map.Entry<String, Object>>, JsonItem {
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

	public Set<String> keySet() {
		return properties.keySet();
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
	public boolean isJsonObject() {
		return true;
	}

	@Override
	public boolean isJsonArray() {
		return false;
	}
}
