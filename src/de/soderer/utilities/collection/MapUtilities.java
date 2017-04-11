package de.soderer.utilities.collection;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class MapUtilities {
	public static <K extends Comparable<K>, V> LinkedHashMap<K, V> sort(Map<K, V> map) {
		LinkedHashMap<K, V> returnMap = new LinkedHashMap<K, V>();
		List<K> keys = new LinkedList<K>(map.keySet());
		Collections.sort(keys);
		for (K key : keys) {
			returnMap.put(key, map.get(key));
		}
		return returnMap;
	}

	public static <K, V> LinkedHashMap<K, V> sort(Map<K, V> map, Comparator<K> comparator) {
		LinkedHashMap<K, V> returnMap = new LinkedHashMap<K, V>();
		List<K> keys = new LinkedList<K>(map.keySet());
		Collections.sort(keys, comparator);
		for (K key : keys) {
			returnMap.put(key, map.get(key));
		}
		return returnMap;
	}

	public static <K, V> LinkedHashMap<K, V> sortEntries(Map<K, V> map, Comparator<Entry<K, V>> comparator) {
		LinkedHashMap<K, V> returnMap = new LinkedHashMap<K, V>();
		List<Entry<K, V>> entries = new LinkedList<Entry<K, V>>(map.entrySet());
		Collections.sort(entries, comparator);
		for (Entry<K, V> entry : entries) {
			returnMap.put(entry.getKey(), entry.getValue());
		}
		return returnMap;
	}

	public static <K extends Comparable<K>, V, L extends LinkedHashMap<K, V>> L sort(L map) {
		@SuppressWarnings("unchecked")
		L returnMap = (L) map.clone();
		returnMap.clear();
		List<K> keys = new LinkedList<K>(map.keySet());
		Collections.sort(keys);
		for (K key : keys) {
			returnMap.put(key, map.get(key));
		}
		return returnMap;
	}

	public static <K, V, L extends LinkedHashMap<K, V>> L sort(L map, Comparator<K> comparator) {
		@SuppressWarnings("unchecked")
		L returnMap = (L) map.clone();
		returnMap.clear();
		List<K> keys = new LinkedList<K>(map.keySet());
		Collections.sort(keys, comparator);
		for (K key : keys) {
			returnMap.put(key, map.get(key));
		}
		return returnMap;
	}

	public static <K, V, L extends LinkedHashMap<K, V>> L sortEntries(L map, Comparator<Entry<K, V>> comparator) {
		@SuppressWarnings("unchecked")
		L returnMap = (L) map.clone();
		returnMap.clear();
		List<Entry<K, V>> entries = new LinkedList<Entry<K, V>>(map.entrySet());
		Collections.sort(entries, comparator);
		for (Entry<K, V> entry : entries) {
			returnMap.put(entry.getKey(), entry.getValue());
		}
		return returnMap;
	}

	public static <K, V, L extends HashMap<K, V>> L filterEntries(L map, Predicate<Entry<K, V>> predicate) {
		@SuppressWarnings("unchecked")
		L returnMap = (L) map.clone();
		returnMap.clear();
		for (Entry<K, V> entry : map.entrySet()) {
			if (predicate.evaluate(entry)) {
				returnMap.put(entry.getKey(), entry.getValue());
			}
		}
		return returnMap;
	}
	
	public static String mapToString(Map<?, ?> map) {
		return mapToString(map, '\"', false, ',');
	}
	
	public static String mapToString(Map<?, ?> map, char textQuoteChar, boolean alwaysQuote, char entrySeparatorChar) {
		StringBuilder returnValue = new StringBuilder();
		
		String quoteString = Character.toString(textQuoteChar);
		String doubleQuote = quoteString + quoteString;
		String entrySeparatorString = Character.toString(entrySeparatorChar);
		
		for (Entry<?, ?> entry : map.entrySet()) {
			if (returnValue.length() > 0) {
				returnValue.append(entrySeparatorChar);
			}
			
			String key;
			if (entry.getKey() == null) {
				key = "null";
			} else {
				key = entry.getKey().toString();
			}
			if (key.contains(quoteString) || key.contains(entrySeparatorString) || alwaysQuote) {
				key = quoteString + key.replace(quoteString, doubleQuote) + quoteString;
			}
			returnValue.append(key);
			
			returnValue.append('=');
			
			String value;
			if (entry.getValue() == null) {
				value = "null";
			} else {
				value = entry.getValue().toString();
			}
			if (value.contains(quoteString) || value.contains(entrySeparatorString) || alwaysQuote) {
				value = quoteString + value.replace(quoteString, doubleQuote) + quoteString;
			}
			returnValue.append(value);
		}
		
		return returnValue.toString();
	}
}
