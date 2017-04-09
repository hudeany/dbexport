package de.soderer.utilities;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import de.soderer.utilities.collection.CaseInsensitiveOrderedMap;

public class SectionedProperties {
	private static final String COMMENT_PREFIX = "#";
	/**
	 *  Blanks around KEY_VALUE_SEPARATOR are ignored.
	 */
	private static final String KEY_VALUE_SEPARATOR = "=";
	private static final String SECTION_PREFIX = "[";
	private static final String SECTION_SUFFIX = "]";

	private boolean useCaseInsensitiveKeys = true;
	private Map<String, Map<String, String>> entries = null;

	public SectionedProperties() {
	}
	
	public SectionedProperties(boolean useCaseInsensitiveKeys) {
		this.useCaseInsensitiveKeys = useCaseInsensitiveKeys;
	}
	
	public void load(InputStream inputStream) throws Exception {
		if (useCaseInsensitiveKeys) {
			entries = new CaseInsensitiveOrderedMap<Map<String, String>>();
		} else {
			entries = new LinkedHashMap<String, Map<String, String>>();
		}

		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
		String rawLine = null;
		String currentSectionName = null;
		int lineCount = 0;
		while ((rawLine = reader.readLine()) != null) {
			lineCount++;
			String line = rawLine.trim();
			if (Utilities.isNotBlank(line) && !line.startsWith(COMMENT_PREFIX)) {
				if (line.startsWith(SECTION_PREFIX)) {
					if (line.endsWith(SECTION_SUFFIX)) {
						currentSectionName = line.substring(1, line.length() - 1).trim();
					} else {
						throw new Exception("Invalid section line (" + lineCount + "): " + rawLine); 
					}
				} else {
					String key;
					String value;
					int posSeparator = line.indexOf(KEY_VALUE_SEPARATOR);
					if (posSeparator < 0) {
						key = line;
						value = "";
					} else {
						key = line.substring(0, posSeparator).trim();
						value = unescapeValue(line.substring(posSeparator + 1).trim());
					}
	
					if (key.length() <= 0) {
						throw new Exception("Invalid key at line " + lineCount);
					}

					if (!entries.containsKey(currentSectionName)) {
						if (useCaseInsensitiveKeys) {
							entries.put(currentSectionName, new CaseInsensitiveOrderedMap<String>());
						} else {
							entries.put(currentSectionName, new LinkedHashMap<String, String>());
						}
					}
					
					entries.get(currentSectionName).put(key, value);
				}
			}
		}
	}

	public void save(OutputStream outputStream) throws Exception {
		for (Entry<String, Map<String, String>> sectionEntry : entries.entrySet()) {
			outputStream.write((SECTION_PREFIX + sectionEntry.getKey().trim() + SECTION_SUFFIX + "\n").getBytes("UTF-8"));
			for (Entry<String, String> keyValueEntry : sectionEntry.getValue().entrySet()) {
				outputStream.write((keyValueEntry.getKey().trim() + KEY_VALUE_SEPARATOR + escapeValue(keyValueEntry.getValue().trim())+ "\n").getBytes("UTF-8"));
			}
			outputStream.write("\n".getBytes("UTF-8"));
		}
	}

	public Set<String> getSectionNames() {
		if (entries != null) {
			return entries.keySet();
		} else {
			return null;
		}
	}
	
	public Set<String> getSectionKeys(String sectionName) {
		if (entries != null) {
			Map<String, String> section = entries.get(sectionName);
			if (section != null) {
				return section.keySet();
			} else {
				return null;
			}
		} else {
			return null;
		}
	}
	
	public String getValue(String sectionName, String key) {
		if (entries != null) {
			Map<String, String> section = entries.get(sectionName);
			if (section != null) {
				return section.get(key);
			} else {
				return null;
			}
		} else {
			return null;
		}
	}
	
	public void setValue(String sectionName, String key, String value) {
		if (entries == null) {
			if (useCaseInsensitiveKeys) {
				entries = new CaseInsensitiveOrderedMap<Map<String, String>>();
			} else {
				entries = new LinkedHashMap<String, Map<String, String>>();
			}
		}

		if (!entries.containsKey(sectionName)) {
			if (useCaseInsensitiveKeys) {
				entries.put(sectionName, new CaseInsensitiveOrderedMap<String>());
			} else {
				entries.put(sectionName, new LinkedHashMap<String, String>());
			}
		}
		
		entries.get(sectionName).put(key, value);
	}
	
	private String unescapeValue(String value) {
		return value.replace("\\n", "\n");
	}
	
	private String escapeValue(String value) {
		return value.replace("\r\n", "\\n").replace("\n", "\\n").replace("\r", "\\n");
	}
}
