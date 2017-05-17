package de.soderer.utilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import de.soderer.utilities.collection.UniqueFifoQueuedList;

public class ConfigurationProperties {
	public static String MAXSIZE_EXTENSION = ".maxSize";

	private Properties properties = new Properties();
	private String applicationName;
	private boolean useSubDirectory = false;

	public ConfigurationProperties(String applicationName) {
		this(applicationName, false);
	}

	public ConfigurationProperties(String applicationName, boolean useSubDirectory) {
		this.applicationName = applicationName;
		this.useSubDirectory = useSubDirectory;
		File configFile = new File(System.getProperty("user.home") + (useSubDirectory ? File.separator + "." + applicationName : "") + File.separator + "." + applicationName + ".config");
		if (configFile.exists()) {
			FileInputStream fileInputStream = null;
			try {
				fileInputStream = new FileInputStream(configFile);
				Properties propertiesLoaded = new Properties();
				propertiesLoaded.load(fileInputStream);
				for (Entry<Object, Object> entry : propertiesLoaded.entrySet()) {
					properties.setProperty((String) entry.getKey(), unEscape((String) entry.getValue()));
				}
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException("Cannot load configuration", e);
			} finally {
				Utilities.closeQuietly(fileInputStream);
			}
		}
	}

	public String get(String name) {
		if (properties.containsKey(name)) {
			return properties.getProperty(name);
		} else {
			return null;
		}
	}

	/**
	 * Ensure this key is enclosed in the configuration within the saved file
	 *
	 * @param name
	 */
	public void ensure(String name) {
		if (!properties.containsKey(name)) {
			properties.setProperty(name, "");
		}
	}

	/**
	 * Ensure this key is enclosed in the configuration within the saved file
	 *
	 * @param name
	 */
	public void ensure(String name, boolean defaultValue) {
		if (!properties.containsKey(name)) {
			properties.setProperty(name, defaultValue ? "true" : "false");
		}
	}

	public void ensureRecentList(String name, int maxSize) {
		if (!properties.containsKey(name)) {
			properties.setProperty(name, "");
		}
		if (!properties.containsKey(name + MAXSIZE_EXTENSION)) {
			properties.setProperty(name + MAXSIZE_EXTENSION, Integer.toString(maxSize));
		}
	}

	public void set(String name, String value) {
		properties.setProperty(name, value);
	}

	public void set(String name, int value) {
		properties.setProperty(name, Integer.toString(value));
	}

	public void set(String name, char value) {
		properties.setProperty(name, Character.toString(value));
	}

	public void set(String name, Date value) {
		properties.setProperty(name, new SimpleDateFormat(DateUtilities.YYYYMMDD_HHMMSS).format(value));
	}

	public int getInteger(String name) {
		if (properties.containsKey(name)) {
			return Integer.parseInt(properties.getProperty(name));
		} else {
			return 0;
		}
	}

	public Character getCharacter(String name) {
		if (properties.containsKey(name) && Utilities.isNotEmpty(properties.getProperty(name))) {
			return properties.getProperty(name).toCharArray()[0];
		} else {
			return null;
		}
	}

	public boolean getBoolean(String name) {
		if (properties.containsKey(name)) {
			return Utilities.interpretAsBool(properties.getProperty(name));
		} else {
			return false;
		}
	}

	public Date getDate(String name) {
		try {
			if (properties.containsKey(name)) {
				return new SimpleDateFormat(DateUtilities.YYYYMMDD_HHMMSS).parse(properties.getProperty(name));
			} else {
				return null;
			}
		} catch (ParseException e) {
			return null;
		}
	}

	public void set(String name, boolean value) {
		properties.setProperty(name, value ? "true" : "false");
	}

	public List<String> getList(String name) {
		if (properties.containsKey(name) && Utilities.isNotEmpty(properties.getProperty(name))) {
			try {
				return CsvReader.parseCsvLine(new CsvFormat().setSeparator(';').setStringQuote('"'), properties.getProperty(name));
			} catch (Exception e) {
				return new ArrayList<String>();
			}
		} else {
			return new ArrayList<String>();
		}
	}

	public boolean removeFromList(String name, String value) {
		if (properties.containsKey(name) && Utilities.isNotEmpty(properties.getProperty(name))) {
			try {
				List<String> currentList = CsvReader.parseCsvLine(new CsvFormat().setSeparator(';').setStringQuote('"'), properties.getProperty(name));
				boolean removedValue = currentList.remove(value);
				set(name, currentList);
				return removedValue;
			} catch (Exception e) {
				return false;
			}
		} else {
			return false;
		}
	}

	public void set(String name, List<String> list) {
		properties.setProperty(name, CsvWriter.getCsvLine(';', '"', list));
	}

	public void addRecentListEntry(String name, String value) {
		List<String> valueList = new UniqueFifoQueuedList<String>(getInteger(name + MAXSIZE_EXTENSION), getList(name));
		valueList.add(value);
		set(name, valueList);
	}

	public void addListEntry(String name, String value) {
		List<String> valueList = getList(name);
		valueList.add(value);
		set(name, valueList);
	}

	public void addSetEntry(String name, String value) {
		Set<String> valueSet = new HashSet<String>(getList(name));
		valueSet.add(value);
		set(name, new ArrayList<String>(valueSet));
	}

	public String getLatestRecentValue(String name) {
		return new UniqueFifoQueuedList<String>(getInteger(name + MAXSIZE_EXTENSION), getList(name)).getLatestAdded();
	}

	public void save() {
		FileOutputStream fileOutputStream = null;
		try {
			if (useSubDirectory && !new File(System.getProperty("user.home") + File.separator + "." + applicationName).exists()) {
				new File(System.getProperty("user.home") + File.separator + "." + applicationName).mkdirs();
			}
			fileOutputStream = new FileOutputStream(
					System.getProperty("user.home") + (useSubDirectory ? File.separator + "." + applicationName : "") + File.separator + "." + applicationName + ".config");
			Properties propertiesToStore = new Properties();
			for (Entry<Object, Object> entry : properties.entrySet()) {
				propertiesToStore.setProperty((String) entry.getKey(), escape((String) entry.getValue()));
			}
			propertiesToStore.store(fileOutputStream, applicationName + ".config");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			Utilities.closeQuietly(fileOutputStream);
		}
	}

	public List<String> getKeyList() {
		List<String> returnList = new ArrayList<String>();
		for (Object key : properties.keySet()) {
			returnList.add(key.toString());
		}
		return returnList;
	}

	public boolean containsKey(String name) {
		return properties.containsKey(name);
	}

	public static String escape(String value) {
		return value.replace("\r", "\\r").replace("\n", "\\n").replace("\t", "\\t").replace("\\", "\\\\");
	}

	public static String unEscape(String value) {
		return value.replace("\\\\", "\\").replace("\\t", "\t").replace("\\n", "\n").replace("\\r", "\r");
	}
}
