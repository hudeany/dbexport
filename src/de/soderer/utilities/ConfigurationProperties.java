package de.soderer.utilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import de.soderer.utilities.collection.UniqueFifoQueuedList;
import de.soderer.utilities.csv.CsvFormat;
import de.soderer.utilities.csv.CsvReader;
import de.soderer.utilities.csv.CsvWriter;

public class ConfigurationProperties {
	public static String MAXSIZE_EXTENSION = ".maxSize";

	private final Properties properties = new Properties();
	private final String applicationName;
	private boolean useSubDirectory = false;

	public ConfigurationProperties(final String applicationName) throws Exception {
		this(applicationName, false);
	}

	public ConfigurationProperties(final String applicationName, final boolean useSubDirectory) throws Exception {
		this.applicationName = applicationName;
		this.useSubDirectory = useSubDirectory;
		final File configFile = new File(System.getProperty("user.home") + (useSubDirectory ? File.separator + "." + applicationName : "") + File.separator + "." + applicationName + ".config");
		if (configFile.exists()) {
			try (FileInputStream fileInputStream = new FileInputStream(configFile)) {
				final Properties propertiesLoaded = new Properties();
				propertiesLoaded.load(fileInputStream);
				for (final Entry<Object, Object> entry : propertiesLoaded.entrySet()) {
					properties.setProperty((String) entry.getKey(), unEscape((String) entry.getValue()));
				}
			} catch (final IOException e) {
				e.printStackTrace();
				throw new Exception("Cannot load configuration", e);
			}
		}
	}

	public String get(final String name) {
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
	public void ensure(final String name) {
		if (!properties.containsKey(name)) {
			properties.setProperty(name, "");
		}
	}

	/**
	 * Ensure this key is enclosed in the configuration within the saved file
	 *
	 * @param name
	 */
	public void ensure(final String name, final boolean defaultValue) {
		if (!properties.containsKey(name)) {
			properties.setProperty(name, defaultValue ? "true" : "false");
		}
	}

	public void ensureRecentList(final String name, final int maxSize) {
		if (!properties.containsKey(name)) {
			properties.setProperty(name, "");
		}
		if (!properties.containsKey(name + MAXSIZE_EXTENSION)) {
			properties.setProperty(name + MAXSIZE_EXTENSION, Integer.toString(maxSize));
		}
	}

	public void set(final String name, final String value) {
		properties.setProperty(name, value);
	}

	public void set(final String name, final int value) {
		properties.setProperty(name, Integer.toString(value));
	}

	public void set(final String name, final char value) {
		properties.setProperty(name, Character.toString(value));
	}

	public void set(final String name, final LocalDateTime value) {
		properties.setProperty(name, DateUtilities.formatDate(DateUtilities.YYYYMMDD_HHMMSS, value));
	}

	public int getInteger(final String name) {
		if (properties.containsKey(name)) {
			return Integer.parseInt(properties.getProperty(name));
		} else {
			return 0;
		}
	}

	public Character getCharacter(final String name) {
		if (properties.containsKey(name) && Utilities.isNotEmpty(properties.getProperty(name))) {
			return properties.getProperty(name).toCharArray()[0];
		} else {
			return null;
		}
	}

	public boolean getBoolean(final String name) {
		if (properties.containsKey(name)) {
			return Utilities.interpretAsBool(properties.getProperty(name));
		} else {
			return false;
		}
	}

	public LocalDateTime getDate(final String name) {
		try {
			if (properties.containsKey(name)) {
				return DateUtilities.parseLocalDateTime(DateUtilities.YYYYMMDD_HHMMSS, properties.getProperty(name));
			} else {
				return null;
			}
		} catch (@SuppressWarnings("unused") final DateTimeParseException e) {
			return null;
		}
	}

	public void set(final String name, final boolean value) {
		properties.setProperty(name, value ? "true" : "false");
	}

	public List<String> getList(final String name) {
		if (properties.containsKey(name) && Utilities.isNotEmpty(properties.getProperty(name))) {
			try {
				return CsvReader.parseCsvLine(new CsvFormat().setSeparator(';').setStringQuote('"'), properties.getProperty(name));
			} catch (@SuppressWarnings("unused") final Exception e) {
				return new ArrayList<>();
			}
		} else {
			return new ArrayList<>();
		}
	}

	public boolean removeFromList(final String name, final String value) {
		if (properties.containsKey(name) && Utilities.isNotEmpty(properties.getProperty(name))) {
			try {
				final List<String> currentList = CsvReader.parseCsvLine(new CsvFormat().setSeparator(';').setStringQuote('"'), properties.getProperty(name));
				final boolean removedValue = currentList.remove(value);
				set(name, currentList);
				return removedValue;
			} catch (@SuppressWarnings("unused") final Exception e) {
				return false;
			}
		} else {
			return false;
		}
	}

	public void set(final String name, final List<String> list) {
		properties.setProperty(name, CsvWriter.getCsvLine(';', '"', list));
	}

	public void addRecentListEntry(final String name, final String value) {
		final List<String> valueList = new UniqueFifoQueuedList<>(getInteger(name + MAXSIZE_EXTENSION), getList(name));
		valueList.add(value);
		set(name, valueList);
	}

	public void addListEntry(final String name, final String value) {
		final List<String> valueList = getList(name);
		valueList.add(value);
		set(name, valueList);
	}

	public void addSetEntry(final String name, final String value) {
		final Set<String> valueSet = new HashSet<>(getList(name));
		valueSet.add(value);
		set(name, new ArrayList<>(valueSet));
	}

	public String getLatestRecentValue(final String name) {
		return new UniqueFifoQueuedList<>(getInteger(name + MAXSIZE_EXTENSION), getList(name)).getLatestAdded();
	}

	public void save() {
		if (useSubDirectory && !new File(System.getProperty("user.home") + File.separator + "." + applicationName).exists()) {
			new File(System.getProperty("user.home") + File.separator + "." + applicationName).mkdirs();
		}
		try (FileOutputStream fileOutputStream = new FileOutputStream(System.getProperty("user.home") + (useSubDirectory ? File.separator + "." + applicationName : "") + File.separator + "." + applicationName + ".config")) {
			final Properties propertiesToStore = new Properties();
			for (final Entry<Object, Object> entry : properties.entrySet()) {
				propertiesToStore.setProperty((String) entry.getKey(), escape((String) entry.getValue()));
			}
			propertiesToStore.store(fileOutputStream, applicationName + ".config");
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	public List<String> getKeyList() {
		final List<String> returnList = new ArrayList<>();
		for (final Object key : properties.keySet()) {
			returnList.add(key.toString());
		}
		return returnList;
	}

	public boolean containsKey(final String name) {
		return properties.containsKey(name);
	}

	public static String escape(final String value) {
		return value.replace("\r", "\\r").replace("\n", "\\n").replace("\t", "\\t").replace("\\", "\\\\");
	}

	public static String unEscape(final String value) {
		return value.replace("\\\\", "\\").replace("\\t", "\t").replace("\\n", "\n").replace("\\r", "\r");
	}
}
