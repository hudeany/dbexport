package de.soderer.dbimport.dataprovider;

import java.io.Closeable;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.NumberUtilities;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.DbColumnType;
import de.soderer.utilities.db.SimpleDataType;

public abstract class DataProvider implements Closeable {
	abstract public String getConfigurationLogString();
	abstract public List<String> getAvailableDataPropertyNames() throws Exception;
	abstract public long getItemsAmountToImport() throws Exception;
	abstract public long getReadDataSize();
	abstract public Map<String, Object> getNextItemData() throws Exception;
	abstract public Map<String, DbColumnType> scanDataPropertyTypes(Map<String, Tuple<String, String>> mapping) throws Exception;
	abstract public File filterDataItems(List<Integer> indexList, String fileSuffix) throws Exception;
	abstract public long getImportDataAmount();

	static void detectNextDataType(final Map<String, Tuple<String, String>> mapping, final Map<String, DbColumnType> dataTypes, final String propertyKey, final String currentValue) {
		String formatInfo = null;
		if (mapping != null) {
			for (final Tuple<String, String> mappingValue : mapping.values()) {
				if (mappingValue.getFirst().equals(propertyKey)) {
					if (Utilities.isNotBlank(mappingValue.getSecond())) {
						formatInfo = mappingValue.getSecond();
						break;
					}
				}
			}
		}

		final SimpleDataType currentType = dataTypes.get(propertyKey) == null ? null : dataTypes.get(propertyKey).getSimpleDataType();
		if (currentType != SimpleDataType.Blob) {
			if (Utilities.isEmpty(currentValue)) {
				if (!dataTypes.containsKey(propertyKey)) {
					dataTypes.put(propertyKey, null);
				}
			} else if ("file".equalsIgnoreCase(formatInfo) || currentValue.length() > 4000) {
				dataTypes.put(propertyKey, new DbColumnType("BLOB", -1, -1, -1, true, false));
			} else if (currentType != SimpleDataType.String && currentType != SimpleDataType.Integer && currentType != SimpleDataType.Float && currentType != SimpleDataType.Blob && currentType != SimpleDataType.Clob && Utilities.isNotBlank(formatInfo) && !".".equals(formatInfo) && !",".equals(formatInfo) && !"file".equalsIgnoreCase(formatInfo) && !"lc".equalsIgnoreCase(formatInfo) && !"uc".equalsIgnoreCase(formatInfo)) {
				try {
					DateUtilities.parseLocalDateTime(formatInfo, currentValue.trim());
					if (formatInfo != null && (formatInfo.toLowerCase().contains("h") || formatInfo.contains("m") || formatInfo.toLowerCase().contains("s"))) {
						dataTypes.put(propertyKey, new DbColumnType("TIMESTAMP", -1, -1, -1, true, false));
					} else {
						dataTypes.put(propertyKey, new DbColumnType("DATE", -1, -1, -1, true, false));
					}
				} catch (@SuppressWarnings("unused") final Exception e) {
					if (NumberUtilities.isInteger(currentValue) && currentValue.trim().length() <= 10) {
						dataTypes.put(propertyKey, new DbColumnType("INTEGER", -1, -1, -1, true, false));
					} else if (NumberUtilities.isDouble(currentValue) && currentValue.trim().length() <= 20) {
						dataTypes.put(propertyKey, new DbColumnType("DOUBLE", -1, -1, -1, true, false));
					} else {
						dataTypes.put(propertyKey, new DbColumnType("VARCHAR", Math.max(dataTypes.get(propertyKey) == null ? 0 : dataTypes.get(propertyKey).getCharacterByteSize(), currentValue.getBytes(StandardCharsets.UTF_8).length), -1, -1, true, false));
					}
				}
			} else if (currentType != SimpleDataType.String && currentType != SimpleDataType.Integer && currentType != SimpleDataType.Float && currentType != SimpleDataType.Blob && currentType != SimpleDataType.Clob && currentType != SimpleDataType.Date && Utilities.isBlank(formatInfo)) {
				try {
					DateUtilities.parseLocalDateTime(DateUtilities.getDateTimeFormatWithSecondsPattern(Locale.getDefault()), currentValue.trim());
					dataTypes.put(propertyKey, new DbColumnType("TIMESTAMP", -1, -1, -1, true, false));
				} catch (@SuppressWarnings("unused") final Exception e) {
					try {
						DateUtilities.parseLocalDate(DateUtilities.getDateFormatPattern(Locale.getDefault()), currentValue.trim());
						dataTypes.put(propertyKey, new DbColumnType("DATE", -1, -1, -1, true, false));
					} catch (@SuppressWarnings("unused") final Exception e1) {
						if (NumberUtilities.isInteger(currentValue) && currentValue.trim().length() <= 10) {
							dataTypes.put(propertyKey, new DbColumnType("INTEGER", -1, -1, -1, true, false));
						} else if (NumberUtilities.isDouble(currentValue) && currentValue.trim().length() <= 20) {
							dataTypes.put(propertyKey, new DbColumnType("DOUBLE", -1, -1, -1, true, false));
						} else {
							dataTypes.put(propertyKey, new DbColumnType("VARCHAR", Math.max(dataTypes.get(propertyKey) == null ? 0 : dataTypes.get(propertyKey).getCharacterByteSize(), currentValue.getBytes(StandardCharsets.UTF_8).length), -1, -1, true, false));
						}
					}
				}
			} else if (currentType != SimpleDataType.String && currentType != SimpleDataType.Integer && currentType != SimpleDataType.Float && currentType != SimpleDataType.Blob && currentType != SimpleDataType.Clob && currentType != SimpleDataType.DateTime && Utilities.isBlank(formatInfo)) {
				try {
					DateUtilities.parseLocalDate(DateUtilities.getDateFormatPattern(Locale.getDefault()), currentValue.trim());
					dataTypes.put(propertyKey, new DbColumnType("DATE", -1, -1, -1, true, false));
				} catch (@SuppressWarnings("unused") final Exception e) {
					if (NumberUtilities.isInteger(currentValue) && currentValue.trim().length() <= 10) {
						dataTypes.put(propertyKey, new DbColumnType("INTEGER", -1, -1, -1, true, false));
					} else if (NumberUtilities.isDouble(currentValue) && currentValue.trim().length() <= 20) {
						dataTypes.put(propertyKey, new DbColumnType("DOUBLE", -1, -1, -1, true, false));
					} else {
						dataTypes.put(propertyKey, new DbColumnType("VARCHAR", Math.max(dataTypes.get(propertyKey) == null ? 0 : dataTypes.get(propertyKey).getCharacterByteSize(), currentValue.getBytes(StandardCharsets.UTF_8).length), -1, -1, true, false));
					}
				}
			} else if (currentType != SimpleDataType.String && currentType != SimpleDataType.Date && currentType != SimpleDataType.DateTime && currentType != SimpleDataType.Float && NumberUtilities.isInteger(currentValue) && currentValue.trim().length() <= 10) {
				dataTypes.put(propertyKey, new DbColumnType("INTEGER", -1, -1, -1, true, false));
			} else if (currentType != SimpleDataType.String && currentType != SimpleDataType.Date && currentType != SimpleDataType.DateTime && NumberUtilities.isDouble(currentValue) && currentValue.trim().length() <= 20) {
				dataTypes.put(propertyKey, new DbColumnType("DOUBLE", -1, -1, -1, true, false));
			} else {
				dataTypes.put(propertyKey, new DbColumnType("VARCHAR", Math.max(dataTypes.get(propertyKey) == null ? 0 : dataTypes.get(propertyKey).getCharacterByteSize(), currentValue.getBytes(StandardCharsets.UTF_8).length), -1, -1, true, false));
			}
		}
	}

	@SuppressWarnings("static-method")
	public String getItemsUnitSign() throws Exception {
		return null;
	}
}
