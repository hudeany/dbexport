package de.soderer.dbimport.dataprovider;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import de.soderer.utilities.CountingInputStream;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.InputStreamWithOtherItemsToClose;
import de.soderer.utilities.IoUtilities;
import de.soderer.utilities.NumberUtilities;
import de.soderer.utilities.TarGzUtilities;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.DbColumnType;
import de.soderer.utilities.db.SimpleDataType;
import de.soderer.utilities.zip.Zip4jUtilities;
import de.soderer.utilities.zip.ZipUtilities;

public abstract class DataProvider implements Closeable {
	abstract public List<String> getAvailableDataPropertyNames() throws Exception;
	abstract public long getItemsAmountToImport() throws Exception;
	abstract public String getItemsUnitSign() throws Exception;
	abstract public Map<String, Object> getNextItemData() throws Exception;
	abstract public Map<String, DbColumnType> scanDataPropertyTypes(Map<String, Tuple<String, String>> mapping) throws Exception;
	abstract public File filterDataItems(List<Integer> indexList, String fileSuffix) throws Exception;

	private final boolean isInlineData;
	private final String importInlineData;
	private final File importFile;
	protected char[] zipPassword;

	private CountingInputStream inputStream = null;

	public DataProvider(final boolean isInlineData, final String importFilePathOrData, final char[] zipPassword) {
		this.isInlineData = isInlineData;
		if (isInlineData) {
			importInlineData = importFilePathOrData;
			importFile = null;
			this.zipPassword = null;
		} else {
			importFile = new File(importFilePathOrData);
			this.zipPassword = zipPassword;
			importInlineData = null;
		}
	}

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
				} catch (final Exception e) {
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
				} catch (final Exception e) {
					try {
						DateUtilities.parseLocalDate(DateUtilities.getDateFormatPattern(Locale.getDefault()), currentValue.trim());
						dataTypes.put(propertyKey, new DbColumnType("DATE", -1, -1, -1, true, false));
					} catch (final Exception e1) {
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
				} catch (final Exception e) {
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

	protected InputStream getInputStream() throws Exception {
		if (isInlineData) {
			if (Utilities.isBlank(importInlineData)) {
				throw new Exception("Import inline data is empty");
			} else {
				return new ByteArrayInputStream(importInlineData.getBytes(StandardCharsets.UTF_8));
			}
		} else {
			if (!importFile.exists()) {
				throw new Exception("Import file does not exist: " + importFile.getAbsolutePath());
			} else if (importFile.isDirectory()) {
				throw new Exception("Import path is a directory: " + importFile.getAbsolutePath());
			} else if (importFile.length() == 0) {
				throw new Exception("Import file is empty: " + importFile.getAbsolutePath());
			} else {
				try {
					if (Utilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".zip")) {
						if (ZipUtilities.getZipFileEntries(importFile).size() != 1) {
							throw new Exception("Compressed import file does not contain a single compressed file: " + importFile.getAbsolutePath());
						} else {
							if (zipPassword != null) {
								inputStream = new CountingInputStream(Zip4jUtilities.openPasswordSecuredZipFile(importFile.getAbsolutePath(), zipPassword));
							} else {
								inputStream = new CountingInputStream(ZipUtilities.openZipFile(importFile.getAbsolutePath()));
							}
						}
					} else if (Utilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".tar.gz")) {
						if (TarGzUtilities.getFilesCount(importFile) != 1) {
							throw new Exception("Compressed import file does not contain a single compressed file: " + importFile.getAbsolutePath());
						} else {
							inputStream = new CountingInputStream(TarGzUtilities.openCompressedFile(importFile));
						}
					} else if (Utilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".tgz")) {
						if (TarGzUtilities.getFilesCount(importFile) != 1) {
							throw new Exception("Compressed import file does not contain a single compressed file: " + importFile.getAbsolutePath());
						} else {
							inputStream = new CountingInputStream(TarGzUtilities.openCompressedFile(importFile));
						}
					} else if (Utilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".gz")) {
						inputStream = new CountingInputStream(new GZIPInputStream(new FileInputStream(importFile)));
					} else {
						inputStream = new CountingInputStream(new InputStreamWithOtherItemsToClose(new FileInputStream(importFile), importFile.getAbsolutePath()));
					}
					return inputStream;
				} catch (final Exception e) {
					if (inputStream != null) {
						try {
							inputStream.close();
						} catch (final IOException e1) {
							// do nothing
						}
					}
					throw e;
				}
			}
		}
	}

	public String getConfigurationLogString() {
		if (isInlineData) {
			return "Inline data: true\n";
		} else {
			String configurationLogString = "File: " + importFile.getAbsolutePath() + "\n";
			if (Utilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".zip")) {
				configurationLogString += "Compression: zip\n";
				if (zipPassword != null) {
					configurationLogString += "ZipPassword: true\n";
				}
			} else if (Utilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".tar.gz")) {
				configurationLogString += "Compression: targz\n";
			} else if (Utilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".tgz")) {
				configurationLogString += "Compression: tgz\n";
			} else if (Utilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".gz")) {
				configurationLogString += "Compression: gz\n";
			}
			return configurationLogString;
		}
	}

	@Override
	public void close() {
		if (inputStream != null) {
			try {
				inputStream.close();
			} catch (final Exception e) {
				e.printStackTrace();
			}
			inputStream = null;
		}
	}

	public String getImportFilePath() {
		return importFile.getAbsolutePath();
	}

	/**
	 * Raw import data size.<br/>
	 * This means, the data might be compressed and this is the size of the <b>compressed</b> data.
	 */
	public long getImportDataSize() {
		if (isInlineData) {
			return importInlineData.getBytes(StandardCharsets.UTF_8).length;
		} else {
			return importFile.length();
		}
	}

	/**
	 * Real import data size.<br/>
	 * This means, the data might be compressed and this is the size of the <b>uncompressed</b> data.
	 */
	public long getImportDataAmount() throws Exception {
		if (isInlineData) {
			return importInlineData.getBytes(StandardCharsets.UTF_8).length;
		} else {
			if (Utilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".zip")) {
				if (zipPassword != null) {
					return Zip4jUtilities.getUncompressedSize(importFile, zipPassword);
				} else {
					return ZipUtilities.getDataSizeUncompressed(importFile);
				}
			} else if (Utilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".tar.gz")) {
				return TarGzUtilities.getUncompressedSize(importFile);
			} else if (Utilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".tgz")) {
				return TarGzUtilities.getUncompressedSize(importFile);
			} else if (Utilities.endsWithIgnoreCase(importFile.getAbsolutePath(), ".gz")) {
				try (InputStream gzStream = new GZIPInputStream(new FileInputStream(importFile))) {
					return IoUtilities.getStreamSize(gzStream);
				}
			} else {
				return importFile.length();
			}
		}
	}

	public long getReadDataSize() {
		if (inputStream != null) {
			return inputStream.getByteCount();
		} else {
			return 0;
		}
	}
}
