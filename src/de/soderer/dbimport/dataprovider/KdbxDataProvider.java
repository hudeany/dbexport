package de.soderer.dbimport.dataprovider;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.TarGzUtilities;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.DbColumnType;
import de.soderer.utilities.db.SimpleDataType;
import de.soderer.utilities.kdbx.KdbxDatabase;
import de.soderer.utilities.kdbx.KdbxReader;
import de.soderer.utilities.kdbx.KdbxWriter;
import de.soderer.utilities.kdbx.data.KdbxEntry;
import de.soderer.utilities.zip.Zip4jUtilities;
import de.soderer.utilities.zip.ZipUtilities;

public class KdbxDataProvider extends DataProvider {
	private KdbxDatabase kdbxDatabase = null;
	private Iterator<KdbxEntry> kdbxEntryIterator = null;
	private List<String> dataPropertyNames = null;
	private final Map<String, DbColumnType> dataTypes = null;
	private Integer itemsAmount = null;
	private char[] password = null;

	public KdbxDataProvider(final String importFilePath, final char[] password) {
		super(false, importFilePath, null);
		this.password = password;
	}

	@Override
	public String getConfigurationLogString() {
		return super.getConfigurationLogString()
				+ "Format: Kdbx" + "\n";
	}

	@Override
	public Map<String, DbColumnType> scanDataPropertyTypes(final Map<String, Tuple<String, String>> mapping) throws Exception {
		if (dataTypes == null) {
			openKdbxDatabase();

			int itemCount = 0;
			dataPropertyNames = new ArrayList<>();

			Map<String, Object> nextItem;
			while ((nextItem = getNextItemData()) != null) {
				for (final Entry<String, Object> itemProperty : nextItem.entrySet()) {
					final String propertyName = itemProperty.getKey();
					final Object propertyValue = itemProperty.getValue();

					String formatInfo = null;
					if (mapping != null) {
						for (final Tuple<String, String> mappingValue : mapping.values()) {
							if (mappingValue.getFirst().equals(propertyName)) {
								if (Utilities.isNotBlank(mappingValue.getSecond())) {
									formatInfo = mappingValue.getSecond();
									break;
								}
							}
						}
					}

					final SimpleDataType currentType = dataTypes.get(propertyName) == null ? null : dataTypes.get(propertyName).getSimpleDataType();
					if (currentType != SimpleDataType.Blob) {
						if (propertyValue == null) {
							if (!dataTypes.containsKey(propertyName)) {
								dataTypes.put(propertyName, null);
							}
						} else if ("file".equalsIgnoreCase(formatInfo) || (propertyValue instanceof String && ((String) propertyValue).length() > 4000)) {
							dataTypes.put(propertyName, new DbColumnType("BLOB", -1, -1, -1, true, false));
						} else if (currentType != SimpleDataType.String && Utilities.isNotBlank(formatInfo) && !".".equals(formatInfo) && !",".equals(formatInfo) && !"file".equalsIgnoreCase(formatInfo) && propertyValue instanceof String) {
							final String value = ((String) propertyValue).trim();
							try {
								DateUtilities.parseLocalDateTime(formatInfo, value);
								dataTypes.put(propertyName, new DbColumnType("TIMESTAMP", -1, -1, -1, true, false));
							} catch (@SuppressWarnings("unused") final Exception e) {
								try {
									DateUtilities.parseLocalDateTime(DateUtilities.ISO_8601_DATETIME_FORMAT, value);
									dataTypes.put(propertyName, new DbColumnType("TIMESTAMP", -1, -1, -1, true, false));
								} catch (@SuppressWarnings("unused") final Exception e2) {
									dataTypes.put(propertyName, new DbColumnType("VARCHAR", Math.max(dataTypes.get(propertyName) == null ? 0 : dataTypes.get(propertyName).getCharacterByteSize(), value.getBytes(StandardCharsets.UTF_8).length), -1, -1, true, false));
								}
							}
						} else if (currentType != SimpleDataType.String && Utilities.isBlank(formatInfo) && propertyValue instanceof String) {
							final String value = ((String) propertyValue).trim();
							try {
								DateUtilities.parseLocalDateTime(DateUtilities.ISO_8601_DATETIME_FORMAT, value);
								dataTypes.put(propertyName, new DbColumnType("TIMESTAMP", -1, -1, -1, true, false));
							} catch (@SuppressWarnings("unused") final Exception e) {
								try {
									DateUtilities.parseDateTime(DateUtilities.ISO_8601_DATE_FORMAT, value);
									dataTypes.put(propertyName, new DbColumnType("DATE", -1, -1, -1, true, false));
								} catch (@SuppressWarnings("unused") final Exception e2) {
									dataTypes.put(propertyName, new DbColumnType("VARCHAR", Math.max(dataTypes.get(propertyName) == null ? 0 : dataTypes.get(propertyName).getCharacterByteSize(), value.getBytes(StandardCharsets.UTF_8).length), -1, -1, true, false));
								}
							}
						} else if (currentType != SimpleDataType.String && currentType != SimpleDataType.DateTime && currentType != SimpleDataType.Float && propertyValue instanceof Integer) {
							dataTypes.put(propertyName, new DbColumnType("INTEGER", -1, -1, -1, true, false));
						} else if (currentType != SimpleDataType.String && currentType != SimpleDataType.DateTime && (propertyValue instanceof Float || propertyValue instanceof Double)) {
							dataTypes.put(propertyName, new DbColumnType("DOUBLE", -1, -1, -1, true, false));
						} else {
							dataTypes.put(propertyName, new DbColumnType("VARCHAR", Math.max(dataTypes.get(propertyName) == null ? 0 : dataTypes.get(propertyName).getCharacterByteSize(), propertyValue.toString().getBytes(StandardCharsets.UTF_8).length), -1, -1, true, false));
						}
					}
				}

				itemCount++;
			}

			close();

			itemsAmount = itemCount;
			dataPropertyNames = new ArrayList<>(dataTypes.keySet());
		}

		return dataTypes;
	}

	@Override
	public List<String> getAvailableDataPropertyNames() throws Exception {
		if (dataPropertyNames == null) {
			openKdbxDatabase();

			int itemCount = 0;
			dataPropertyNames = new ArrayList<>();

			Map<String, Object> nextItem;
			while ((nextItem = getNextItemData()) != null) {
				for (final Entry<String, Object> itemProperty : nextItem.entrySet()) {
					final String propertyName = itemProperty.getKey();
					dataPropertyNames.add(propertyName);
				}

				itemCount++;
			}

			close();

			itemsAmount = itemCount;
		}

		return dataPropertyNames;
	}

	@Override
	public long getItemsAmountToImport() throws Exception {
		if (itemsAmount == null) {
			getAvailableDataPropertyNames();
		}

		return itemsAmount;
	}

	@Override
	public String getItemsUnitSign() throws Exception {
		return null;
	}

	@Override
	public Map<String, Object> getNextItemData() throws Exception {
		if (kdbxEntryIterator == null) {
			openKdbxDatabase();
		}

		final KdbxEntry kdbxEntry = kdbxEntryIterator.next();

		if (kdbxEntry == null) {
			return null;
		} else {
			final Map<String, Object> returnMap = new HashMap<>();
			for (final Entry<String, Object> itemEntry : kdbxEntry.getItems().entrySet()) {
				returnMap.put(itemEntry.getKey(), itemEntry.getValue());
			}
			return returnMap;
		}
	}

	@Override
	public void close() {
		kdbxDatabase = null;
		kdbxEntryIterator = null;
		super.close();
	}

	@Override
	public File filterDataItems(final List<Integer> indexList, final String fileSuffix) throws Exception {
		OutputStream outputStream = null;
		File tempFile = null;
		try {
			openKdbxDatabase();

			File filteredDataFile;
			if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".zip")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".kdbx.zip");
				outputStream = ZipUtilities.openNewZipOutputStream(filteredDataFile, null);
				((ZipOutputStream) outputStream).putNextEntry(new ZipEntry(new File(getImportFilePath() + "." + fileSuffix + ".kdbx").getName()));
			} else if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".tar.gz")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".kdbx.tar.gz");
				tempFile = File.createTempFile(new File(getImportFilePath()).getName(), fileSuffix);
				outputStream = new FileOutputStream(tempFile);
			} else if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".tgz")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".kdbx.tgz");
				tempFile = File.createTempFile(new File(getImportFilePath()).getName(), fileSuffix);
				outputStream = new FileOutputStream(tempFile);
			} else if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".gz")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".kdbx.gz");
				outputStream = new GZIPOutputStream(new FileOutputStream(filteredDataFile));
			} else {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".kdbx");
				outputStream = new FileOutputStream(filteredDataFile);
			}

			final KdbxDatabase filteredKdbxDatabase = new KdbxDatabase();
			int count = 0;
			for (final KdbxEntry entry : kdbxDatabase.getAllEntries()) {
				if (indexList.contains(count)) {
					filteredKdbxDatabase.getEntries().add(entry);
				}
				count++;
			}

			try (KdbxWriter kdbxWriter = new KdbxWriter(outputStream)) {
				kdbxWriter.writeKdbxDatabase(filteredKdbxDatabase, password);
			}

			if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".zip") && zipPassword != null) {
				Zip4jUtilities.createPasswordSecuredZipFile(filteredDataFile.getAbsolutePath(), zipPassword, false);
			} else if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".tar.gz")) {
				TarGzUtilities.compress(filteredDataFile, tempFile, new File(getImportFilePath()).getName() + "." + fileSuffix);
			} else if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".tgz")) {
				TarGzUtilities.compress(filteredDataFile, tempFile, new File(getImportFilePath()).getName() + "." + fileSuffix);
			}

			return filteredDataFile;
		} finally {
			close();
			Utilities.closeQuietly(outputStream);
			if (tempFile != null && tempFile.exists()) {
				tempFile.delete();
				tempFile = null;
			}
		}
	}

	private void openKdbxDatabase() throws Exception {
		if (kdbxEntryIterator != null) {
			throw new Exception("KdbxDatabase was already opened before");
		}

		try (final KdbxReader kdbxReader = new KdbxReader(getInputStream())) {
			kdbxDatabase = kdbxReader.readKdbxDatabase(password);
			kdbxEntryIterator = kdbxDatabase.getAllEntries().iterator();
		} catch (final Exception e) {
			close();
			throw e;
		}
	}
}
