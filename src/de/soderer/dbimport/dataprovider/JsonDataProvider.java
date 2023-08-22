package de.soderer.dbimport.dataprovider;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import de.soderer.dbimport.DbImportException;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.TarGzUtilities;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.DbColumnType;
import de.soderer.utilities.db.SimpleDataType;
import de.soderer.utilities.json.Json5Reader;
import de.soderer.utilities.json.JsonNode;
import de.soderer.utilities.json.JsonObject;
import de.soderer.utilities.json.JsonReader.JsonToken;
import de.soderer.utilities.json.JsonUtilities;
import de.soderer.utilities.json.JsonWriter;
import de.soderer.utilities.json.schema.JsonSchema;
import de.soderer.utilities.zip.Zip4jUtilities;
import de.soderer.utilities.zip.ZipUtilities;

public class JsonDataProvider extends DataProvider {
	private Json5Reader jsonReader = null;
	private List<String> dataPropertyNames = null;
	private final Map<String, DbColumnType> dataTypes = null;
	private Integer itemsAmount = null;
	private String dataPath = null;
	private String schemaFilePath = null;

	private final Charset encoding = StandardCharsets.UTF_8;

	public JsonDataProvider(final boolean isInlineData, final String importFilePathOrData, final char[] zipPassword, final String dataPath, final String schemaFilePath) {
		super(isInlineData, importFilePathOrData, zipPassword);
		this.dataPath = dataPath;
		this.schemaFilePath = schemaFilePath;
	}

	@Override
	public String getConfigurationLogString() {
		return super.getConfigurationLogString()
				+ "Format: JSON" + "\n"
				+ "Encoding: " + encoding + "\n";
	}

	@Override
	public Map<String, DbColumnType> scanDataPropertyTypes(final Map<String, Tuple<String, String>> mapping) throws Exception {
		if (dataTypes == null) {
			openReader();

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
			openReader();

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
		if (jsonReader == null) {
			openReader();
		}

		final JsonNode nextJsonNode = jsonReader.readNextJsonNode();

		if (nextJsonNode == null) {
			return null;
		} else {
			if (nextJsonNode.isJsonObject()) {
				final JsonObject nextJsonObject = (JsonObject) nextJsonNode.getValue();
				final Map<String, Object> returnMap = new HashMap<>();
				for (final String key : nextJsonObject.keySet()) {
					returnMap.put(key, nextJsonObject.get(key));
				}
				return returnMap;
			} else {
				throw new Exception("Invalid json data of type: " + nextJsonNode.getJsonDataType().getName());
			}
		}
	}

	@Override
	public void close() {
		Utilities.closeQuietly(jsonReader);
		jsonReader = null;
		super.close();
	}

	@Override
	public File filterDataItems(final List<Integer> indexList, final String fileSuffix) throws Exception {
		OutputStream outputStream = null;
		File tempFile = null;
		try {
			openReader();

			File filteredDataFile;
			if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".zip")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".json.zip");
				outputStream = ZipUtilities.openNewZipOutputStream(filteredDataFile, null);
				((ZipOutputStream) outputStream).putNextEntry(new ZipEntry(new File(getImportFilePath() + "." + fileSuffix + ".json").getName()));
			} else if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".tar.gz")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".json.tar.gz");
				tempFile = File.createTempFile(new File(getImportFilePath()).getName(), fileSuffix);
				outputStream = new FileOutputStream(tempFile);
			} else if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".tgz")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".json.tgz");
				tempFile = File.createTempFile(new File(getImportFilePath()).getName(), fileSuffix);
				outputStream = new FileOutputStream(tempFile);
			} else if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".gz")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".json.gz");
				outputStream = new GZIPOutputStream(new FileOutputStream(filteredDataFile));
			} else {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".json");
				outputStream = new FileOutputStream(filteredDataFile);
			}

			try (JsonWriter jsonWriter = new JsonWriter(outputStream, encoding)) {
				jsonWriter.openJsonArray();

				JsonNode nextJsonNode;
				int itemIndex = 0;
				while ((nextJsonNode = jsonReader.readNextJsonNode()) != null) {
					final JsonObject nextJsonObject = (JsonObject) nextJsonNode.getValue();
					itemIndex++;
					if (indexList.contains(itemIndex)) {
						jsonWriter.add(nextJsonObject);
					}
				}

				jsonWriter.closeJsonArray();
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

	private void openReader() throws Exception {
		if (jsonReader != null) {
			throw new Exception("Reader was already opened before");
		}

		try {
			if (Utilities.isNotEmpty(schemaFilePath)) {
				if (!new File(schemaFilePath).exists()) {
					throw new DbImportException("JSON-Schema file does not exist: " + schemaFilePath);
				} else if (new File(schemaFilePath).isDirectory()) {
					throw new DbImportException("JSON-Schema path is a directory: " + schemaFilePath);
				} else if (new File(schemaFilePath).length() == 0) {
					throw new DbImportException("JSON-Schema file is empty: " + schemaFilePath);
				}

				try (InputStream validationStream = getInputStream();
						InputStream schemaStream = new FileInputStream(new File(schemaFilePath))) {
					final JsonSchema schema = new JsonSchema(schemaStream);
					schema.validate(validationStream);
				} catch (final Exception e) {
					throw new Exception("JSON data does not comply to JSON schema '" + schemaFilePath + "': " + e.getMessage());
				}
			}

			jsonReader = new Json5Reader(getInputStream(), encoding);
			if (Utilities.isNotEmpty(dataPath)) {
				// Read JSON path
				JsonUtilities.readUpToJsonPath(jsonReader, dataPath);
				jsonReader.readNextToken();
				if (jsonReader.getCurrentToken() != JsonToken.JsonArray_Open) {
					throw new Exception("Invalid non-array json data for import at: " + dataPath);
				}
			} else {
				jsonReader.readNextToken();
				if (jsonReader.getCurrentToken() != JsonToken.JsonArray_Open) {
					throw new Exception("Invalid non-array json data for import");
				}
			}
		} catch (final Exception e) {
			close();
			throw e;
		}
	}
}
