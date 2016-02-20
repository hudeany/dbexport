package de.soderer.dbcsvimport.worker;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbColumnType;
import de.soderer.utilities.DbColumnType.SimpleDataType;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.WorkerParentSimple;
import de.soderer.utilities.ZipUtilities;
import de.soderer.utilities.json.Json5Reader;
import de.soderer.utilities.json.JsonObject;
import de.soderer.utilities.json.JsonReader.JsonToken;
import de.soderer.utilities.json.JsonWriter;

public class DbJsonImportWorker extends AbstractDbImportWorker {
	private Json5Reader jsonReader = null;
	private Map<String, DbColumnType> dataTypes = null;
	private Integer itemsAmount = null;
	
	public DbJsonImportWorker(WorkerParentSimple parent, DbVendor dbVendor, String hostname, String dbName, String username, String password, String tableName, String importFilePath) throws Exception {
		super(parent, dbVendor, hostname, dbName, username, password, tableName, importFilePath);
	}

	@Override
	public String getConfigurationLogString() {
		return
			"File: " + importFilePath + "\n"
			+ "Zip: " + Utilities.endsWithIgnoreCase(importFilePath, ".zip") + "\n"
			+ "Format: JSON" + "\n"
			+ "Encoding: " + encoding + "\n"
			+ "CommitOnFullSuccessOnly: " + commitOnFullSuccessOnly + "\n"
			+ "Table name: " + tableName + "\n"
			+ "Import file path: " + importFilePath + "\n"
			+ "Import mode: " + importMode + "\n"
			+ "Key columns: " + Utilities.join(keyColumns, ", ") + "\n"
			+ (createTableIfNotExists ? "New table was created: " + tableWasCreated + "\n" : "")
			+ "Mapping: " + convertMappingToString(mapping) + "\n"
			+ "Additional insert values: " + additionalInsertValues + "\n"
			+ "Additional update values: " + additionalUpdateValues + "\n"
			+ "Update with null values: " + updateWithNullValues + "\n";
	}
	
	@Override
	public Map<String, DbColumnType> scanDataPropertyTypes() throws Exception {
		if (dataTypes == null) {
			getAvailableDataPropertyNames();
		}
		
		return dataTypes;
	}



	@Override
	public List<String> getAvailableDataPropertyNames() throws Exception {
		if (dataTypes == null) {
			openReader();
			
			int itemCount = 0;
			dataTypes = new HashMap<String, DbColumnType>();
			
			Map<String, Object> nextItem;
			while ((nextItem = getNextItemData()) != null) {
				for (Entry<String, Object> itemProperty : nextItem.entrySet()) {
					String propertyName = itemProperty.getKey();
					Object propertyValue = itemProperty.getValue();
					
					String formatInfo = null;
					if (mapping != null) {
						for (Tuple<String, String> mappingValue : mapping.values()) {
							if (mappingValue.getFirst().equals(propertyName)) {
								if (formatInfo == null && Utilities.isNotBlank(mappingValue.getSecond())) {
									formatInfo = mappingValue.getSecond();
									break;
								}
							}
						}
					}
					
					SimpleDataType currentType = dataTypes.get(propertyName) == null ? null : dataTypes.get(propertyName).getSimpleDataType();
					if (currentType != SimpleDataType.Blob) {
						if (propertyValue == null) {
							dataTypes.put(propertyName, null);
						} else if ("file".equalsIgnoreCase(formatInfo) || (propertyValue instanceof String && ((String) propertyValue).length() > 4000)) {
							dataTypes.put(propertyName, new DbColumnType("BLOB", -1, -1, -1, true));
						} else if (currentType != SimpleDataType.String && Utilities.isNotBlank(formatInfo) && !formatInfo.equals(".") && !formatInfo.equals(",") && !formatInfo.equalsIgnoreCase("file") && propertyValue instanceof String) {
							String value = ((String) propertyValue).trim();
							try {
								new SimpleDateFormat(formatInfo).parse(value);
								dataTypes.put(propertyName, new DbColumnType("DATE", -1, -1, -1, true));
							} catch (Exception e1) {
								try {
									DateUtilities.ISO_8601_DATETIME_FORMAT.parse(value);
									dataTypes.put(propertyName, new DbColumnType("DATE", -1, -1, -1, true));
								} catch (Exception e2) {
									dataTypes.put(propertyName, new DbColumnType("VARCHAR", Math.max(dataTypes.get(propertyName) == null ? 0 : dataTypes.get(propertyName).getCharacterLength(), value.length()), -1, -1, true));
								}
							}
							dataTypes.put(propertyName, new DbColumnType("DATE", -1, -1, -1, true));
						} else if (currentType != SimpleDataType.String && currentType != SimpleDataType.Date && currentType != SimpleDataType.Double && propertyValue instanceof Integer) {
							dataTypes.put(propertyName, new DbColumnType("INTEGER", -1, -1, -1, true));
						} else if (currentType != SimpleDataType.String && currentType != SimpleDataType.Date && (propertyValue instanceof Float || propertyValue instanceof Double)) {
							dataTypes.put(propertyName, new DbColumnType("DOUBLE", -1, -1, -1, true));
						} else {
							dataTypes.put(propertyName, new DbColumnType("VARCHAR", Math.max(dataTypes.get(propertyName) == null ? 0 : dataTypes.get(propertyName).getCharacterLength(), propertyValue.toString().length()), -1, -1, true));
						}
					}
				}
				
				itemCount++;
			}
			
			itemsAmount = itemCount;
			
			closeReader();
		}
		
		return new ArrayList<String>(dataTypes.keySet());
	}

	@Override
	protected int getItemsAmountToImport() throws Exception {
		if (itemsAmount == null) {
			getAvailableDataPropertyNames();
		}
		return itemsAmount;
	}

	@Override
	protected void openReader() throws Exception {
		InputStream inputStream = null;
		try {
			inputStream = new FileInputStream(new File(importFilePath));
			if (Utilities.endsWithIgnoreCase(importFilePath, ".zip")) {
				inputStream = new ZipInputStream(inputStream);
				((ZipInputStream) inputStream).getNextEntry();
			}
			jsonReader = new Json5Reader(inputStream, encoding);
			JsonToken startToken = jsonReader.readNextToken();
			if (startToken != JsonToken.JsonArray_Open) {
				throw new Exception("Invalid json data for import starting with: " + startToken.toString());
			}
		} catch (Exception e) {
			Utilities.closeQuietly(jsonReader);
			Utilities.closeQuietly(inputStream);
		}
	}

	@Override
	protected Map<String, Object> getNextItemData() throws Exception {
		if (!jsonReader.readNextJsonItem()) {
			return null;
		} else {
			Object nextObject = jsonReader.getCurrentObject();
			if (nextObject instanceof JsonObject) {
				JsonObject nextJsonObject = (JsonObject) nextObject;
				Map<String, Object> returnMap = new HashMap<String, Object>();
				for (String key : nextJsonObject.keySet()) {
					returnMap.put(key, nextJsonObject.get(key));
				}
				return returnMap;
			} else {
				throw new Exception("Invalid json data of type: " + nextObject.getClass().getName());
			}
		}
	}

	@Override
	protected void closeReader() throws Exception {
		Utilities.closeQuietly(jsonReader);
	}

	@Override
	protected File filterDataItems(List<Integer> indexList, String fileSuffix) throws Exception {
		OutputStream outputStream = null;
		JsonWriter jsonWriter = null;
		try {
			openReader();
			
			File filteredDataFile;
			if (Utilities.endsWithIgnoreCase(importFilePath, ".zip")) {
				filteredDataFile = new File(importFilePath + "." + fileSuffix + ".json.zip");
				outputStream = ZipUtilities.openNewZipOutputStream(filteredDataFile);
				((ZipOutputStream) outputStream).putNextEntry(new ZipEntry(new File(importFilePath + "." + fileSuffix + ".json").getName()));
			} else {
				filteredDataFile = new File(importFilePath + "." + fileSuffix + ".json");
				outputStream = new FileOutputStream(filteredDataFile);
			}
			
			jsonWriter = new JsonWriter(outputStream, encoding);
			jsonWriter.openJsonArray();
			
			Map<String, Object> item;
			int itemIndex = 0;
			while ((item = getNextItemData()) != null) {
				itemIndex++;
				if (indexList.contains(itemIndex)) {
					JsonObject filteredObject = new JsonObject();
					for (Entry<String, Object> entry : item.entrySet()) {
						filteredObject.add(entry.getKey(), entry.getValue());
					}
					
					jsonWriter.add(filteredObject);
				}
			}
			
			jsonWriter.closeJsonArray();
			
			return filteredDataFile;
		} finally {
			closeReader();
			Utilities.closeQuietly(jsonWriter);
			Utilities.closeQuietly(outputStream);
		}
	}
}
