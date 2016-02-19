package de.soderer.dbcsvimport.worker;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.ZipInputStream;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbColumnType.SimpleDataType;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.WorkerParentSimple;
import de.soderer.utilities.json.Json5Reader;
import de.soderer.utilities.json.JsonObject;
import de.soderer.utilities.json.JsonReader.JsonToken;

public class DbJsonImportWorker extends AbstractDbImportWorker {
	private Json5Reader jsonReader = null;
	private Map<String, SimpleDataType> dataTypes = null;
	private Integer itemsAmount = null;
	
	public DbJsonImportWorker(WorkerParentSimple parent, DbVendor dbVendor, String hostname, String dbName, String username, String password, String tableName, String importFilePath) throws Exception {
		super(parent, dbVendor, hostname, dbName, username, password, tableName, importFilePath);
	}

	@Override
	public String getConfigurationLogString() {
		return
			"File: " + importFilePath
			+ "Zip: " + Utilities.endsWithIgnoreCase(importFilePath, ".zip")
			+ "Format: JSON"
			+ "Encoding: " + encoding
			+ "CommitOnFullSuccessOnly: " + commitOnFullSuccessOnly
			+ "Table name: " + tableName
			+ "Import file path: " + importFilePath;
	}
	
	@Override
	public Map<String, SimpleDataType> scanDataPropertyTypes() throws Exception {
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
			dataTypes = new HashMap<String, SimpleDataType>();
			
			Map<String, Object> nextItem;
			while ((nextItem = getNextItemData()) != null) {
				for (Entry<String, Object> itemProperty : nextItem.entrySet()) {
					String propertyName = itemProperty.getKey();
					Object propertyValue = itemProperty.getValue();
					
					String formatInfo = null;
					for (Tuple<String, String> mappingValue : mapping.values()) {
						if (mappingValue.getFirst().equals(propertyName)) {
							if (formatInfo == null && Utilities.isNotBlank(mappingValue.getSecond())) {
								formatInfo = mappingValue.getSecond();
								break;
							}
						}
					}
					
					SimpleDataType currentType = dataTypes.get(propertyName);
					if (currentType != SimpleDataType.Blob) {
						if (propertyValue == null) {
							dataTypes.put(propertyName, null);
						} else if ("file".equalsIgnoreCase(formatInfo) || (propertyValue instanceof String && ((String) propertyValue).length() > 4000)) {
							dataTypes.put(propertyName, SimpleDataType.Blob);
						} else if (currentType != SimpleDataType.String && Utilities.isNotBlank(formatInfo) && !formatInfo.equals(".") && !formatInfo.equals(",") && !formatInfo.equalsIgnoreCase("file") && propertyValue instanceof String) {
							String value = ((String) propertyValue).trim();
							try {
								new SimpleDateFormat(formatInfo).parse(value);
								dataTypes.put(propertyName, SimpleDataType.Date);
							} catch (Exception e1) {
								try {
									DateUtilities.ISO_8601_DATETIME_FORMAT.parse(value);
									dataTypes.put(propertyName, SimpleDataType.Date);
								} catch (Exception e2) {
									dataTypes.put(propertyName, SimpleDataType.String);
								}
							}
							dataTypes.put(propertyName, SimpleDataType.Date);
						} else if (currentType != SimpleDataType.String && currentType != SimpleDataType.Date && currentType != SimpleDataType.Double && propertyValue instanceof Integer) {
							dataTypes.put(propertyName, SimpleDataType.Integer);
						} else if (currentType != SimpleDataType.String && currentType != SimpleDataType.Date && (propertyValue instanceof Float || propertyValue instanceof Double)) {
							dataTypes.put(propertyName, SimpleDataType.Double);
						} else {
							dataTypes.put(propertyName, SimpleDataType.String);
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
}
