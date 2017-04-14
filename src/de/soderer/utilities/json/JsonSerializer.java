package de.soderer.utilities.json;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import de.soderer.utilities.Utilities;

public class JsonSerializer {
	/**
	 * Serialize an object in json data<br />
	 * - Serializes null values<br />
	 * - Excludes static fields<br />
	 * - Excludes transient fields<br />
	 * - Does not show object type infos<br />
	 * 
	 * @param dataObject
	 * @return
	 * @throws Exception
	 */
	public static JsonNode serialize(Object dataObject) throws Exception {
		return serializeInternal(dataObject, false, false, false, false, new ArrayList<Object>());
	}
	
	/**
	 * Serialize an object in json data
	 * 
	 * @param dataObject
	 * @param excludeNull Do not serialize null values
	 * @param includeStatic Serialize fields with a "static" modifier
	 * @param includeTransient Serialize fields with a "transient" modifier
	 * @param addObjectTypeInfo Add object type info
	 * @return
	 * @throws Exception
	 */
	public static JsonNode serialize(Object dataObject, boolean excludeNull, boolean includeStatic, boolean includeTransient, boolean addObjectTypeInfo) throws Exception {
		return serializeInternal(dataObject, excludeNull, includeStatic, includeTransient, addObjectTypeInfo, new ArrayList<Object>());
	}
		
	private static JsonNode serializeInternal(Object dataObject, boolean excludeNull, boolean includeStatic, boolean includeTransient, boolean addObjectTypeInfo, List<Object> alreadyVisitedObjects) throws Exception {
		if (dataObject == null) {
			if (excludeNull) {
				// This may only occur on top level of data
				return null;
			} else if (addObjectTypeInfo) {
		    	JsonObject jsonObjectWithTypeInfo = new JsonObject();
		    	jsonObjectWithTypeInfo.add("class", null);
		    	jsonObjectWithTypeInfo.add("value", null);
		    	return new JsonNode(jsonObjectWithTypeInfo);
		    } else {
		    	return new JsonNode(dataObject);
		    }
		} else if (dataObject instanceof Number
				|| dataObject instanceof String
				|| dataObject instanceof Character
				|| dataObject instanceof Date
				|| dataObject instanceof Boolean) {
			if (addObjectTypeInfo) {
		    	JsonObject jsonObjectWithTypeInfo = new JsonObject();
		    	jsonObjectWithTypeInfo.add("class", dataObject.getClass().getName());
		    	jsonObjectWithTypeInfo.add("value", dataObject);
		    	return new JsonNode(jsonObjectWithTypeInfo);
		    } else {
		    	return new JsonNode(dataObject);
		    }
		} else if (dataObject.getClass().isArray()) {
			if (Utilities.containsObject(alreadyVisitedObjects, dataObject)) {
				throw new Exception("Cyclic reference detected. Cannot serialize object: " + dataObject.getClass() + " (Hashcode " + System.identityHashCode(dataObject) + ")");
			} else {
				alreadyVisitedObjects.add(dataObject);
			}
		
			JsonArray jsonArray = new JsonArray();
			int length = Array.getLength(dataObject);
		    for (int i = 0; i < length; i ++) {
		        Object item = Array.get(dataObject, i);
		        if (item != null || !excludeNull) {
		        	jsonArray.add(serializeInternal(item, excludeNull, includeStatic, includeTransient, addObjectTypeInfo, alreadyVisitedObjects).getValue());
		        }
		    }
		    
		    alreadyVisitedObjects.remove(dataObject);
		    
		    if (addObjectTypeInfo) {
		    	JsonObject jsonObjectWithTypeInfo = new JsonObject();
		    	jsonObjectWithTypeInfo.add("class", dataObject.getClass().getName());
		    	jsonObjectWithTypeInfo.add("value", jsonArray);
		    	return new JsonNode(jsonObjectWithTypeInfo);
		    } else {
		    	return new JsonNode(jsonArray);
		    }
		} else if (dataObject instanceof Iterable<?>) {
			if (Utilities.containsObject(alreadyVisitedObjects, dataObject)) {
				throw new Exception("Cyclic reference detected. Cannot serialize object: " + dataObject.getClass() + " (Hashcode " + System.identityHashCode(dataObject) + ")");
			} else {
				alreadyVisitedObjects.add(dataObject);
			}
			
			JsonArray jsonArray = new JsonArray();
			for (Object item : (Iterable<?>) dataObject) {
				if (item != null || !excludeNull) {
					jsonArray.add(serializeInternal(item, excludeNull, includeStatic, includeTransient, addObjectTypeInfo, alreadyVisitedObjects).getValue());
				}
			}
		    
		    alreadyVisitedObjects.remove(dataObject);
		    
		    if (addObjectTypeInfo) {
		    	JsonObject jsonObjectWithTypeInfo = new JsonObject();
		    	jsonObjectWithTypeInfo.add("class", dataObject.getClass().getName());
		    	jsonObjectWithTypeInfo.add("value", jsonArray);
		    	return new JsonNode(jsonObjectWithTypeInfo);
		    } else {
		    	return new JsonNode(jsonArray);
		    }
		} else {
			if (Utilities.containsObject(alreadyVisitedObjects, dataObject)) {
				throw new Exception("Cyclic reference detected. Cannot serialize object: " + dataObject.getClass() + " (Hashcode " + System.identityHashCode(dataObject) + ")");
			} else {
				alreadyVisitedObjects.add(dataObject);
			}
			
			JsonObject dataJsonObject = new JsonObject();
			Class<?> dataClass = dataObject.getClass();
			for (Field dataField : dataClass.getDeclaredFields()) {
				boolean serializeField = true;
				if ((dataField.getModifiers() & Modifier.TRANSIENT) == Modifier.TRANSIENT && !includeTransient) {
					serializeField = false;
				} else if ((dataField.getModifiers() & Modifier.STATIC) == Modifier.STATIC && !includeStatic) {
					serializeField = false;
				}
				
				if (serializeField) {
					dataField.setAccessible(true);
					String fieldName = dataField.getName();
					Object fieldData = dataField.get(dataObject);
					if (fieldData != null || !excludeNull) {
						if (fieldData == null && addObjectTypeInfo) {
							JsonObject jsonObjectWithTypeInfo = new JsonObject();
					    	jsonObjectWithTypeInfo.add("class", dataField.getType().getName());
					    	jsonObjectWithTypeInfo.add("value", null);
							dataJsonObject.add(fieldName, jsonObjectWithTypeInfo);
						} else {
							dataJsonObject.add(fieldName, serializeInternal(fieldData, excludeNull, includeStatic, includeTransient, addObjectTypeInfo, alreadyVisitedObjects).getValue());
						}
					}
				}
			}
		    
		    alreadyVisitedObjects.remove(dataObject);
		    
		    if (addObjectTypeInfo) {
		    	JsonObject jsonObjectWithTypeInfo = new JsonObject();
		    	jsonObjectWithTypeInfo.add("class", dataObject.getClass().getName());
		    	jsonObjectWithTypeInfo.add("value", dataJsonObject);
		    	return new JsonNode(jsonObjectWithTypeInfo);
		    } else {
		    	return new JsonNode(dataJsonObject);
		    }
		}
	}
	
	public static Object deserialize(JsonNode jsonData) throws Exception {
		if (jsonData == null) {
			throw new Exception("JSON data is null");
		} else if (jsonData.isJsonObject()) {
			return deserialize((JsonObject) jsonData.getValue());
		} else {
			throw new Exception("JSON data is not an object");
		}
	}
	
	public static Object deserialize(JsonObject jsonObject) throws Exception {
		if (jsonObject == null) {
			throw new Exception("JSON object is null");
		} else {
			if (!jsonObject.containsPropertyKey("class")) {
				throw new Exception("JSON object is missing mandatory type information");
			} else if (jsonObject.get("class") == null) {
				// Null value that has no class info
				return null;
			} else if (!(jsonObject.get("class") instanceof String)) {
				throw new Exception("JSON object has invalid type information");
			} else if (!jsonObject.containsPropertyKey("value")) {
				throw new Exception("JSON object is missing mandatory value information");
			}
			
			Object value = jsonObject.get("value");
			if (value == null || value instanceof String || value instanceof Character || value instanceof Boolean || value instanceof Number) {
				return value;
			} else if (jsonObject.get("class") == null || !(jsonObject.get("class") instanceof String)) {
				throw new Exception("Invalid class type serialization value");
			} else if (value instanceof JsonObject) {
				String className = (String) jsonObject.get("class");
				Class<?> clazz = Class.forName(className);
				Object object = clazz.newInstance();
				for (Entry<String, Object> entry : ((JsonObject) value).entrySet()) {
					if (!(entry.getValue() instanceof JsonObject)) {
						throw new Exception("Invalid value type serialization value");
					}
					
					Field field;
					try {
						field = clazz.getDeclaredField(entry.getKey());
					} catch (Exception e) {
						throw new Exception("Invalid field name serialization value");
					}
			        field.setAccessible(true);
					field.set(object, deserialize((JsonObject) entry.getValue()));
				}
				return object;
			} else if (value instanceof JsonArray) {
				String className = (String) jsonObject.get("class");
				Class<?> clazz = Class.forName(className);
				List<Object> listOfItems = new ArrayList<Object>();
				for (Object item : (JsonArray) value) {
					if (!(item instanceof JsonObject)) {
						throw new Exception("Invalid value type serialization value");
					}
					
					listOfItems.add(deserialize((JsonObject) item));
				}
				
				if (clazz.isArray()) {
					Object[] array = (Object[]) Array.newInstance(clazz.getComponentType(), listOfItems.size());
					return listOfItems.toArray(array);
				} else {
					Object object = clazz.newInstance();
					if (object instanceof Collection) {
						for (Object item : listOfItems) {
							@SuppressWarnings("unchecked")
							Collection<Object> collectionObject = (Collection<Object>) object;
							collectionObject.add(item);
						}
						return object;
					} else {
						throw new Exception("");
					}
				}
			} else {
				throw new Exception("");
			}
		}
	}
}
