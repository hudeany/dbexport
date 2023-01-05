package de.soderer.utilities.json;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import de.soderer.utilities.ClassUtilities;
import de.soderer.utilities.DateUtilities;
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
	public static JsonNode serialize(final Object dataObject) throws Exception {
		return serializeInternal(dataObject, false, false, false, false, new ArrayList<>());
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
	public static JsonNode serialize(final Object dataObject, final boolean excludeNull, final boolean includeStatic, final boolean includeTransient, final boolean addObjectTypeInfo) throws Exception {
		return serializeInternal(dataObject, excludeNull, includeStatic, includeTransient, addObjectTypeInfo, new ArrayList<>());
	}

	private static JsonNode serializeInternal(final Object dataObject, final boolean excludeNull, final boolean includeStatic, final boolean includeTransient, final boolean addObjectTypeInfo, final List<Object> alreadyVisitedObjects) throws Exception {
		if (dataObject == null) {
			if (excludeNull) {
				// This may only occur on top level of data
				return null;
			} else if (addObjectTypeInfo) {
				final JsonObject jsonObjectWithTypeInfo = new JsonObject();
				jsonObjectWithTypeInfo.add("class", null);
				jsonObjectWithTypeInfo.add("value", null);
				return new JsonNode(jsonObjectWithTypeInfo);
			} else {
				return new JsonNode(dataObject);
			}
		} else if (dataObject instanceof Number
				|| dataObject instanceof String
				|| dataObject instanceof Character
				|| dataObject instanceof Boolean) {
			if (addObjectTypeInfo) {
				final JsonObject jsonObjectWithTypeInfo = new JsonObject();
				jsonObjectWithTypeInfo.add("class", dataObject.getClass().getName());
				jsonObjectWithTypeInfo.add("value", dataObject);
				return new JsonNode(jsonObjectWithTypeInfo);
			} else {
				return new JsonNode(dataObject);
			}
		} else if (dataObject instanceof Charset) {
			if (addObjectTypeInfo) {
				final JsonObject jsonObjectWithTypeInfo = new JsonObject();
				jsonObjectWithTypeInfo.add("class", Charset.class.getName());
				jsonObjectWithTypeInfo.add("value", ((Charset) dataObject).toString());
				return new JsonNode(jsonObjectWithTypeInfo);
			} else {
				return new JsonNode(dataObject);
			}
		} else if (dataObject instanceof Date) {
			if (addObjectTypeInfo) {
				final JsonObject jsonObjectWithTypeInfo = new JsonObject();
				jsonObjectWithTypeInfo.add("class", dataObject.getClass().getName());
				jsonObjectWithTypeInfo.add("value", dataObject);
				return new JsonNode(jsonObjectWithTypeInfo);
			} else {
				return new JsonNode(DateUtilities.formatDate(DateUtilities.ISO_8601_DATETIME_FORMAT, (Date) dataObject));
			}
		} else if (dataObject instanceof LocalDateTime) {
			if (addObjectTypeInfo) {
				final JsonObject jsonObjectWithTypeInfo = new JsonObject();
				jsonObjectWithTypeInfo.add("class", dataObject.getClass().getName());
				jsonObjectWithTypeInfo.add("value", dataObject);
				return new JsonNode(jsonObjectWithTypeInfo);
			} else {
				return new JsonNode(DateUtilities.formatDate(DateUtilities.ISO_8601_DATETIME_FORMAT_NO_TIMEZONE, (LocalDateTime) dataObject));
			}
		} else if (dataObject instanceof LocalDate) {
			if (addObjectTypeInfo) {
				final JsonObject jsonObjectWithTypeInfo = new JsonObject();
				jsonObjectWithTypeInfo.add("class", dataObject.getClass().getName());
				jsonObjectWithTypeInfo.add("value", dataObject);
				return new JsonNode(jsonObjectWithTypeInfo);
			} else {
				return new JsonNode(DateUtilities.formatDate(DateUtilities.ISO_8601_DATE_FORMAT_NO_TIMEZONE, (LocalDate) dataObject));
			}
		} else if (dataObject instanceof ZonedDateTime) {
			if (addObjectTypeInfo) {
				final JsonObject jsonObjectWithTypeInfo = new JsonObject();
				jsonObjectWithTypeInfo.add("class", dataObject.getClass().getName());
				jsonObjectWithTypeInfo.add("value", dataObject);
				return new JsonNode(jsonObjectWithTypeInfo);
			} else {
				return new JsonNode(DateUtilities.formatDate(DateUtilities.ISO_8601_DATETIME_FORMAT, (ZonedDateTime) dataObject));
			}
		} else if (dataObject instanceof Enum) {
			if (addObjectTypeInfo) {
				final JsonObject jsonObjectWithTypeInfo = new JsonObject();
				jsonObjectWithTypeInfo.add("class", dataObject.getClass().getName());
				jsonObjectWithTypeInfo.add("value", dataObject.toString());
				return new JsonNode(jsonObjectWithTypeInfo);
			} else {
				return new JsonNode(dataObject.toString());
			}
		} else if (dataObject.getClass().isArray()) {
			if (Utilities.containsObject(alreadyVisitedObjects, dataObject)) {
				throw new Exception("Cyclic reference detected. Cannot serialize object: " + dataObject.getClass() + " (Hashcode " + System.identityHashCode(dataObject) + ")");
			} else {
				// the same object may be included multiple times in parent objects, but not in child objects
				alreadyVisitedObjects.add(dataObject);

				final JsonArray jsonArray = new JsonArray();
				final int length = Array.getLength(dataObject);
				for (int i = 0; i < length; i ++) {
					final Object item = Array.get(dataObject, i);
					if (item != null || !excludeNull) {
						jsonArray.add(serializeInternal(item, excludeNull, includeStatic, includeTransient, addObjectTypeInfo, alreadyVisitedObjects).getValue());
					}
				}

				// the same object may be included multiple times in parent objects
				alreadyVisitedObjects.remove(dataObject);

				if (addObjectTypeInfo) {
					final JsonObject jsonObjectWithTypeInfo = new JsonObject();
					jsonObjectWithTypeInfo.add("class", dataObject.getClass().getName());
					jsonObjectWithTypeInfo.add("value", jsonArray);
					return new JsonNode(jsonObjectWithTypeInfo);
				} else {
					return new JsonNode(jsonArray);
				}
			}
		} else if (dataObject instanceof Iterable<?>) {
			if (Utilities.containsObject(alreadyVisitedObjects, dataObject)) {
				throw new Exception("Cyclic reference detected. Cannot serialize object: " + dataObject.getClass() + " (Hashcode " + System.identityHashCode(dataObject) + ")");
			} else {
				// the same object may be included multiple times in parent objects, but not in child objects
				alreadyVisitedObjects.add(dataObject);

				final JsonArray jsonArray = new JsonArray();
				for (final Object item : (Iterable<?>) dataObject) {
					if (item != null || !excludeNull) {
						jsonArray.add(serializeInternal(item, excludeNull, includeStatic, includeTransient, addObjectTypeInfo, alreadyVisitedObjects).getValue());
					}
				}

				// the same object may be included multiple times in parent objects
				alreadyVisitedObjects.remove(dataObject);

				if (addObjectTypeInfo) {
					final JsonObject jsonObjectWithTypeInfo = new JsonObject();
					jsonObjectWithTypeInfo.add("class", dataObject.getClass().getName());
					jsonObjectWithTypeInfo.add("value", jsonArray);
					return new JsonNode(jsonObjectWithTypeInfo);
				} else {
					return new JsonNode(jsonArray);
				}
			}
		} else if (dataObject instanceof Map<?, ?>) {
			if (Utilities.containsObject(alreadyVisitedObjects, dataObject)) {
				throw new Exception("Cyclic reference detected. Cannot serialize object: " + dataObject.getClass() + " (Hashcode " + System.identityHashCode(dataObject) + ")");
			} else {
				// the same object may be included multiple times in parent objects, but not in child objects
				alreadyVisitedObjects.add(dataObject);

				final JsonArray jsonArray = new JsonArray();
				for (final Entry<?, ?> entry : ((Map<?, ?>) dataObject).entrySet()) {
					final JsonObject jsonObject = new JsonObject();

					if (entry.getKey() == null) {
						jsonObject.add("key", null);
					} else {
						jsonObject.add("key", serializeInternal(entry.getKey(), excludeNull, includeStatic, includeTransient, addObjectTypeInfo, alreadyVisitedObjects).getValue());
					}

					if (entry.getValue() == null) {
						jsonObject.add("value", null);
					} else {
						jsonObject.add("value", serializeInternal(entry.getValue(), excludeNull, includeStatic, includeTransient, addObjectTypeInfo, alreadyVisitedObjects).getValue());
					}

					jsonArray.add(jsonObject);
				}

				// the same object may be included multiple times in parent objects
				alreadyVisitedObjects.remove(dataObject);

				if (addObjectTypeInfo) {
					final JsonObject jsonObjectWithTypeInfo = new JsonObject();
					jsonObjectWithTypeInfo.add("class", dataObject.getClass().getName());
					jsonObjectWithTypeInfo.add("value", jsonArray);
					return new JsonNode(jsonObjectWithTypeInfo);
				} else {
					return new JsonNode(jsonArray);
				}
			}
		} else {
			if (Utilities.containsObject(alreadyVisitedObjects, dataObject)) {
				throw new Exception("Cyclic reference detected. Cannot serialize object: " + dataObject.getClass() + " (Hashcode " + System.identityHashCode(dataObject) + ")");
			} else {
				// the same object may be included multiple times in parent objects, but not in child objects
				alreadyVisitedObjects.add(dataObject);

				final JsonObject dataJsonObject = new JsonObject();
				final Class<?> dataClass = dataObject.getClass();
				for (final Field dataField : ClassUtilities.getAllFields(dataClass)) {
					boolean serializeField = true;
					if ((dataField.getModifiers() & Modifier.TRANSIENT) == Modifier.TRANSIENT && !includeTransient) {
						serializeField = false;
					} else if ((dataField.getModifiers() & Modifier.STATIC) == Modifier.STATIC && !includeStatic) {
						serializeField = false;
					}

					if (serializeField) {
						dataField.setAccessible(true);
						final String fieldName = dataField.getName();
						final Object fieldData = dataField.get(dataObject);
						if (fieldData != null || !excludeNull) {
							if (fieldData == null && addObjectTypeInfo) {
								final JsonObject jsonObjectWithTypeInfo = new JsonObject();
								jsonObjectWithTypeInfo.add("class", dataField.getType().getName());
								jsonObjectWithTypeInfo.add("value", null);
								dataJsonObject.add(fieldName, jsonObjectWithTypeInfo);
							} else {
								dataJsonObject.add(fieldName, serializeInternal(fieldData, excludeNull, includeStatic, includeTransient, addObjectTypeInfo, alreadyVisitedObjects).getValue());
							}
						}
					}
				}

				// the same object may be included multiple times in parent objects
				alreadyVisitedObjects.remove(dataObject);

				if (addObjectTypeInfo) {
					final JsonObject jsonObjectWithTypeInfo = new JsonObject();
					jsonObjectWithTypeInfo.add("class", dataObject.getClass().getName());
					jsonObjectWithTypeInfo.add("value", dataJsonObject);
					return new JsonNode(jsonObjectWithTypeInfo);
				} else {
					return new JsonNode(dataJsonObject);
				}
			}
		}
	}

	public static Object deserialize(final JsonNode jsonData) throws Exception {
		if (jsonData == null) {
			throw new Exception("JSON data is null");
		} else if (jsonData.isJsonObject()) {
			return deserialize((JsonObject) jsonData.getValue());
		} else {
			throw new Exception("JSON data is not an object");
		}
	}

	public static Object deserialize(final JsonObject jsonObject) throws Exception {
		try {
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

				final Object value = jsonObject.get("value");
				final Class<?> clazz = Class.forName((String) jsonObject.get("class"));
				if (value == null) {
					return null;
				} else if (clazz == Boolean.TYPE || clazz == Boolean.class) {
					return value;
				} else if (clazz == Byte.TYPE || clazz == Byte.class) {
					return ((Number) value).byteValue();
				} else if (clazz == Short.TYPE || clazz == Short.class) {
					return ((Number) value).shortValue();
				} else if (clazz == Integer.TYPE || clazz == Integer.class) {
					return ((Number) value).intValue();
				} else if (clazz == Long.TYPE || clazz == Long.class) {
					return ((Number) value).longValue();
				} else if (clazz == Float.TYPE || clazz == Float.class) {
					return ((Number) value).floatValue();
				} else if (clazz == Double.TYPE || clazz == Double.class) {
					if (value.getClass() == Float.class) {
						return Double.parseDouble(Float.toString((Float) value));
					} else {
						return ((Number) value).doubleValue();
					}
				} else if (clazz == BigDecimal.class) {
					return value;
				} else if (clazz == Character.TYPE || clazz == Character.class) {
					if (value instanceof Character) {
						return value;
					} else {
						return ((String) value).charAt(0);
					}
				} else if (clazz == String.class) {
					return value;
				} else if (clazz == Charset.class) {
					return Charset.forName((String) value);
				} else if (clazz.isEnum()) {
					final String enumName = (String) value;
					for (final Object enumConstant : clazz.getEnumConstants()) {
						if (enumName.equals(enumConstant.toString())) {
							return enumConstant;
						}
					}
					throw new Exception("Invalid enum name '" + enumName + "' for type '" + clazz.getName() + "'");
				} else if (Date.class.isAssignableFrom(clazz)) {
					return DateUtilities.getDateForZonedDateTime(DateUtilities.parseIso8601DateTimeString((String) value));
				} else if (LocalDateTime.class.isAssignableFrom(clazz)) {
					return DateUtilities.parseIso8601DateTimeString((String) value).toLocalDateTime();
				} else if (LocalDate.class.isAssignableFrom(clazz)) {
					return DateUtilities.parseIso8601DateTimeString((String) value).toLocalDate();
				} else if (ZonedDateTime.class.isAssignableFrom(clazz)) {
					return DateUtilities.parseIso8601DateTimeString((String) value);
				} else if (value instanceof JsonObject) {
					Object object;
					final Constructor<?> constructor = ClassUtilities.getConstructor(clazz);
					constructor.setAccessible(true);
					object = constructor.newInstance();

					for (final Entry<String, Object> entry : ((JsonObject) value).entrySet()) {
						if (!(entry.getValue() instanceof JsonObject)) {
							throw new Exception("Invalid value type serialization value");
						}

						Field field;
						try {
							field = ClassUtilities.getField(clazz, entry.getKey());
						} catch (@SuppressWarnings("unused") final Exception e) {
							throw new Exception("Invalid field name serialization value");
						}
						field.setAccessible(true);
						field.set(object, deserialize((JsonObject) entry.getValue()));
					}
					return object;
				} else if (value instanceof JsonArray) {
					final JsonArray jsonArray = (JsonArray) value;
					if (clazz.isArray()) {
						if (clazz.getComponentType() == Boolean.TYPE) {
							final boolean[] arrayValue = new boolean[jsonArray.size()];
							for (int i = 0; i < arrayValue.length; i++) {
								arrayValue[i] = (boolean) deserialize((JsonObject) jsonArray.get(i));
							}
							return arrayValue;
						} else if (clazz.getComponentType() == Boolean.class) {
							final Boolean[] arrayValue = new Boolean[jsonArray.size()];
							for (int i = 0; i < arrayValue.length; i++) {
								arrayValue[i] = (Boolean) deserialize((JsonObject) jsonArray.get(i));
							}
							return arrayValue;
						} else if (clazz.getComponentType() == Byte.TYPE) {
							final byte[] arrayValue = new byte[jsonArray.size()];
							for (int i = 0; i < arrayValue.length; i++) {
								arrayValue[i] = (byte) deserialize((JsonObject) jsonArray.get(i));
							}
							return arrayValue;
						} else if (clazz.getComponentType() == Byte.class) {
							final Byte[] arrayValue = new Byte[jsonArray.size()];
							for (int i = 0; i < arrayValue.length; i++) {
								arrayValue[i] = (Byte) deserialize((JsonObject) jsonArray.get(i));
							}
							return arrayValue;
						} else if (clazz.getComponentType() == Short.TYPE) {
							final short[] arrayValue = new short[jsonArray.size()];
							for (int i = 0; i < arrayValue.length; i++) {
								arrayValue[i] = (short) deserialize((JsonObject) jsonArray.get(i));
							}
							return arrayValue;
						} else if (clazz.getComponentType() == Short.class) {
							final Short[] arrayValue = new Short[jsonArray.size()];
							for (int i = 0; i < arrayValue.length; i++) {
								arrayValue[i] = (Short) deserialize((JsonObject) jsonArray.get(i));
							}
							return arrayValue;
						} else if (clazz.getComponentType() == Integer.TYPE) {
							final int[] arrayValue = new int[jsonArray.size()];
							for (int i = 0; i < arrayValue.length; i++) {
								arrayValue[i] = (int) deserialize((JsonObject) jsonArray.get(i));
							}
							return arrayValue;
						} else if (clazz.getComponentType() == Integer.class) {
							final Integer[] arrayValue = new Integer[jsonArray.size()];
							for (int i = 0; i < arrayValue.length; i++) {
								arrayValue[i] = (Integer) deserialize((JsonObject) jsonArray.get(i));
							}
							return arrayValue;
						} else if (clazz.getComponentType() == Long.TYPE) {
							final long[] arrayValue = new long[jsonArray.size()];
							for (int i = 0; i < arrayValue.length; i++) {
								arrayValue[i] = (long) deserialize((JsonObject) jsonArray.get(i));
							}
							return arrayValue;
						} else if (clazz.getComponentType() == Long.class) {
							final Long[] arrayValue = new Long[jsonArray.size()];
							for (int i = 0; i < arrayValue.length; i++) {
								arrayValue[i] = (Long) deserialize((JsonObject) jsonArray.get(i));
							}
							return arrayValue;
						} else if (clazz.getComponentType() == Float.TYPE) {
							final float[] arrayValue = new float[jsonArray.size()];
							for (int i = 0; i < arrayValue.length; i++) {
								arrayValue[i] = (float) deserialize((JsonObject) jsonArray.get(i));
							}
							return arrayValue;
						} else if (clazz.getComponentType() == Float.class) {
							final Float[] arrayValue = new Float[jsonArray.size()];
							for (int i = 0; i < arrayValue.length; i++) {
								arrayValue[i] = (Float) deserialize((JsonObject) jsonArray.get(i));
							}
							return arrayValue;
						} else if (clazz.getComponentType() == Double.TYPE) {
							final double[] arrayValue = new double[jsonArray.size()];
							for (int i = 0; i < arrayValue.length; i++) {
								arrayValue[i] = (double) deserialize((JsonObject) jsonArray.get(i));
							}
							return arrayValue;
						} else if (clazz.getComponentType() == Double.class) {
							final Double[] arrayValue = new Double[jsonArray.size()];
							for (int i = 0; i < arrayValue.length; i++) {
								arrayValue[i] = (Double) deserialize((JsonObject) jsonArray.get(i));
							}
							return arrayValue;
						} else if (clazz.getComponentType() == Character.TYPE) {
							final char[] arrayValue = new char[jsonArray.size()];
							for (int i = 0; i < arrayValue.length; i++) {
								arrayValue[i] = (char) deserialize((JsonObject) jsonArray.get(i));
							}
							return arrayValue;
						} else if (clazz.getComponentType() == Character.class) {
							final Character[] arrayValue = new Character[jsonArray.size()];
							for (int i = 0; i < arrayValue.length; i++) {
								arrayValue[i] = (Character) deserialize((JsonObject) jsonArray.get(i));
							}
							return arrayValue;
						} else {
							final Object[] array = (Object[]) Array.newInstance(clazz.getComponentType(), jsonArray.size());
							for (int i = 0; i < array.length; i++) {
								array[i] = deserialize((JsonObject) jsonArray.get(i));
							}
							return array;
						}
					} else if (Map.class.isAssignableFrom(clazz)) {
						final Constructor<?> constructor = clazz.getConstructor();
						@SuppressWarnings("unchecked")
						final Map<Object, Object> mapObject = (Map<Object, Object>) constructor.newInstance();
						for (final Object keyValueObject : (JsonArray) value) {
							if (!(keyValueObject instanceof JsonObject)) {
								throw new Exception("Invalid value type serialization value");
							}

							final JsonObject keyJsonObject = (JsonObject) ((JsonObject) keyValueObject).get("key");
							final Object keyObject = deserialize(keyJsonObject);

							final JsonObject valueJsonObject = (JsonObject) ((JsonObject) keyValueObject).get("value");
							final Object valueObject = deserialize(valueJsonObject);

							mapObject.put(keyObject, valueObject);
						}
						return mapObject;
					} else {
						final List<Object> listOfItems = new ArrayList<>();
						for (final Object item : (JsonArray) value) {
							if (!(item instanceof JsonObject)) {
								throw new Exception("Invalid value type serialization value");
							}

							listOfItems.add(deserialize((JsonObject) item));
						}

						final Constructor<?> constructor = clazz.getConstructor();
						final Object object = constructor.newInstance();
						if (object instanceof Collection) {
							for (final Object item : listOfItems) {
								@SuppressWarnings("unchecked")
								final
								Collection<Object> collectionObject = (Collection<Object>) object;
								collectionObject.add(item);
							}
							return object;
						} else {
							throw new Exception("");
						}
					}
				} else {
					throw new Exception("Invalid value type for deserialization: " + clazz.getName());
				}
			}
		} catch (final Exception e) {
			throw e;
		}
	}

	public static Object deserialize(final Class<?> classType, final JsonObject jsonObject) throws Exception {
		try {
			if (classType == null) {
				throw new Exception("Invalid class type serialization value");
			} else if (jsonObject == null) {
				return null;
			} else {
				final Constructor<?> constructor = ClassUtilities.getConstructor(classType);
				constructor.setAccessible(true);
				final Object object = constructor.newInstance();
				for (final Entry<String, Object> entry : jsonObject.entrySet()) {
					Field field;
					try {
						field = ClassUtilities.getField(classType, entry.getKey());
					} catch (@SuppressWarnings("unused") final Exception e) {
						throw new Exception("Invalid serialization field name '" + entry.getKey() + "' for class '" + classType.toString() + "'");
					}
					field.setAccessible(true);

					final Object value = entry.getValue();
					try {
						final Class<?> clazz = field.getType();
						if (value == null) {
							field.set(object, null);
						} else if (clazz == Boolean.TYPE || clazz == Boolean.class) {
							field.set(object, value);
						} else if (clazz == Byte.TYPE || clazz == Byte.class) {
							field.set(object, ((Number) value).byteValue());
						} else if (clazz == Short.TYPE || clazz == Short.class) {
							field.set(object, ((Number) value).shortValue());
						} else if (clazz == Integer.TYPE || clazz == Integer.class) {
							field.set(object, ((Number) value).intValue());
						} else if (clazz == Long.TYPE || clazz == Long.class) {
							field.set(object, ((Number) value).longValue());
						} else if (clazz == Float.TYPE || clazz == Float.class) {
							field.set(object, ((Number) value).floatValue());
						} else if (clazz == Double.TYPE || clazz == Double.class) {
							if (value.getClass() == Float.class) {
								field.set(object, Double.parseDouble(Float.toString((Float) value)));
							} else {
								field.set(object, ((Number) value).doubleValue());
							}
						} else if (clazz == BigDecimal.class) {
							field.set(object, value);
						} else if (clazz == Character.TYPE || clazz == Character.class) {
							field.set(object, ((String) value).charAt(0));
						} else if (clazz == String.class) {
							field.set(object, value);
						} else if (clazz == Charset.class) {
							field.set(object, Charset.forName((String) value));
						} else if (Date.class.isAssignableFrom(clazz)) {
							field.set(object, DateUtilities.getDateForZonedDateTime(DateUtilities.parseIso8601DateTimeString((String) value)));
						} else if (LocalDateTime.class.isAssignableFrom(clazz)) {
							field.set(object, DateUtilities.parseIso8601DateTimeString((String) value).toLocalDateTime());
						} else if (LocalDate.class.isAssignableFrom(clazz)) {
							field.set(object, DateUtilities.parseIso8601DateTimeString((String) value).toLocalDate());
						} else if (ZonedDateTime.class.isAssignableFrom(clazz)) {
							field.set(object, DateUtilities.parseIso8601DateTimeString((String) value));
						} else if (clazz.isEnum()) {
							final String enumName = (String) value;
							for (final Object enumConstant : clazz.getEnumConstants()) {
								if (enumName.equals(enumConstant.toString())) {
									field.set(object, enumConstant);
									break;
								}
							}
						} else if (clazz.isArray()) {
							final JsonArray jsonArray = ((JsonArray) value);
							if (clazz.getComponentType() == Boolean.TYPE) {
								final boolean[] arrayValue = new boolean[jsonArray.size()];
								for (int i = 0; i < arrayValue.length; i++) {
									arrayValue[i] = (boolean) jsonArray.get(i);
								}
								field.set(object, arrayValue);
							} else if (clazz.getComponentType() == Boolean.class) {
								final Boolean[] arrayValue = new Boolean[jsonArray.size()];
								for (int i = 0; i < arrayValue.length; i++) {
									arrayValue[i] = (Boolean) jsonArray.get(i);
								}
								field.set(object, arrayValue);
							} else if (clazz.getComponentType() == Byte.TYPE) {
								final byte[] arrayValue = new byte[jsonArray.size()];
								for (int i = 0; i < arrayValue.length; i++) {
									arrayValue[i] = ((Number) jsonArray.get(i)).byteValue();
								}
								field.set(object, arrayValue);
							} else if (clazz.getComponentType() == Byte.class) {
								final Byte[] arrayValue = new Byte[jsonArray.size()];
								for (int i = 0; i < arrayValue.length; i++) {
									arrayValue[i] = ((Number) jsonArray.get(i)).byteValue();
								}
								field.set(object, arrayValue);
							} else if (clazz.getComponentType() == Short.TYPE) {
								final short[] arrayValue = new short[jsonArray.size()];
								for (int i = 0; i < arrayValue.length; i++) {
									arrayValue[i] = ((Number) jsonArray.get(i)).shortValue();
								}
								field.set(object, arrayValue);
							} else if (clazz.getComponentType() == Short.class) {
								final Short[] arrayValue = new Short[jsonArray.size()];
								for (int i = 0; i < arrayValue.length; i++) {
									arrayValue[i] = ((Number) jsonArray.get(i)).shortValue();
								}
								field.set(object, arrayValue);
							} else if (clazz.getComponentType() == Integer.TYPE) {
								final int[] arrayValue = new int[jsonArray.size()];
								for (int i = 0; i < arrayValue.length; i++) {
									arrayValue[i] = ((Number) jsonArray.get(i)).intValue();
								}
								field.set(object, arrayValue);
							} else if (clazz.getComponentType() == Integer.class) {
								final Integer[] arrayValue = new Integer[jsonArray.size()];
								for (int i = 0; i < arrayValue.length; i++) {
									arrayValue[i] = ((Number) jsonArray.get(i)).intValue();
								}
								field.set(object, arrayValue);
							} else if (clazz.getComponentType() == Long.TYPE) {
								final long[] arrayValue = new long[jsonArray.size()];
								for (int i = 0; i < arrayValue.length; i++) {
									arrayValue[i] = ((Number) jsonArray.get(i)).longValue();
								}
								field.set(object, arrayValue);
							} else if (clazz.getComponentType() == Long.class) {
								final Long[] arrayValue = new Long[jsonArray.size()];
								for (int i = 0; i < arrayValue.length; i++) {
									arrayValue[i] = ((Number) jsonArray.get(i)).longValue();
								}
								field.set(object, arrayValue);
							} else if (clazz.getComponentType() == Float.TYPE) {
								final float[] arrayValue = new float[jsonArray.size()];
								for (int i = 0; i < arrayValue.length; i++) {
									arrayValue[i] = (float) jsonArray.get(i);
								}
								field.set(object, arrayValue);
							} else if (clazz.getComponentType() == Float.class) {
								final Float[] arrayValue = new Float[jsonArray.size()];
								for (int i = 0; i < arrayValue.length; i++) {
									arrayValue[i] = (float) jsonArray.get(i);
								}
								field.set(object, arrayValue);
							} else if (clazz.getComponentType() == Double.TYPE) {
								final double[] arrayValue = new double[jsonArray.size()];
								for (int i = 0; i < arrayValue.length; i++) {
									arrayValue[i] = (double) jsonArray.get(i);
								}
								field.set(object, arrayValue);
							} else if (clazz.getComponentType() == Double.class) {
								final Double[] arrayValue = new Double[jsonArray.size()];
								for (int i = 0; i < arrayValue.length; i++) {
									arrayValue[i] = (double) jsonArray.get(i);
								}
								field.set(object, arrayValue);
							} else if (clazz.getComponentType() == Character.TYPE) {
								final char[] arrayValue = new char[jsonArray.size()];
								for (int i = 0; i < arrayValue.length; i++) {
									arrayValue[i] = ((String) jsonArray.get(i)).charAt(0);
								}
								field.set(object, arrayValue);
							} else if (clazz.getComponentType() == Character.class) {
								final Character[] arrayValue = new Character[jsonArray.size()];
								for (int i = 0; i < arrayValue.length; i++) {
									arrayValue[i] = ((String) jsonArray.get(i)).charAt(0);
								}
								field.set(object, arrayValue);
							} else if (clazz.getComponentType() == String.class) {
								final String[] arrayValue = new String[jsonArray.size()];
								for (int i = 0; i < arrayValue.length; i++) {
									arrayValue[i] = (String) jsonArray.get(i);
								}
								field.set(object, arrayValue);
							} else if (Date.class.isAssignableFrom(clazz.getComponentType())) {
								final Date[] arrayValue = new Date[jsonArray.size()];
								for (int i = 0; i < arrayValue.length; i++) {
									arrayValue[i] = DateUtilities.getDateForLocalDateTime(DateUtilities.parseLocalDateTime(DateUtilities.ISO_8601_DATETIME_FORMAT, (String) jsonArray.get(i)));
								}
								field.set(object, arrayValue);
							} else if (LocalDateTime.class.isAssignableFrom(clazz.getComponentType())) {
								final LocalDateTime[] arrayValue = new LocalDateTime[jsonArray.size()];
								for (int i = 0; i < arrayValue.length; i++) {
									arrayValue[i] = DateUtilities.parseLocalDateTime(DateUtilities.ISO_8601_DATETIME_FORMAT, (String) jsonArray.get(i));
								}
								field.set(object, arrayValue);
							} else if (LocalDate.class.isAssignableFrom(clazz.getComponentType())) {
								final LocalDate[] arrayValue = new LocalDate[jsonArray.size()];
								for (int i = 0; i < arrayValue.length; i++) {
									arrayValue[i] = DateUtilities.parseLocalDate(DateUtilities.ISO_8601_DATETIME_FORMAT, (String) jsonArray.get(i));
								}
								field.set(object, arrayValue);
							} else if (ZonedDateTime.class.isAssignableFrom(clazz.getComponentType())) {
								final ZonedDateTime[] arrayValue = new ZonedDateTime[jsonArray.size()];
								for (int i = 0; i < arrayValue.length; i++) {
									arrayValue[i] = DateUtilities.parseZonedDateTime(DateUtilities.ISO_8601_DATETIME_FORMAT, (String) jsonArray.get(i), ZoneId.systemDefault());
								}
								field.set(object, arrayValue);
							} else if (Enum.class.isAssignableFrom(clazz.getComponentType())) {
								final Object[] arrayValue = (Object[]) Array.newInstance(clazz.getComponentType(), jsonArray.size());
								for (int i = 0; i < arrayValue.length; i++) {
									final String enumName = (String) jsonArray.get(i);
									boolean enumFound = false;
									for (final Object enumConstant : clazz.getComponentType().getEnumConstants()) {
										if (enumName.equals(enumConstant.toString())) {
											arrayValue[i] = enumConstant;
											enumFound = true;
											break;
										}
									}
									if (enumFound == false) {
										throw new Exception("Invalid enum name '" + enumName + "' for type '" + clazz.getComponentType().getName() + "'");
									}
								}
								field.set(object, arrayValue);
							} else {
								final Object[] arrayValue = (Object[]) Array.newInstance(clazz.getComponentType(), jsonArray.size());
								for (int i = 0; i < arrayValue.length; i++) {
									arrayValue[i] = deserialize(Object.class, (JsonObject) jsonArray.get(i));
								}
								field.set(object, arrayValue);
							}
						} else if (List.class.isAssignableFrom(clazz)) {
							final List<Object> listOfItems = new ArrayList<>();
							for (final Object item : (JsonArray) value) {
								listOfItems.add(item);
							}
							field.set(object, listOfItems);
						} else if (Set.class.isAssignableFrom(clazz)) {
							final Set<Object> setOfItems = new HashSet<>();
							for (final Object item : (JsonArray) value) {
								setOfItems.add(item);
							}
							field.set(object, setOfItems);
						} else if (Collection.class.isAssignableFrom(clazz)) {
							final Collection<Object> collectionOfItems = new ArrayList<>();
							for (final Object item : (JsonArray) value) {
								collectionOfItems.add(item);
							}
							field.set(object, collectionOfItems);
						} else if (Map.class.isAssignableFrom(clazz)) {
							final Map<Object, Object> mapObject = new HashMap<>();
							if (!(value instanceof JsonArray)) {
								throw new Exception("Invalid value type serialization value");
							}
							for (final Object keyValueObject : (JsonArray) value) {
								if (!(keyValueObject instanceof JsonObject)) {
									throw new Exception("Invalid value type serialization value");
								}

								final Object keyObject = ((JsonObject) keyValueObject).get("key");
								final Object valueObject = ((JsonObject) keyValueObject).get("value");

								mapObject.put(keyObject, valueObject);
							}
							field.set(object, mapObject);
						} else if (value instanceof JsonObject) {
							field.set(object, deserialize(clazz, (JsonObject) value));
						} else {
							field.set(object, deserialize(clazz.getComponentType(), (JsonObject) value));
						}
					} catch (final Exception e) {
						throw new Exception("Invalid value type for field '" + entry.getKey() + "' in class '" + classType.toString() + "': " + e.getMessage(), e);
					}
				}
				return object;
			}
		} catch (final Exception e) {
			throw e;
		}
	}
}
