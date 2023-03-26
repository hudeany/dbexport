package de.soderer.utilities.json;

import java.math.BigDecimal;

public class JsonNode {
	private final String propertyName;
	private final Object value;
	private JsonDataType jsonDataType;

	public JsonNode(final Object value) throws Exception {
		this(null, value);
	}

	public JsonNode(final String propertyName, final Object value) throws Exception {
		this.propertyName = propertyName;
		this.value = value;
		if (value == null) {
			jsonDataType = JsonDataType.NULL;
		} else if (value instanceof Boolean) {
			jsonDataType = JsonDataType.BOOLEAN;
		} else if (value instanceof Integer || value instanceof Long || (value instanceof BigDecimal && ((BigDecimal) value).scale() == 0)) {
			jsonDataType = JsonDataType.INTEGER;
		} else if (value instanceof Number) {
			jsonDataType = JsonDataType.NUMBER;
		} else if (value instanceof String || value instanceof Character) {
			jsonDataType = JsonDataType.STRING;
		} else if (value instanceof JsonObject) {
			jsonDataType = JsonDataType.OBJECT;
		} else if (value instanceof JsonArray) {
			jsonDataType = JsonDataType.ARRAY;
		} else {
			throw new Exception("Unknown JSON data type: " + value.getClass().getSimpleName());
		}
	}

	public String getPropertyName() {
		return propertyName;
	}

	public Object getValue() {
		return value;
	}

	public JsonDataType getJsonDataType() {
		return jsonDataType;
	}

	public boolean isNull() {
		return jsonDataType == JsonDataType.NULL;
	}

	public boolean isBoolean() {
		return jsonDataType == JsonDataType.BOOLEAN;
	}

	public boolean isInteger() {
		return jsonDataType == JsonDataType.INTEGER;
	}

	public boolean isNumber() {
		return jsonDataType == JsonDataType.NUMBER || jsonDataType == JsonDataType.INTEGER;
	}

	public boolean isString() {
		return jsonDataType == JsonDataType.STRING;
	}

	public boolean isJsonObject() {
		return jsonDataType == JsonDataType.OBJECT;
	}

	public boolean isJsonArray() {
		return jsonDataType == JsonDataType.ARRAY;
	}

	public boolean isSimpleValue() {
		return jsonDataType == JsonDataType.NULL
				|| jsonDataType == JsonDataType.STRING
				|| jsonDataType == JsonDataType.BOOLEAN
				|| jsonDataType == JsonDataType.INTEGER
				|| jsonDataType == JsonDataType.NUMBER
				|| jsonDataType == JsonDataType.STRING;
	}

	public boolean isKomplexValue() {
		return jsonDataType == JsonDataType.OBJECT
				|| jsonDataType == JsonDataType.ARRAY;
	}

	@Override
	public String toString() {
		if (propertyName != null) {
			if (isNull()) {
				return propertyName + ": " + "null";
			} else {
				return propertyName + ": " + getValue().toString();
			}
		} else {
			if (isNull()) {
				return "null";
			} else {
				return getValue().toString();
			}
		}
	}
}
