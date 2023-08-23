package de.soderer.utilities.json;

public enum JsonDataType {
	STRING,
	INTEGER,
	NUMBER,
	OBJECT,
	ARRAY,
	BOOLEAN,
	NULL;

	public String getName() {
		return name().toLowerCase();
	}

	public static JsonDataType getFromString(final String value) throws Exception {
		for (final JsonDataType jsonDataType : JsonDataType.values()) {
			if (jsonDataType.name().toLowerCase().equals(value)) {
				return jsonDataType;
			}
		}
		throw new Exception("Invalid JSON data type: " + value);
	}
}
