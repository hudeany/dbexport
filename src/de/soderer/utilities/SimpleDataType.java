package de.soderer.utilities;

public enum SimpleDataType {
	Integer,
	Float,
	Date,
	DateTime,
	String,
	Clob,
	Blob;

	public static SimpleDataType getSimpleDataTypeByName(final String name) {
		for (final SimpleDataType simpleDataType : SimpleDataType.values()) {
			if (simpleDataType.name().equalsIgnoreCase(name)) {
				return simpleDataType;
			}
		}
		throw new RuntimeException("Unknown SimpleDataType: " + name);
	}
}
