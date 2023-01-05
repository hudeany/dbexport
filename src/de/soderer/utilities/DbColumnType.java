package de.soderer.utilities;

public class DbColumnType {
	public enum SimpleDataType {
		String, Date, DateTime, Integer, Double, Blob, Clob
	}

	private final String typeName;
	private final long characterByteSize; // only for VARCHAR and VARCHAR2 types
	private final int numericPrecision; // only for numeric types
	private final int numericScale; // only for numeric types
	private final boolean nullable;
	private final boolean autoIncrement;

	public DbColumnType(final String typeName, final long characterByteSize, final int numericPrecision, final int numericScale, final boolean nullable, final boolean autoIncrement) {
		this.typeName = typeName;
		this.characterByteSize = characterByteSize;
		this.numericPrecision = numericPrecision;
		this.numericScale = numericScale;
		this.nullable = nullable;
		this.autoIncrement = autoIncrement;
	}

	public String getTypeName() {
		return typeName;
	}

	public long getCharacterByteSize() {
		return characterByteSize;
	}

	public int getNumericPrecision() {
		return numericPrecision;
	}

	public int getNumericScale() {
		return numericScale;
	}

	public boolean isNullable() {
		return nullable;
	}

	public boolean isAutoIncrement() {
		return autoIncrement;
	}

	public SimpleDataType getSimpleDataType() {
		if (typeName.toLowerCase().contains("time")) {
			return SimpleDataType.DateTime;
		} else if (typeName.toLowerCase().contains("date")) {
			return SimpleDataType.Date;
		} else if ("clob".equals(typeName.toLowerCase()) || "longtext".equals(typeName.toLowerCase())) {
			return SimpleDataType.Clob;
		} else if (typeName.toLowerCase().startsWith("varchar") || typeName.toLowerCase().startsWith("char") || typeName.toLowerCase().contains("text") || typeName.toLowerCase().startsWith("character")) {
			return SimpleDataType.String;
		} else if ("blob".equals(typeName.toLowerCase()) ||"bytea".equals( typeName.toLowerCase())) {
			return SimpleDataType.Blob;
		} else if (typeName.toLowerCase().contains("int")) {
			return SimpleDataType.Integer;
		} else {
			// e.g.: PostgreSQL "REAL"
			return SimpleDataType.Double;
		}
	}

	@Override
	public String toString() {
		final SimpleDataType simpleDataType = getSimpleDataType();
		return typeName
				+ (simpleDataType == SimpleDataType.String ? "(" + characterByteSize + ")" : "")
				+ (simpleDataType == SimpleDataType.Integer || simpleDataType == SimpleDataType.Double ? "(" + numericPrecision + ", " + numericScale + ")" : "")
				+ (nullable ? " nullable": " not nullable")
				+ (autoIncrement ? " autoIncrement": "");
	}
}
