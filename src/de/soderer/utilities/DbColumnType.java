package de.soderer.utilities;

public class DbColumnType {
	public enum SimpleDataType {
		String, Date, Integer, Double, Blob, Clob
	}

	private String typeName;
	private long characterLength; // only for VARCHAR and VARCHAR2 types
	private int numericPrecision; // only for numeric types
	private int numericScale; // only for numeric types
	private boolean nullable;

	public DbColumnType(String typeName, long characterLength, int numericPrecision, int numericScale, boolean nullable) {
		this.typeName = typeName;
		this.characterLength = characterLength;
		this.numericPrecision = numericPrecision;
		this.numericScale = numericScale;
		this.nullable = nullable;
	}

	public String getTypeName() {
		return typeName;
	}

	public long getCharacterLength() {
		return characterLength;
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

	public SimpleDataType getSimpleDataType() {
		if (typeName.toLowerCase().startsWith("date") || typeName.toLowerCase().startsWith("time")) {
			return SimpleDataType.Date;
		} else if (typeName.toLowerCase().equals("clob") || typeName.toLowerCase().equals("longtext")) {
			return SimpleDataType.Clob;
		} else if (typeName.toLowerCase().startsWith("varchar") || typeName.toLowerCase().contains("text") || typeName.toLowerCase().startsWith("character")) {
			return SimpleDataType.String;
		} else if (typeName.toLowerCase().equals("blob") || typeName.toLowerCase().equals("bytea")) {
			return SimpleDataType.Blob;
		} else if (typeName.toLowerCase().contains("int")) {
			return SimpleDataType.Integer;
		} else {
			// e.g.: PostgreSQL "REAL"
			return SimpleDataType.Double;
		}
	}
}
