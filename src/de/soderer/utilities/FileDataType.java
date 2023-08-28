package de.soderer.utilities;

public enum FileDataType {
	CSV,
	EXCEL,
	JSON,
	KDBX,
	ODS,
	SQL,
	VCF,
	XML;

	public static FileDataType getFromString(final String dataTypeString) {
		for (final FileDataType dataType : FileDataType.values()) {
			if (dataType.toString().equalsIgnoreCase(dataTypeString)) {
				return dataType;
			}
		}
		throw new RuntimeException("Invalid file data type: " + dataTypeString);
	}
}
