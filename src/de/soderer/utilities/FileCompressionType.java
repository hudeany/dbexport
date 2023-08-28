package de.soderer.utilities;

/**
 * The Enum CompressionType.
 */
public enum FileCompressionType {
	ZIP ("zip"),
	TARGZ ("tar.gz"),
	TGZ ("tgz"),
	GZ ("gz");

	private final String defaultFileExtension;

	public String getDefaultFileExtension() {
		return defaultFileExtension;
	}

	FileCompressionType(final String defaultFileExtension) {
		this.defaultFileExtension = defaultFileExtension;
	}

	public static FileCompressionType getFromString(final String compressionTypeString) {
		for (final FileCompressionType compressionType : FileCompressionType.values()) {
			if (compressionType.toString().equalsIgnoreCase(compressionTypeString)) {
				return compressionType;
			}
		}
		throw new RuntimeException("Invalid compression type: " + compressionTypeString);
	}
}
