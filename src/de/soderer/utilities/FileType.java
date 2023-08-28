package de.soderer.utilities;

public class FileType {
	FileDataType fileDataType = null;
	FileCompressionType fileCompressionType = null;

	public FileDataType getFileDataType() {
		return fileDataType;
	}

	public void setFileDataType(final FileDataType fileDataType) {
		this.fileDataType = fileDataType;
	}

	public FileCompressionType getFileCompressionType() {
		return fileCompressionType;
	}

	public void setFileCompressionType(final FileCompressionType fileCompressionType) {
		this.fileCompressionType = fileCompressionType;
	}
}
