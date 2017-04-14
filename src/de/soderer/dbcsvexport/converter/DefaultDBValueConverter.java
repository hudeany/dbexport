package de.soderer.dbcsvexport.converter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Base64;
import java.util.Date;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import de.soderer.utilities.Utilities;
import de.soderer.utilities.ZipUtilities;

public class DefaultDBValueConverter {
	protected boolean zip;
	protected boolean createBlobFiles;
	protected boolean createClobFiles;
	protected String outputFilePath;
	protected String fileExtension;

	public DefaultDBValueConverter(boolean zip, boolean createBlobFiles, boolean createClobFiles, String fileExtension) {
		this.zip = zip;
		this.createBlobFiles = createBlobFiles;
		this.createClobFiles = createClobFiles;
		this.fileExtension = fileExtension;
	}
	
	public Object convert(ResultSet resultSet, int columnIndex, String outputFilePath) throws Exception {
		ResultSetMetaData metaData = resultSet.getMetaData();
		Object value;
		if (metaData.getColumnType(columnIndex) == Types.BLOB) {
			Blob blob = resultSet.getBlob(columnIndex);
			if (resultSet.wasNull()) {
				value = null;
			} else if (createBlobFiles) {
				File blobOutputFile = new File(getLobFilePath(outputFilePath, "blob"));
				try (InputStream input = blob.getBinaryStream()) {
					OutputStream output = null;
					try {
						output = openLobOutputStream(blobOutputFile);
						Utilities.copy(input, output);
					} finally {
						checkAndCloseZipEntry(output);
						Utilities.closeQuietly(output);
					}
					value = blobOutputFile;
				} catch (Exception e) {
					throw new Exception("Error creating blob file '" + blobOutputFile.getAbsolutePath() + "': " + e.getMessage());
				}
			} else {
				try (InputStream input = blob.getBinaryStream()) {
					byte[] data = Utilities.toByteArray(input);
					value = Base64.getEncoder().encodeToString(data);
				}
			}
		} else if (metaData.getColumnType(columnIndex) == Types.CLOB) {
			Clob clob = resultSet.getClob(columnIndex);
			if (resultSet.wasNull()) {
				value = null;
			} else if (createClobFiles) {
				File clobOutputFile = new File(getLobFilePath(outputFilePath, "clob"));
				try (Reader input = clob.getCharacterStream()) {
					OutputStream output = null;
					try {
						output = openLobOutputStream(clobOutputFile);
						Utilities.copy(input, output, "UTF-8");
					} finally {
						checkAndCloseZipEntry(output);
						Utilities.closeQuietly(output);
					}
					value = clobOutputFile;
				} catch (Exception e) {
					throw new Exception("Error creating clob file '" + clobOutputFile.getAbsolutePath() + "': " + e.getMessage());
				}
			} else {
				try (Reader input = clob.getCharacterStream()) {
					value = Utilities.toString(input);
				}
			}
		} else if (metaData.getColumnType(columnIndex) == Types.DATE || metaData.getColumnType(columnIndex) == Types.TIMESTAMP) {
			value = resultSet.getObject(columnIndex);
			if ("0000-00-00 00:00:00".equals(value)) {
				value = null;
			}
		} else {
			value = resultSet.getObject(columnIndex);
			if (resultSet.wasNull()) {
				value = null;
			}
		}
		return value;
	}
	
	protected String getLobFilePath(String outputFilePath, String lobType) throws Exception {
		String lobOutputFilePathPrefix = outputFilePath;
		if (outputFilePath.endsWith(".zip")) {
			outputFilePath.substring(0, outputFilePath.length() - 4);
		}
		if (outputFilePath.endsWith("." + fileExtension)) {
			outputFilePath.substring(0, outputFilePath.length() - (fileExtension.length() + 1));
		}
		return File.createTempFile(lobOutputFilePathPrefix + "_", "." + lobType + (zip ? ".zip" : "")).getAbsolutePath();
	}
	
	protected OutputStream openLobOutputStream(File lobOutputFile) throws IOException, FileNotFoundException {
		if (zip) {
			OutputStream output = ZipUtilities.openNewZipOutputStream(new FileOutputStream(lobOutputFile));
			String entryFileName = lobOutputFile.getName().substring(0, lobOutputFile.getName().lastIndexOf("."));
			ZipEntry entry = new ZipEntry(entryFileName);
			entry.setTime(new Date().getTime());
			((ZipOutputStream) output).putNextEntry(entry);
			return output;
		} else {
			return new FileOutputStream(lobOutputFile);
		}
	}

	protected void checkAndCloseZipEntry(OutputStream outputStream) {
		if (outputStream instanceof ZipOutputStream) {
			try {
				((ZipOutputStream) outputStream).closeEntry();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
