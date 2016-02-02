package de.soderer.dbcsvexport.converter;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Base64;

import de.soderer.utilities.Utilities;

public class PostgreSQLDBValueConverter extends DefaultDBValueConverter {
	public PostgreSQLDBValueConverter(boolean zip, boolean createBlobFiles, boolean createClobFiles, String fileExtension) {
		super(zip, createBlobFiles, createClobFiles, fileExtension);
	}

	@Override
	public Object convert(ResultSet resultSet, int columnIndex, String outputFilePath) throws Exception {
		ResultSetMetaData metaData = resultSet.getMetaData();
		Object value;
		if (metaData.getColumnType(columnIndex) == Types.BINARY) {
			// getBlob-method is not implemented by PostgreSQL JDBC
			resultSet.getObject(columnIndex);
			if (resultSet.wasNull()) {
				value = null;
			} else {
				InputStream blobStream = null;
				try {
					blobStream = resultSet.getBinaryStream(columnIndex);
					if (createBlobFiles) {
						File blobOutputFile = new File(getLobFilePath(outputFilePath, "blob"));
						try (InputStream input = blobStream) {
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
						byte[] data = Utilities.toByteArray(blobStream);
						value = Base64.getEncoder().encodeToString(data);
					}
				} finally {
					Utilities.closeQuietly(blobStream);
				}
			}
		} else {
			value = super.convert(resultSet, columnIndex, outputFilePath);
		}
		return value;
	}
}
