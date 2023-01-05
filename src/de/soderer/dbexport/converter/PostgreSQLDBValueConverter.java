package de.soderer.dbexport.converter;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Base64;

import de.soderer.utilities.IoUtilities;
import de.soderer.utilities.Utilities;

public class PostgreSQLDBValueConverter extends DefaultDBValueConverter {
	public PostgreSQLDBValueConverter(final boolean zip, final char[] zipPassword, final boolean useZipCrypto, final boolean createBlobFiles, final boolean createClobFiles, final String fileExtension) {
		super(zip, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, fileExtension);
	}

	@Override
	public Object convert(final ResultSetMetaData metaData, final ResultSet resultSet, final int columnIndex, final String exportFilePath) throws Exception {
		Object value;
		final int columnTypeCode = metaData.getColumnType(columnIndex);
		if (columnTypeCode == Types.BINARY) {
			// getBlob-method is not implemented by PostgreSQL JDBC
			resultSet.getObject(columnIndex);
			if (resultSet.wasNull()) {
				value = null;
			} else {
				InputStream blobStream = null;
				try {
					blobStream = resultSet.getBinaryStream(columnIndex);
					if (createBlobFiles) {
						final File blobOutputFile = new File(getLobFilePath(exportFilePath, "blob"));
						try (InputStream input = blobStream) {
							OutputStream output = null;
							try {
								output = openLobOutputStream(blobOutputFile);
								IoUtilities.copy(input, output);
							} finally {
								checkAndCloseZipEntry(output, blobOutputFile);
								Utilities.closeQuietly(output);
							}
							value = blobOutputFile;
						} catch (final Exception e) {
							throw new Exception("Error creating blob file '" + blobOutputFile.getAbsolutePath() + "': " + e.getMessage());
						}
					} else {
						final byte[] data = IoUtilities.toByteArray(blobStream);
						value = Base64.getEncoder().encodeToString(data);
					}
				} finally {
					Utilities.closeQuietly(blobStream);
				}
			}
		} else {
			value = super.convert(metaData, resultSet, columnIndex, exportFilePath);
		}
		return value;
	}
}
