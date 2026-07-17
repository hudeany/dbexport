package de.soderer.dbexport.converter;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Base64;

import de.soderer.utilities.FileCompressionType;
import de.soderer.utilities.IoUtilities;

public class PostgreSQLDBValueConverter extends DefaultDBValueConverter {
	public PostgreSQLDBValueConverter(final FileCompressionType compressionType, final char[] zipPassword, final boolean useZipCrypto, final boolean createBlobFiles, final boolean createClobFiles, final String fileExtension) {
		super(compressionType, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, fileExtension);
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
				if (createBlobFiles) {
					try (InputStream dataStream = resultSet.getBinaryStream(columnIndex)) {
						value = writeLobFile(exportFilePath, "blob", dataStream);
					}
				} else {
					try (InputStream dataStream = resultSet.getBinaryStream(columnIndex)) {
						final byte[] data = IoUtilities.toByteArray(dataStream);
						value = Base64.getEncoder().encodeToString(data);
					}
				}
			}
		} else {
			value = super.convert(metaData, resultSet, columnIndex, exportFilePath);
		}
		return value;
	}
}
