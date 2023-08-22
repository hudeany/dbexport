package de.soderer.dbexport.converter;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Base64;

import de.soderer.utilities.FileCompressionType;
import de.soderer.utilities.IoUtilities;
import de.soderer.utilities.db.DbUtilities;

public class SQLiteDBValueConverter extends DefaultDBValueConverter {
	public SQLiteDBValueConverter(final FileCompressionType compressionType, final char[] zipPassword, final boolean useZipCrypto, final boolean createBlobFiles, final boolean createClobFiles, final String fileExtension) {
		super(compressionType, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, fileExtension);
	}

	@Override
	public Object convert(final ResultSetMetaData metaData, final ResultSet resultSet, final int columnIndex, final String exportFilePath) throws Exception {
		Object value;
		final int columnTypeCode = metaData.getColumnType(columnIndex);
		if (columnTypeCode == Types.BLOB) {
			// getBlob-method is not implemented by SQLite JDBC
			resultSet.getObject(columnIndex);
			if (resultSet.wasNull()) {
				value = null;
			} else {
				try (InputStream blobStream = resultSet.getBinaryStream(columnIndex)) {
					if (createBlobFiles) {
						value = writeLobFile(exportFilePath, "blob", blobStream);
					} else {
						final byte[] data = IoUtilities.toByteArray(blobStream);
						value = Base64.getEncoder().encodeToString(data);
					}
				}
			}
		} else if (columnTypeCode == Types.INTEGER) {
			value = resultSet.getObject(columnIndex);
			if (resultSet.wasNull()) {
				value = null;
			} else if (value instanceof Long) {
				value = (long) value;
			}
		} else if ("DATE".equals(metaData.getColumnTypeName(columnIndex))) {
			value = DbUtilities.extractSqliteLocalDate(resultSet.getObject(columnIndex));
		} else if ("TIMESTAMP".equals(metaData.getColumnTypeName(columnIndex))) {
			value = DbUtilities.extractSqliteLocalDateTime(resultSet.getObject(columnIndex));
		} else {
			value = super.convert(metaData, resultSet, columnIndex, exportFilePath);
		}
		return value;
	}
}
