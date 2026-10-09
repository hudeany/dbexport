package de.soderer.dbexport.converter;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Base64;

import de.soderer.utilities.FileCompressionType;
import de.soderer.utilities.IoUtilities;
import de.soderer.utilities.db.DbUtilities;

/**
 * Value converter for SQLite (blobs are read as stream, dates and timestamps are parsed from their stored values).
 */
public class SQLiteDBValueConverter extends DefaultDBValueConverter {
	/**
	 * Creates the converter.
	 *
	 * @param compressionType compression of the lob files, or null for uncompressed files
	 * @param zipPassword password of zip compressed lob files, or null
	 * @param useZipCrypto true to encrypt zip files with the weak ZipCrypto method instead of AES
	 * @param createBlobFiles true to export blobs as separate files instead of base64 encoded values
	 * @param createClobFiles true to export clobs as separate files instead of text values
	 * @param fileExtension file extension of the export format (e.g. "csv"), removed from the lob file names
	 */
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
