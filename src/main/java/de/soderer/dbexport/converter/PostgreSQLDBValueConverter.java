package de.soderer.dbexport.converter;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Base64;

import de.soderer.utilities.FileCompressionType;
import de.soderer.utilities.IoUtilities;

/**
 * Value converter for PostgreSQL (binary data is read as stream, the JDBC driver does not support getBlob()).
 */
public class PostgreSQLDBValueConverter extends DefaultDBValueConverter {
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
