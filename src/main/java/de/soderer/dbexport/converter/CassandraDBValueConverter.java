package de.soderer.dbexport.converter;

import java.io.ByteArrayInputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Base64;

import de.soderer.utilities.FileCompressionType;

public class CassandraDBValueConverter extends DefaultDBValueConverter {
	public CassandraDBValueConverter(final FileCompressionType compression, final char[] zipPassword, final boolean useZipCrypto, final boolean createBlobFiles, final boolean createClobFiles, final String fileExtension) {
		super(compression, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, fileExtension);
	}

	@Override
	public Object convert(final ResultSetMetaData metaData, final ResultSet resultSet, final int columnIndex, final String exportFilePath) throws Exception {
		Object value;
		final int columnTypeCode = metaData.getColumnType(columnIndex);
		if (columnTypeCode == Types.LONGVARBINARY) {
			final byte[] data = (byte[]) resultSet.getObject(columnIndex);
			if (resultSet.wasNull()) {
				value = null;
			} else if (createBlobFiles) {
				value = writeLobFile(exportFilePath, "blob", new ByteArrayInputStream(data));
			} else {
				value = Base64.getEncoder().encodeToString(data);
			}
		} else {
			value = super.convert(metaData, resultSet, columnIndex, exportFilePath);
		}
		return value;
	}
}
