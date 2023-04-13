package de.soderer.dbexport.converter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import de.soderer.utilities.IoUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.zip.Zip4jUtilities;
import de.soderer.utilities.zip.ZipUtilities;

public class DefaultDBValueConverter {
	protected boolean zip;
	protected char[] zipPassword;
	protected boolean useZipCrypto = false;
	protected boolean createBlobFiles;
	protected boolean createClobFiles;
	protected String outputFilePath;
	protected String fileExtension;

	public DefaultDBValueConverter(final boolean zip, final char[] zipPassword, final boolean useZipCrypto, final boolean createBlobFiles, final boolean createClobFiles, final String fileExtension) {
		this.zip = zip;
		this.zipPassword = zipPassword;
		this.useZipCrypto = useZipCrypto;
		this.createBlobFiles = createBlobFiles;
		this.createClobFiles = createClobFiles;
		this.fileExtension = fileExtension;
	}

	public Object convert(final ResultSetMetaData metaData, final ResultSet resultSet, final int columnIndex, final String exportFilePath) throws Exception {
		Object value;
		final int columnTypeCode = metaData.getColumnType(columnIndex);
		if (columnTypeCode == Types.BLOB) {
			final Blob blob = resultSet.getBlob(columnIndex);
			if (resultSet.wasNull()) {
				value = null;
			} else if (createBlobFiles) {
				final File blobOutputFile = new File(getLobFilePath(exportFilePath, "blob"));
				try (InputStream input = blob.getBinaryStream()) {
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
				try (InputStream input = blob.getBinaryStream()) {
					final byte[] data = IoUtilities.toByteArray(input);
					value = Base64.getEncoder().encodeToString(data);
				}
			}
		} else if (columnTypeCode == Types.CLOB) {
			final Clob clob = resultSet.getClob(columnIndex);
			if (resultSet.wasNull()) {
				value = null;
			} else if (createClobFiles) {
				final File clobOutputFile = new File(getLobFilePath(exportFilePath, "clob"));
				try (Reader input = clob.getCharacterStream()) {
					OutputStream output = null;
					try {
						output = openLobOutputStream(clobOutputFile);
						IoUtilities.copy(input, output, StandardCharsets.UTF_8);
					} finally {
						checkAndCloseZipEntry(output, clobOutputFile);
						Utilities.closeQuietly(output);
					}
					value = clobOutputFile;
				} catch (final Exception e) {
					throw new Exception("Error creating clob file '" + clobOutputFile.getAbsolutePath() + "': " + e.getMessage());
				}
			} else {
				try (Reader input = clob.getCharacterStream()) {
					value = Utilities.toString(input);
				}
			}
		} else if (columnTypeCode == Types.TIMESTAMP || columnTypeCode == Types.DATE) {
			value = resultSet.getObject(columnIndex);
			if (value != null && "0000-00-00 00:00:00".equals(value)) {
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

	protected String getLobFilePath(final String exportFilePath, final String lobType) throws Exception {
		String lobOutputFilePathPrefix = exportFilePath;
		if (lobOutputFilePathPrefix.endsWith(".zip")) {
			lobOutputFilePathPrefix = exportFilePath.substring(0, exportFilePath.length() - 4);
		}
		if (lobOutputFilePathPrefix.endsWith("." + fileExtension)) {
			lobOutputFilePathPrefix = lobOutputFilePathPrefix.substring(0, lobOutputFilePathPrefix.length() - (fileExtension.length() + 1));
		}
		return File.createTempFile(new File(lobOutputFilePathPrefix).getName() + "_", "." + lobType + (zip ? ".zip" : ""), new File(exportFilePath).getParentFile()).getAbsolutePath();
	}

	protected OutputStream openLobOutputStream(final File lobOutputFile) throws IOException, FileNotFoundException {
		if (zip) {
			final OutputStream outputStream = ZipUtilities.openNewZipOutputStream(new FileOutputStream(lobOutputFile));
			final String entryFileName = lobOutputFile.getName().substring(0, lobOutputFile.getName().lastIndexOf("."));
			final ZipEntry entry = new ZipEntry(entryFileName);
			entry.setTime(ZonedDateTime.now().toInstant().toEpochMilli());
			((ZipOutputStream) outputStream).putNextEntry(entry);
			return outputStream;
		} else {
			return new FileOutputStream(lobOutputFile);
		}
	}

	protected void checkAndCloseZipEntry(final OutputStream outputStream, final File lobOutputFile) throws Exception {
		if (outputStream instanceof ZipOutputStream) {
			try {
				outputStream.close();
			} catch (final Exception e) {
				e.printStackTrace();
			}

			try {
				((ZipOutputStream) outputStream).closeEntry();
			} catch (final Exception e) {
				e.printStackTrace();
			}

			if (zip && zipPassword != null) {
				Zip4jUtilities.createPasswordSecuredZipFile(lobOutputFile.getAbsolutePath(), zipPassword, useZipCrypto);
			}
		}
	}
}
