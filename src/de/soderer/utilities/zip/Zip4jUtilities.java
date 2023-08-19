package de.soderer.utilities.zip;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import de.soderer.utilities.InputStreamWithOtherItemsToClose;
import de.soderer.utilities.Utilities;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.model.FileHeader;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.model.enums.AesKeyStrength;
import net.lingala.zip4j.model.enums.CompressionMethod;
import net.lingala.zip4j.model.enums.EncryptionMethod;

public class Zip4jUtilities {
	public static void createPasswordSecuredZipFile(final String originalZipFilePath, final char[] zipPassword, final boolean useZipCrypto) throws IOException {
		final ZipParameters zipParameters = new ZipParameters();
		zipParameters.setCompressionMethod(CompressionMethod.DEFLATE);
		zipParameters.setEncryptFiles(true);
		if (!useZipCrypto) {
			zipParameters.setEncryptionMethod(EncryptionMethod.AES);
			zipParameters.setAesKeyStrength(AesKeyStrength.KEY_STRENGTH_256);
		} else {
			zipParameters.setEncryptionMethod(EncryptionMethod.ZIP_STANDARD);
		}
		zipParameters.setFileNameInZip(new File(originalZipFilePath.substring(0, originalZipFilePath.length() - 4)).getName());
		try (ZipFile zipFile = new ZipFile(new File(originalZipFilePath + ".tmp"), zipPassword)) {
			try (ZipFile unencryptedZipFile = new ZipFile(new File(originalZipFilePath))) {
				try (InputStream inputStream = new InputStreamWithOtherItemsToClose(unencryptedZipFile.getInputStream(unencryptedZipFile.getFileHeaders().get(0)), unencryptedZipFile)) {
					zipFile.addStream(inputStream, zipParameters);
				}
			}
		}
		new File(originalZipFilePath).delete();
		new File(originalZipFilePath + ".tmp").renameTo(new File(originalZipFilePath));
	}

	public static long getUncompressedSize(final File zipFilePath, final char[] zipPassword) throws IOException {
		try (final ZipFile zipFile = new ZipFile(zipFilePath, zipPassword)) {
			final List<FileHeader> fileHeaders = zipFile.getFileHeaders();
			long uncompressedSize = 0;
			for (final FileHeader fileHeader : fileHeaders) {
				final long originalSize = fileHeader.getUncompressedSize();
				if (originalSize >= 0) {
					uncompressedSize += originalSize;
				} else {
					// -1 indicates, that size is unknown
					uncompressedSize = originalSize;
					break;
				}
			}
			return uncompressedSize;
		}
	}

	public static InputStreamWithOtherItemsToClose openPasswordSecuredZipFile(final String importFilePathOrData, final char[] zipPassword) throws Exception {
		return openPasswordSecuredZipFile(importFilePathOrData, zipPassword, null);
	}

	public static InputStreamWithOtherItemsToClose openPasswordSecuredZipFile(final String importFilePathOrData, final char[] zipPassword, final String zippedFilePathAndName) throws Exception {
		ZipFile zipFile = null;
		try {
			zipFile = new ZipFile(importFilePathOrData, zipPassword);
			final List<FileHeader> fileHeaders = zipFile.getFileHeaders();
			FileHeader selectedFileHeader = null;

			if (fileHeaders != null && fileHeaders.size() >= 0) {
				if (Utilities.isBlank(zippedFilePathAndName)) {
					if (fileHeaders.size() == 1 && !fileHeaders.get(0).isDirectory()) {
						return new InputStreamWithOtherItemsToClose(zipFile.getInputStream(fileHeaders.get(0)), fileHeaders.get(0).getFileName(), zipFile);
					} else {
						throw new Exception("Zip file '" + importFilePathOrData + "' contains more than one file");
					}
				} else {
					for (final FileHeader fileHeader : fileHeaders) {
						if (!fileHeader.isDirectory() && fileHeader.getFileName().equals(zippedFilePathAndName)) {
							selectedFileHeader = fileHeader;
							break;
						}
					}
					if (selectedFileHeader != null) {
						return new InputStreamWithOtherItemsToClose(zipFile.getInputStream(selectedFileHeader), selectedFileHeader.getFileName(), zipFile);
					} else {
						throw new Exception("Zip file '" + importFilePathOrData + "' does not include defined zipped file '" + zippedFilePathAndName + "'");
					}
				}
			} else {
				throw new Exception("Zip file '" + importFilePathOrData + "' is empty");
			}
		} catch (final Exception e) {
			try {
				if (zipFile != null) {
					zipFile.close();
				}
			} catch (final IOException e1) {
				// Do nothing
			}
			throw e;
		}
	}
}
