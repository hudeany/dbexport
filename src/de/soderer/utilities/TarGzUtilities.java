package de.soderer.utilities;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

public class TarGzUtilities {
	public static long getUncompressedSize(final File tarGzFile) throws Exception {
		if (!tarGzFile.exists()) {
			throw new Exception("TarGz file does not exist: " + tarGzFile.getAbsolutePath());
		} else if (!tarGzFile.isFile()) {
			throw new Exception("TarGz file path is not a file: " + tarGzFile.getAbsolutePath());
		} else {
			try (TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(new FileInputStream(tarGzFile))))) {
				TarArchiveEntry entry;
				long size = 0;
				while ((entry = tarArchiveInputStream.getNextTarEntry()) != null) {
					if (!entry.isDirectory()) {
						size += entry.getRealSize();
					}
				}
				return size;
			} catch (final Exception e) {
				throw new Exception("Cannot read '" + tarGzFile + "'", e);
			}
		}
	}

	public static int getFilesCount(final File tarGzFile) throws Exception {
		if (!tarGzFile.exists()) {
			throw new Exception("TarGz file does not exist: " + tarGzFile.getAbsolutePath());
		} else if (!tarGzFile.isFile()) {
			throw new Exception("TarGz file path is not a file: " + tarGzFile.getAbsolutePath());
		} else {
			try (TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(new FileInputStream(tarGzFile))))) {
				TarArchiveEntry entry;
				int count = 0;
				while ((entry = tarArchiveInputStream.getNextTarEntry()) != null) {
					if (!entry.isDirectory()) {
						count++;
					}
				}
				return count;
			} catch (final Exception e) {
				throw new Exception("Cannot read '" + tarGzFile + "'", e);
			}
		}
	}

	public static void decompress(final File tarGzFile, final File decompressToPath) throws Exception {
		if (!tarGzFile.exists()) {
			throw new Exception("TarGz file does not exist: " + tarGzFile.getAbsolutePath());
		} else if (!tarGzFile.isFile()) {
			throw new Exception("TarGz file path is not a file: " + tarGzFile.getAbsolutePath());
		} else if (decompressToPath.exists()) {
			throw new Exception("Destination path already exists: " + decompressToPath.getAbsolutePath());
		} else {
			try (TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(new FileInputStream(tarGzFile))))) {
				decompressToPath.mkdirs();

				TarArchiveEntry entry;
				while ((entry = tarArchiveInputStream.getNextTarEntry()) != null) {
					if (entry.isDirectory()) {
						continue;
					}
					String entryFilePath = entry.getName();
					entryFilePath = entryFilePath.replace("\\", "/");
					if (entryFilePath.startsWith("/") || entryFilePath.startsWith("../") || entryFilePath.endsWith("/..") || entryFilePath.contains("/../")) {
						throw new Exception("Traversal error in tar gz file: " + tarGzFile.getAbsolutePath());
					}
					final File currentfile = new File(decompressToPath, entryFilePath);
					if (!currentfile.getCanonicalPath().startsWith(decompressToPath.getCanonicalPath())) {
						throw new Exception("Traversal error in tar gz file: " + tarGzFile.getAbsolutePath() + "/");
					}
					final File parent = currentfile.getParentFile();
					if (!parent.exists()) {
						parent.mkdirs();
					}

					try(final FileOutputStream fileOutputStream = new FileOutputStream(currentfile)) {
						IoUtilities.copy(tarArchiveInputStream, fileOutputStream);
					}
				}
			} catch (final Exception e) {
				try {
					if (decompressToPath.exists()) {
						FileUtilities.delete(decompressToPath);
					}
				} catch (@SuppressWarnings("unused") final Exception e1) {
					// do nothing else
				}

				throw new Exception("Cannot decompress '" + tarGzFile + "'", e);
			}
		}
	}

	public static void compress(final File tarGzFile, final File fileToCompress, final String filePathInTarGzFile) throws IOException {
		if (tarGzFile.exists()) {
			throw new IOException("TarGz file already exists: " + tarGzFile.getAbsolutePath());
		} else if (!tarGzFile.getParentFile().exists()) {
			throw new IOException("Parent directory for TarGz file does not exist: " + tarGzFile.getParentFile().getAbsolutePath());
		}

		try (TarArchiveOutputStream tarArchiveOutputStream = new TarArchiveOutputStream(new GzipCompressorOutputStream(new BufferedOutputStream(new FileOutputStream(tarGzFile))));
				FileInputStream fileInputStream = new FileInputStream(fileToCompress)) {
			tarArchiveOutputStream.putArchiveEntry(new TarArchiveEntry(fileToCompress, filePathInTarGzFile));
			IoUtilities.copy(fileInputStream, tarArchiveOutputStream);
			tarArchiveOutputStream.closeArchiveEntry();
			tarArchiveOutputStream.finish();
		} catch (final IOException e) {
			if (tarGzFile.exists()) {
				tarGzFile.delete();
			}
			throw e;
		}
	}

	public static InputStream openCompressedFile(final File tarGzFile) throws Exception {
		return openCompressedFile(tarGzFile, null);
	}

	public static InputStream openCompressedFile(final File tarGzFile, final String filePathInTarGzFile) throws Exception {
		if (!tarGzFile.exists()) {
			throw new Exception("TarGz file does not exist: " + tarGzFile.getAbsolutePath());
		} else if (!tarGzFile.isFile()) {
			throw new Exception("TarGz file path is not a file: " + tarGzFile.getAbsolutePath());
		} else {
			TarArchiveInputStream tarArchiveInputStream = null;
			try {
				tarArchiveInputStream = new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(new FileInputStream(tarGzFile))));
				TarArchiveEntry entry = null;
				while ((entry = tarArchiveInputStream.getNextTarEntry()) != null) {
					if (entry.isDirectory()) {
						continue;
					} else {
						if (Utilities.isBlank(filePathInTarGzFile)) {
							return tarArchiveInputStream;
						} else {
							String entryFilePath = entry.getName();
							entryFilePath = entryFilePath.replace("\\", "/");
							if (entryFilePath.equals(filePathInTarGzFile)) {
								return tarArchiveInputStream;
							}
						}
					}
				}

				if (Utilities.isBlank(filePathInTarGzFile)) {
					throw new Exception("TarGz file '" + tarGzFile.getAbsolutePath() + "' is empty");
				} else {
					throw new Exception("TarGz file does not contain file '" + filePathInTarGzFile + "'");
				}
			} catch (final Exception e) {
				try {
					if (tarArchiveInputStream != null) {
						tarArchiveInputStream.close();
					}
				} catch (@SuppressWarnings("unused") final IOException e1) {
					// Do nothing
				}
				throw e;
			}
		}
	}
}
