package de.soderer.utilities.zip;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class Zip4jUtilities {
	public static void createPasswordSecuredZipFile(final String originalZipFilePath, final char[] zipPassword, final boolean useZipCrypto) throws IOException {
		throw new IOException("Password secured zip files are not supported");
	}

	public static long getUncompressedSize(final File zipFilePath, final char[] zipPassword) throws IOException {
		throw new IOException("Password secured zip files are not supported");
	}

	public static InputStream openPasswordSecuredZipFile(final String importFilePathOrData, final char[] zipPassword) throws Exception {
		throw new Exception("Password secured zip files are not supported");
	}
}
