package de.soderer.utilities.kdbx.data;

import java.io.IOException;
import java.io.InputStream;

import de.soderer.utilities.kdbx.data.KdbxConstants.InnerEncryptionAlgorithm;
import de.soderer.utilities.kdbx.data.KdbxConstants.KdbxVersion;
import de.soderer.utilities.kdbx.data.KdbxConstants.OuterEncryptionAlgorithm;
import de.soderer.utilities.kdbx.util.Utilities;
import de.soderer.utilities.kdbx.util.Version;

public abstract class KdbxHeaderFormat {
	public static Version readKdbxDataFormatVersion(final InputStream inputStream) throws Exception {
		int magicNumber;
		try {
			magicNumber = Utilities.readLittleEndianIntFromStream(inputStream);
		} catch (final Exception e) {
			throw new Exception("Cannot read kdbx magic number", e);
		}
		if (magicNumber != KdbxConstants.KDBX_MAGICNUMBER) {
			throw new IOException("Data does not include kdbx data (Invalid magic number " + Integer.toHexString(magicNumber) + ")");
		}

		int kdbxVersionId;
		KdbxVersion kdbxVersion;
		try {
			kdbxVersionId = Utilities.readLittleEndianIntFromStream(inputStream);
		} catch (final Exception e) {
			throw new Exception("Cannot read kdbx version number", e);
		}
		try {
			kdbxVersion = KdbxVersion.getById(kdbxVersionId);
		} catch (final Exception e) {
			throw new Exception("Invalid kdbx version: " + e.getMessage(), e);
		}
		if (kdbxVersion != KdbxVersion.KEEPASS2) {
			throw new Exception("Unsupported kdbx version: " + kdbxVersion);
		}

		try {
			final short minorDataFormatVersion = Utilities.readLittleEndianShortFromStream(inputStream);
			final short majorDataFormatVersion = Utilities.readLittleEndianShortFromStream(inputStream);
			return new Version(majorDataFormatVersion, minorDataFormatVersion, 0);
		} catch (final Exception e) {
			throw new Exception("Cannot read kdbx data format version: " + e.getMessage(), e);
		}
	}

	public abstract Version getDataFormatVersion();

	public abstract byte[] getHeaderBytes() throws Exception;

	public abstract OuterEncryptionAlgorithm getOuterEncryptionAlgorithm();

	public abstract KdbxHeaderFormat setOuterEncryptionAlgorithm(OuterEncryptionAlgorithm outerEncryptionAlgorithm);

	public abstract InnerEncryptionAlgorithm getInnerEncryptionAlgorithm();

	public abstract KdbxHeaderFormat setInnerEncryptionAlgorithm(InnerEncryptionAlgorithm chacha20);

	public abstract boolean isCompressData();

	public abstract void resetCryptoKeys();

	public abstract byte[] getEncryptionKey(final byte[] credentialsCompositeKeyBytes) throws Exception;
}
