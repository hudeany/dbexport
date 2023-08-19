package de.soderer.utilities.kdbx.data;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;

import de.soderer.utilities.kdbx.data.KdbxConstants.InnerEncryptionAlgorithm;
import de.soderer.utilities.kdbx.data.KdbxConstants.KdbxVersion;
import de.soderer.utilities.kdbx.data.KdbxConstants.OuterEncryptionAlgorithm;
import de.soderer.utilities.kdbx.util.CopyInputStream;
import de.soderer.utilities.kdbx.util.TypeLengthValueStructure;
import de.soderer.utilities.kdbx.util.Utilities;
import de.soderer.utilities.kdbx.util.Version;

public class KdbxHeaderFormat3 extends KdbxHeaderFormat {
	public static KdbxHeaderFormat3 read(final InputStream inputStream) throws Exception {
		final CopyInputStream copyInputStream = new CopyInputStream(inputStream);
		copyInputStream.setCopyOnRead(true);

		final KdbxHeaderFormat3 header = new KdbxHeaderFormat3();

		header.setDataFormatVersion(readKdbxDataFormatVersion(copyInputStream));

		TypeLengthValueStructure nextStructure;
		while ((nextStructure = TypeLengthValueStructure.read(copyInputStream, false)).getTypeId() != 0) {
			switch(nextStructure.getTypeId()) {
				case 2:
					header.setOuterEncryptionAlgorithm(OuterEncryptionAlgorithm.getById(nextStructure.getData()));
					break;
				case 3:
					header.setCompressData((Utilities.readIntFromLittleEndianBytes(nextStructure.getData()) & 1) == 1);
					break;
				case 4:
					header.setMasterSeed(nextStructure.getData());
					break;
				case 5:
					header.setTransformSeed(nextStructure.getData());
					break;
				case 6:
					header.setTransformRounds(Utilities.readLittleEndianValueFromByteArray(nextStructure.getData()));
					break;
				case 7:
					header.setEncryptionIV(nextStructure.getData());
					break;
				case 8:
					header.setInnerEncryptionKeyBytes(nextStructure.getData());
					break;
				case 9:
					header.setStreamStartBytes(nextStructure.getData());
					break;
				case 10:
					header.setInnerEncryptionAlgorithm(InnerEncryptionAlgorithm.getById(Utilities.readIntFromLittleEndianBytes(nextStructure.getData())));
					break;
				default:
					throw new Exception("Invalid header type id: " + Integer.toHexString(nextStructure.getTypeId()));
			}
		}

		header.headerBytes = copyInputStream.getCopiedData();
		copyInputStream.setCopyOnRead(false);

		return header;
	}

	private byte[] headerBytes;

	private Version dataFormatVersion = new Version(3, 1, 0);

	//CIPHER_ID(2)
	private OuterEncryptionAlgorithm outerEncryptionAlgorithm = OuterEncryptionAlgorithm.AES_256;

	//COMPRESSION_FLAGS(3)
	private boolean compressData = true;

	//MASTER_SEED(4)
	private byte[] masterSeed;

	//TRANSFORM_SEED(5)
	private byte[] transformSeed;

	//TRANSFORM_ROUNDS(6)
	private long transformRounds = 60000;

	//ENCRYPTION_IV(7)
	private byte[] encryptionIV;

	//PROTECTED_STREAM_KEY(8)
	private byte[] innerEncryptionKeyBytes;

	//STREAM_START_BYTES(9)
	private byte[] streamStartBytes;

	//INNER_RANDOM_STREAM_ID(10)
	private InnerEncryptionAlgorithm innerEncryptionAlgorithm = InnerEncryptionAlgorithm.CHACHA20;

	@Override
	public byte[] getHeaderBytes() throws Exception {
		if (headerBytes == null) {
			masterSeed = new byte[32];
			new SecureRandom().nextBytes(masterSeed);

			transformSeed = new byte[32];
			new SecureRandom().nextBytes(transformSeed);

			if (outerEncryptionAlgorithm == OuterEncryptionAlgorithm.CHACHA20) {
				encryptionIV = new byte[12];
			} else {
				encryptionIV = new byte[16];
			}
			new SecureRandom().nextBytes(encryptionIV);

			innerEncryptionKeyBytes = new byte[32];
			new SecureRandom().nextBytes(innerEncryptionKeyBytes);

			streamStartBytes = new byte[32];
			new SecureRandom().nextBytes(streamStartBytes);

			final ByteArrayOutputStream outerHeaderBufferStream = new ByteArrayOutputStream();

			outerHeaderBufferStream.write(Utilities.getLittleEndianBytes(KdbxConstants.KDBX_MAGICNUMBER));
			outerHeaderBufferStream.write(Utilities.getLittleEndianBytes(KdbxVersion.KEEPASS2.getVersionId()));
			outerHeaderBufferStream.write(Utilities.getLittleEndianBytes((short) dataFormatVersion.getMinorVersionNumber()));
			outerHeaderBufferStream.write(Utilities.getLittleEndianBytes((short) dataFormatVersion.getMajorVersionNumber()));
			new TypeLengthValueStructure(2, outerEncryptionAlgorithm.getId()).write(outerHeaderBufferStream, false);
			new TypeLengthValueStructure(3, Utilities.getLittleEndianBytes(compressData ? 1 : 0)).write(outerHeaderBufferStream, false);
			new TypeLengthValueStructure(4, masterSeed).write(outerHeaderBufferStream, false);
			new TypeLengthValueStructure(5, transformSeed).write(outerHeaderBufferStream, false);
			new TypeLengthValueStructure(6, Utilities.getLittleEndianBytes(transformRounds)).write(outerHeaderBufferStream, false);
			new TypeLengthValueStructure(7, encryptionIV).write(outerHeaderBufferStream, false);
			new TypeLengthValueStructure(8, innerEncryptionKeyBytes).write(outerHeaderBufferStream, false);
			new TypeLengthValueStructure(9, streamStartBytes).write(outerHeaderBufferStream, false);
			new TypeLengthValueStructure(10, Utilities.getLittleEndianBytes(innerEncryptionAlgorithm.getId())).write(outerHeaderBufferStream, false);
			new TypeLengthValueStructure(0, new byte[] {0x0D, 0x0A, 0x0D, 0x0A}).write(outerHeaderBufferStream, false);

			headerBytes = outerHeaderBufferStream.toByteArray();
		}
		return headerBytes;
	}

	@Override
	public Version getDataFormatVersion() {
		return dataFormatVersion;
	}

	public KdbxHeaderFormat3 setDataFormatVersion(final Version dataFormatVersion) {
		headerBytes = null;
		if (dataFormatVersion.getMajorVersionNumber() != 3) {
			throw new IllegalArgumentException("Invalid major data version for storage format settings of version 3");
		} else {
			this.dataFormatVersion = dataFormatVersion;
			return this;
		}
	}

	public long getTransformRounds() {
		return transformRounds;
	}

	public KdbxHeaderFormat3 setTransformRounds(final long transformRounds) {
		headerBytes = null;
		this.transformRounds = transformRounds;
		return this;
	}

	@Override
	public boolean isCompressData() {
		return compressData;
	}

	public KdbxHeaderFormat3 setCompressData(final boolean compressData) {
		headerBytes = null;
		this.compressData = compressData;
		return this;
	}

	@Override
	public OuterEncryptionAlgorithm getOuterEncryptionAlgorithm() {
		return outerEncryptionAlgorithm;
	}

	@Override
	public KdbxHeaderFormat3 setOuterEncryptionAlgorithm(final OuterEncryptionAlgorithm outerEncryptionAlgorithm) {
		headerBytes = null;
		if (outerEncryptionAlgorithm == null) {
			this.outerEncryptionAlgorithm = OuterEncryptionAlgorithm.AES_256;
		} else {
			this.outerEncryptionAlgorithm = outerEncryptionAlgorithm;
		}
		return this;
	}

	@Override
	public InnerEncryptionAlgorithm getInnerEncryptionAlgorithm() {
		return innerEncryptionAlgorithm;
	}

	@Override
	public KdbxHeaderFormat3 setInnerEncryptionAlgorithm(final InnerEncryptionAlgorithm innerEncryptionAlgorithm) {
		headerBytes = null;
		if (innerEncryptionAlgorithm == null) {
			this.innerEncryptionAlgorithm = InnerEncryptionAlgorithm.CHACHA20;
		} else {
			this.innerEncryptionAlgorithm = innerEncryptionAlgorithm;
		}
		return this;
	}

	public byte[] getMasterSeed() {
		return masterSeed;
	}

	public KdbxHeaderFormat3 setMasterSeed(final byte[] masterSeed) {
		headerBytes = null;
		this.masterSeed = masterSeed;
		return this;
	}

	public byte[] getTransformSeed() {
		return transformSeed;
	}

	public KdbxHeaderFormat3 setTransformSeed(final byte[] transformSeed) {
		headerBytes = null;
		this.transformSeed = transformSeed;
		return this;
	}

	public byte[] getEncryptionIV() {
		return encryptionIV;
	}

	public KdbxHeaderFormat3 setEncryptionIV(final byte[] encryptionIV) {
		headerBytes = null;
		this.encryptionIV = encryptionIV;
		return this;
	}

	public byte[] getInnerEncryptionKeyBytes() {
		return innerEncryptionKeyBytes;
	}

	public KdbxHeaderFormat3 setInnerEncryptionKeyBytes(final byte[] innerEncryptionKeyBytes) {
		headerBytes = null;
		this.innerEncryptionKeyBytes = innerEncryptionKeyBytes;
		return this;
	}

	public byte[] getStreamStartBytes() {
		return streamStartBytes;
	}

	public KdbxHeaderFormat3 setStreamStartBytes(final byte[] streamStartBytes) {
		headerBytes = null;
		this.streamStartBytes = streamStartBytes;
		return this;
	}

	@Override
	public byte[] getEncryptionKey(final byte[] credentialsCompositeKeyBytes) throws Exception {
		if (credentialsCompositeKeyBytes == null || credentialsCompositeKeyBytes.length != 32) {
			throw new Exception("Cannot derive key");
		}
		final byte[] resultLeft = Utilities.deriveKeyByAES(transformSeed, transformRounds, Arrays.copyOfRange(credentialsCompositeKeyBytes, 0, 16));
		final byte[] resultRight = Utilities.deriveKeyByAES(transformSeed, transformRounds, Arrays.copyOfRange(credentialsCompositeKeyBytes, 16, 32));
		final byte[] transformed = Utilities.concatArrays(resultLeft, resultRight);
		return MessageDigest.getInstance("SHA-256").digest(transformed);
	}

	@Override
	public void resetCryptoKeys() {
		headerBytes = null;
		masterSeed = null;
		transformSeed = null;
		encryptionIV = null;
		innerEncryptionKeyBytes = null;
		streamStartBytes = null;
	}
}
