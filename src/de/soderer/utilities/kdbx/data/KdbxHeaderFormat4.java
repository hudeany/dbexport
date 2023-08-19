package de.soderer.utilities.kdbx.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bouncycastle.crypto.generators.Argon2BytesGenerator;
import org.bouncycastle.crypto.params.Argon2Parameters;

import de.soderer.utilities.kdbx.KdbxDatabase;
import de.soderer.utilities.kdbx.data.KdbxConstants.InnerEncryptionAlgorithm;
import de.soderer.utilities.kdbx.data.KdbxConstants.KdbxVersion;
import de.soderer.utilities.kdbx.data.KdbxConstants.KeyDerivationFunction;
import de.soderer.utilities.kdbx.data.KdbxConstants.OuterEncryptionAlgorithm;
import de.soderer.utilities.kdbx.util.CopyInputStream;
import de.soderer.utilities.kdbx.util.TypeLengthValueStructure;
import de.soderer.utilities.kdbx.util.Utilities;
import de.soderer.utilities.kdbx.util.VariantDictionary;
import de.soderer.utilities.kdbx.util.Version;

public class KdbxHeaderFormat4 extends KdbxHeaderFormat {
	public static KdbxHeaderFormat4 read(final InputStream inputStream) throws Exception {
		final CopyInputStream copyInputStream = new CopyInputStream(inputStream);
		copyInputStream.setCopyOnRead(true);

		final KdbxHeaderFormat4 header = new KdbxHeaderFormat4();

		header.setDataFormatVersion(readKdbxDataFormatVersion(copyInputStream));

		TypeLengthValueStructure nextStructure;
		while ((nextStructure = TypeLengthValueStructure.read(copyInputStream, true)).getTypeId() != 0) {
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
				case 7:
					header.setEncryptionIV(nextStructure.getData());
					break;
				case 11:
					header.setKdfParamsBytes(nextStructure.getData());
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

	private Version dataFormatVersion = new Version(4, 1, 0);

	//CIPHER_ID(2)
	private OuterEncryptionAlgorithm outerEncryptionAlgorithm = OuterEncryptionAlgorithm.AES_256;

	//COMPRESSION_FLAGS(3)
	private boolean compressData = true;

	//MASTER_SEED(4)
	private byte[] masterSeed;

	//ENCRYPTION_IV(7)
	private byte[] encryptionIV;

	//KDF_PARAMETERS(11)
	private KeyDerivationFunctionInfo keyDerivationFunctionInfo;

	//Inner INNER_RANDOM_STREAM_ID(1)
	private InnerEncryptionAlgorithm innerEncryptionAlgorithm = InnerEncryptionAlgorithm.CHACHA20;

	//Inner INNER_RANDOM_STREAM_KEY(2)
	private byte[] innerEncryptionKeyBytes;

	//Inner BINARY_ATTACHMENT(3);
	List<KdbxBinary> binaryAttachments = new ArrayList<>();

	@Override
	public byte[] getHeaderBytes() throws Exception {
		if (headerBytes == null) {
			masterSeed = new byte[32];
			new SecureRandom().nextBytes(masterSeed);

			if (outerEncryptionAlgorithm == OuterEncryptionAlgorithm.CHACHA20) {
				encryptionIV = new byte[12];
			} else {
				encryptionIV = new byte[16];
			}
			new SecureRandom().nextBytes(encryptionIV);

			innerEncryptionKeyBytes = new byte[32];
			new SecureRandom().nextBytes(innerEncryptionKeyBytes);

			final ByteArrayOutputStream outerHeaderBufferStream = new ByteArrayOutputStream();

			outerHeaderBufferStream.write(Utilities.getLittleEndianBytes(KdbxConstants.KDBX_MAGICNUMBER));
			outerHeaderBufferStream.write(Utilities.getLittleEndianBytes(KdbxVersion.KEEPASS2.getVersionId()));
			outerHeaderBufferStream.write(Utilities.getLittleEndianBytes((short) dataFormatVersion.getMinorVersionNumber()));
			outerHeaderBufferStream.write(Utilities.getLittleEndianBytes((short) dataFormatVersion.getMajorVersionNumber()));
			new TypeLengthValueStructure(2, outerEncryptionAlgorithm.getId()).write(outerHeaderBufferStream, true);
			new TypeLengthValueStructure(3, Utilities.getLittleEndianBytes(compressData ? 1 : 0)).write(outerHeaderBufferStream, true);
			new TypeLengthValueStructure(4, masterSeed).write(outerHeaderBufferStream, true);
			new TypeLengthValueStructure(7, encryptionIV).write(outerHeaderBufferStream, true);
			new TypeLengthValueStructure(11, getKeyDerivationFunctionInfo().getKdfParamsBytes()).write(outerHeaderBufferStream, true);
			new TypeLengthValueStructure(0, new byte[] {0x0D, 0x0A, 0x0D, 0x0A}).write(outerHeaderBufferStream, true);

			headerBytes = outerHeaderBufferStream.toByteArray();
		}
		return headerBytes;
	}

	public byte[] getInnerHeaderBytes(final KdbxDatabase database) throws Exception {
		final ByteArrayOutputStream innerHeaderBufferStream = new ByteArrayOutputStream();
		new TypeLengthValueStructure(1, Utilities.getLittleEndianBytes(innerEncryptionAlgorithm.getId())).write(innerHeaderBufferStream, true);
		new TypeLengthValueStructure(2, innerEncryptionKeyBytes).write(innerHeaderBufferStream, true);

		for (final KdbxBinary binaryAttachment : database.getBinaryAttachments()) {
			byte[] binaryAttachmentData = binaryAttachment.getData();
			if (binaryAttachment.isCompressed()) {
				binaryAttachmentData = Utilities.gunzip(binaryAttachmentData);
			}
			final boolean isEncrypted = false;
			final byte flags = isEncrypted ? 1 : 0;
			binaryAttachmentData = Utilities.concatArrays(new byte[] { flags }, binaryAttachmentData);
			new TypeLengthValueStructure(3, binaryAttachmentData).write(innerHeaderBufferStream, true);
		}
		new TypeLengthValueStructure(0, new byte[0]).write(innerHeaderBufferStream, true);
		return innerHeaderBufferStream.toByteArray();
	}

	public void readInnerHeader(final InputStream dataInputStream) throws Exception {
		binaryAttachments = new ArrayList<>();
		final Map<Integer, byte[]> innerHeaders = new LinkedHashMap<>();
		TypeLengthValueStructure nextInnerHeaderStructure;
		while ((nextInnerHeaderStructure = TypeLengthValueStructure.read(dataInputStream, true)).getTypeId() != 0) {
			if (nextInnerHeaderStructure.getTypeId() == 3) {
				// BINARY_ATTACHMENT data is referenced by entries via position id in data version 4.0 and higher
				final KdbxBinary binaryAttachment = new KdbxBinary();
				binaryAttachment.setId(binaryAttachments.size());
				byte[] databaseBinaryData = nextInnerHeaderStructure.getData();
				final byte flags = databaseBinaryData[0];
				final boolean isEncrypted = (flags & 1) == 1;
				if (isEncrypted) {
					throw new Exception("Memoryprotection for attachments is not supported yet");
				}
				databaseBinaryData = Arrays.copyOfRange(databaseBinaryData, 1, databaseBinaryData.length);
				binaryAttachment.setData(databaseBinaryData);
				binaryAttachments.add(binaryAttachment);
			} else {
				innerHeaders.put(nextInnerHeaderStructure.getTypeId(), nextInnerHeaderStructure.getData());
			}
		}

		innerEncryptionAlgorithm = InnerEncryptionAlgorithm.getById(Utilities.readIntFromLittleEndianBytes(innerHeaders.get(1)));
		innerEncryptionKeyBytes = innerHeaders.get(2);
	}

	@Override
	public Version getDataFormatVersion() {
		return dataFormatVersion;
	}

	public KdbxHeaderFormat4 setDataFormatVersion(final Version dataFormatVersion) {
		headerBytes = null;
		if (dataFormatVersion.getMajorVersionNumber() != 4) {
			throw new IllegalArgumentException("Invalid major data version for storage format settings of version 4");
		} else {
			this.dataFormatVersion = dataFormatVersion;
			return this;
		}
	}

	@Override
	public boolean isCompressData() {
		return compressData;
	}

	public KdbxHeaderFormat4 setCompressData(final boolean compressData) {
		headerBytes = null;
		this.compressData = compressData;
		return this;
	}

	@Override
	public OuterEncryptionAlgorithm getOuterEncryptionAlgorithm() {
		return outerEncryptionAlgorithm;
	}

	@Override
	public KdbxHeaderFormat4 setOuterEncryptionAlgorithm(final OuterEncryptionAlgorithm outerEncryptionAlgorithm) {
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
	public KdbxHeaderFormat4 setInnerEncryptionAlgorithm(final InnerEncryptionAlgorithm innerEncryptionAlgorithm) {
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

	public KdbxHeaderFormat4 setMasterSeed(final byte[] masterSeed) {
		if (masterSeed.length != 32) {
			throw new IllegalStateException("Master seed should have 32 bytes");
		} else {
			headerBytes = null;
			this.masterSeed = masterSeed;
			return this;
		}
	}

	public byte[] getEncryptionIV() {
		return encryptionIV;
	}

	public KdbxHeaderFormat4 setEncryptionIV(final byte[] encryptionIV) {
		headerBytes = null;
		this.encryptionIV = encryptionIV;
		return this;
	}

	public KeyDerivationFunctionInfo getKeyDerivationFunctionInfo() {
		if (keyDerivationFunctionInfo == null) {
			keyDerivationFunctionInfo = new KeyDerivationFunctionInfoArgon();
		}
		return keyDerivationFunctionInfo;
	}

	public KdbxHeaderFormat4 setKeyDerivationFunctionInfo(final KeyDerivationFunctionInfo keyDerivationFunctionInfo) {
		this.keyDerivationFunctionInfo = keyDerivationFunctionInfo;
		return this;
	}

	public KdbxHeaderFormat4 setKdfParamsBytes(final byte[] kdfParamsBytes) throws Exception {
		headerBytes = null;
		final VariantDictionary variantDictionary = VariantDictionary.read(new ByteArrayInputStream(kdfParamsBytes));
		final KeyDerivationFunction keyDerivationFunction = KeyDerivationFunction.getById((byte[]) variantDictionary.get(VariantDictionary.KDF_UUID).getJavaValue());
		switch (keyDerivationFunction) {
			case AES_KDBX3:
			case AES_KDBX4:
				keyDerivationFunctionInfo = new KeyDerivationFunctionInfoAes().setValues(variantDictionary);
				break;
			case ARGON2D:
			case ARGON2ID:
				keyDerivationFunctionInfo = new KeyDerivationFunctionInfoArgon().setValues(variantDictionary);
				break;
			default:
				throw new Exception("Unknown KeyDerivationFunction(KDF): " + keyDerivationFunction);
		}
		return this;
	}

	public byte[] getInnerEncryptionKeyBytes() {
		return innerEncryptionKeyBytes;
	}

	public KdbxHeaderFormat4 setInnerEncryptionKeyBytes(final byte[] innerEncryptionKeyBytes) {
		headerBytes = null;
		this.innerEncryptionKeyBytes = innerEncryptionKeyBytes;
		return this;
	}

	public List<KdbxBinary> getBinaryAttachments() {
		return binaryAttachments;
	}

	public KdbxHeaderFormat4 setBinaryAttachments(final List<KdbxBinary> binaryAttachments) {
		headerBytes = null;
		this.binaryAttachments = binaryAttachments;
		return this;
	}

	@Override
	public byte[] getEncryptionKey(final byte[] credentialsCompositeKeyBytes) throws Exception {
		if (credentialsCompositeKeyBytes == null || credentialsCompositeKeyBytes.length != 32) {
			throw new Exception("Cannot derive key");
		}
		if (keyDerivationFunctionInfo instanceof KeyDerivationFunctionInfoAes) {
			final KeyDerivationFunctionInfoAes keyDerivationFunctionInfoAes = (KeyDerivationFunctionInfoAes) keyDerivationFunctionInfo;
			final long aesTransformRounds = keyDerivationFunctionInfoAes.getAesTransformRounds();
			final byte[] aesTransformSeed = keyDerivationFunctionInfoAes.getAesTransformSeed();
			final byte[] resultLeft = Utilities.deriveKeyByAES(aesTransformSeed, aesTransformRounds, Arrays.copyOfRange(credentialsCompositeKeyBytes, 0, 16));
			final byte[] resultRight = Utilities.deriveKeyByAES(aesTransformSeed, aesTransformRounds, Arrays.copyOfRange(credentialsCompositeKeyBytes, 16, 32));
			final byte[] transformed = Utilities.concatArrays(resultLeft, resultRight);
			return MessageDigest.getInstance("SHA-256").digest(transformed);
		} else if (keyDerivationFunctionInfo instanceof KeyDerivationFunctionInfoArgon) {
			final KeyDerivationFunctionInfoArgon keyDerivationFunctionInfoArgon = (KeyDerivationFunctionInfoArgon) keyDerivationFunctionInfo;
			final Argon2Parameters.Builder builder = new Argon2Parameters.Builder(keyDerivationFunctionInfoArgon.getType().getArgon2TypeID());
			builder.withIterations(keyDerivationFunctionInfoArgon.getIterations());
			builder.withMemoryAsKB((int) (keyDerivationFunctionInfoArgon.getMemoryInBytes() / 1024));
			builder.withParallelism(keyDerivationFunctionInfoArgon.getParallelism());
			builder.withSalt(keyDerivationFunctionInfoArgon.getSalt());
			builder.withVersion(keyDerivationFunctionInfoArgon.getVersion());

			final Argon2Parameters parameters = builder.build();
			final Argon2BytesGenerator generator = new Argon2BytesGenerator();
			generator.init(parameters);

			final byte[] output = new byte[32];
			generator.generateBytes(credentialsCompositeKeyBytes, output);
			return output;
		} else {
			throw new Exception("Unknown KeyDerivationFunction (KDF)");
		}
	}

	@Override
	public void resetCryptoKeys() {
		headerBytes = null;
		masterSeed = null;
		encryptionIV = null;
		innerEncryptionKeyBytes = null;
		if (keyDerivationFunctionInfo != null) {
			keyDerivationFunctionInfo.resetCryptoKeys();
		}
	}
}
