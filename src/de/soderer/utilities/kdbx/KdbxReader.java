package de.soderer.utilities.kdbx;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.Security;
import java.security.spec.AlgorithmParameterSpec;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.Mac;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.bouncycastle.crypto.StreamCipher;
import org.bouncycastle.crypto.engines.ChaCha7539Engine;
import org.bouncycastle.crypto.engines.Salsa20Engine;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.crypto.params.ParametersWithIV;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import de.soderer.utilities.kdbx.data.KdbxBinary;
import de.soderer.utilities.kdbx.data.KdbxConstants.InnerEncryptionAlgorithm;
import de.soderer.utilities.kdbx.data.KdbxConstants.OuterEncryptionAlgorithm;
import de.soderer.utilities.kdbx.data.KdbxConstants.PayloadBlockType;
import de.soderer.utilities.kdbx.data.KdbxCustomDataItem;
import de.soderer.utilities.kdbx.data.KdbxEntry;
import de.soderer.utilities.kdbx.data.KdbxEntryBinary;
import de.soderer.utilities.kdbx.data.KdbxGroup;
import de.soderer.utilities.kdbx.data.KdbxHeaderFormat;
import de.soderer.utilities.kdbx.data.KdbxHeaderFormat3;
import de.soderer.utilities.kdbx.data.KdbxHeaderFormat4;
import de.soderer.utilities.kdbx.data.KdbxMemoryProtection;
import de.soderer.utilities.kdbx.data.KdbxMeta;
import de.soderer.utilities.kdbx.data.KdbxTimes;
import de.soderer.utilities.kdbx.data.KdbxUUID;
import de.soderer.utilities.kdbx.util.HmacInputStream;
import de.soderer.utilities.kdbx.util.TypeHashLengthValueStructure;
import de.soderer.utilities.kdbx.util.Utilities;
import de.soderer.utilities.kdbx.util.Version;

public class KdbxReader implements AutoCloseable {
	private boolean strictMode = false;

	private final InputStream inputStream;

	private StreamCipher innerEncryptionCipher;

	public KdbxReader(final InputStream inputStream) {
		this.inputStream = inputStream;
	}

	public KdbxReader setStrictMode(final boolean strictMode) {
		this.strictMode = strictMode;
		return this;
	}

	public boolean isStrictMode() {
		return strictMode;
	}

	public KdbxDatabase readKdbxDatabase(final char[] password) throws Exception {
		return readKdbxDatabase(new KdbxCredentials(password));
	}

	public KdbxDatabase readKdbxDatabase(final KdbxCredentials credentials) throws Exception {
		try (BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream)) {
			bufferedInputStream.mark(1024);
			final Version dataFormatVersion = KdbxHeaderFormat.readKdbxDataFormatVersion(bufferedInputStream);
			bufferedInputStream.reset();

			final KdbxDatabase database = new KdbxDatabase();
			if (dataFormatVersion.getMajorVersionNumber() == 3) {
				return readDataFormat3(credentials, bufferedInputStream, dataFormatVersion, database);
			} else if (dataFormatVersion.getMajorVersionNumber() == 4) {
				return readDataFormat4(credentials, bufferedInputStream, dataFormatVersion, database);
			} else {
				throw new Exception("Major kdbx file data format version " + dataFormatVersion.getMajorVersionNumber() + " is not supported");
			}
		}
	}

	private KdbxDatabase readDataFormat3(final KdbxCredentials credentials, final InputStream dataInputStream, final Version dataFormatVersion, final KdbxDatabase database) throws Exception {
		final Document document;
		final List<KdbxBinary> binaryAttachments = new ArrayList<>();
		database.setBinaryAttachments(binaryAttachments);
		final KdbxHeaderFormat3 headerFormat3 = KdbxHeaderFormat3.read(dataInputStream);
		database.setHeaderFormat(headerFormat3);
		final byte[] decryptionKey = headerFormat3.getEncryptionKey(credentials.createCompositeKeyHash());

		final byte[] encryptedData = Utilities.toByteArray(dataInputStream);
		byte[] decryptedData;
		try {
			final byte[] transformedKey = Utilities.concatArrays(headerFormat3.getMasterSeed(), decryptionKey);
			final byte[] finalKey = MessageDigest.getInstance("SHA-256").digest(transformedKey);
			final OuterEncryptionAlgorithm outerEncryptionAlgorithm = headerFormat3.getOuterEncryptionAlgorithm();
			if (outerEncryptionAlgorithm != OuterEncryptionAlgorithm.AES_128 && outerEncryptionAlgorithm != OuterEncryptionAlgorithm.AES_256) {
				throw new IllegalArgumentException("Cipher " + outerEncryptionAlgorithm + " is not implemented yet");
			}
			final Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
			final SecretKeySpec secretKeySpec = new SecretKeySpec(finalKey, "AES");
			final AlgorithmParameterSpec paramSpec = new IvParameterSpec(headerFormat3.getEncryptionIV());
			cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, paramSpec);
			decryptedData = cipher.doFinal(encryptedData);
		} catch (final BadPaddingException e) {
			throw new Exception("KDBX database decryption failed. Maybe the given credentials are wrong.", e);
		} catch (final Exception e) {
			throw new Exception("KDBX database decryption failed. Maybe the given credentials are wrong.", e);
		}

		final ByteArrayInputStream decryptedPayloadStream = new ByteArrayInputStream(decryptedData);
		final byte[] expectedStartBytes = headerFormat3.getStreamStartBytes();
		final byte[] actualStartBytes = new byte[expectedStartBytes.length];
		final int readBytes = decryptedPayloadStream.read(actualStartBytes);
		if (readBytes != expectedStartBytes.length) {
			throw new Exception("Cannot read start bytes from payload: Not enough data left");
		} else if (!Arrays.equals(expectedStartBytes, actualStartBytes)) {
			throw new Exception("KDBX database decryption failed. Maybe the given credentials are wrong.");
		}

		final ByteArrayOutputStream payloadData = new ByteArrayOutputStream();
		TypeHashLengthValueStructure nextTypeHashLengthValueStructure;
		do {
			nextTypeHashLengthValueStructure = TypeHashLengthValueStructure.read(decryptedPayloadStream, "SHA-256");
			if (nextTypeHashLengthValueStructure.getTypeId() == PayloadBlockType.PAYLOAD.getId()) {
				payloadData.write(nextTypeHashLengthValueStructure.getData());
			}
		} while (nextTypeHashLengthValueStructure.getTypeId() != PayloadBlockType.END_OF_PAYLOAD.getId());

		innerEncryptionCipher = createInnerEncryptionCipher(headerFormat3.getInnerEncryptionAlgorithm(), headerFormat3.getInnerEncryptionKeyBytes());

		byte[] decryptedPayload = payloadData.toByteArray();
		if (headerFormat3.isCompressData()) {
			decryptedPayload = Utilities.gunzip(decryptedPayload);
		}
		final byte[] decryptedXmlPayloadData = decryptedPayload;
		document = Utilities.parseXmlFile(decryptedXmlPayloadData);

		final Node rootNode = document.getDocumentElement();
		if (!"KeePassFile".equals(rootNode.getNodeName())) {
			if (strictMode) {
				throw new Exception("Unexpected xml root node name: " + rootNode.getNodeName());
			}
		}
		final NodeList childNodes = rootNode.getChildNodes();
		for (int i = 0; i < childNodes.getLength(); i++) {
			final Node childNode = childNodes.item(i);
			if (childNode.getNodeType() != Node.TEXT_NODE) {
				if ("Meta".equals(childNode.getNodeName())) {
					database.setMeta(readKdbxMetaData(dataFormatVersion, childNode, binaryAttachments));
				} else if ("Root".equals(childNode.getNodeName())) {
					readRoot(dataFormatVersion, database, childNode);
				} else {
					if (strictMode) {
						throw new Exception("Unexpected data node name: " + childNode.getNodeName());
					}
				}
			}
		}

		// Check header hash by given value in decrypted kdbx xml meta data
		final byte[] actualSha256 = MessageDigest.getInstance("SHA-256").digest(headerFormat3.getHeaderBytes());
		if (!Arrays.equals(actualSha256, Base64.getDecoder().decode(database.getMeta().getHeaderHash()))) {
			throw new Exception("Outer header data corrupted, SHA-256 hashes do not match");
		}

		database.getHeaderFormat().resetCryptoKeys();

		for (final KdbxEntry entry : database.getAllEntries()) {
			for (final KdbxEntryBinary binary : entry.getBinaries()) {
				if (binary.getRefId() != null) {
					final KdbxBinary databaseBinary = database.getBinaryAttachments().get(binary.getRefId());
					if (databaseBinary != null) {
						if (databaseBinary.isCompressed()) {
							binary.setCompressedData(databaseBinary.getData());
						} else {
							binary.setCompressedData(Utilities.gzip(databaseBinary.getData()));
						}
					} else {
						throw new Exception("Cannot find referenced binary id: " + binary.getRefId());
					}
				}
			}
		}

		return database;
	}

	private KdbxDatabase readDataFormat4(final KdbxCredentials credentials, InputStream dataInputStream, final Version dataFormatVersion, final KdbxDatabase database) throws Exception {
		final Document document;
		final KdbxHeaderFormat4 headerFormat4 = KdbxHeaderFormat4.read(dataInputStream);
		database.setHeaderFormat(headerFormat4);
		final byte[] decryptionKey = headerFormat4.getEncryptionKey(credentials.createCompositeKeyHash());

		// SHA-256 Hash verification of headerBytes
		final byte[] actualSha256 = MessageDigest.getInstance("SHA-256").digest(headerFormat4.getHeaderBytes());
		final byte[] expectedSha256 = new byte[32];
		final int readBytesExpectedSha256 = dataInputStream.read(expectedSha256);
		if (readBytesExpectedSha256 != expectedSha256.length) {
			throw new IllegalStateException("Cannot read header SHA-256 hash bytes");
		} else if (!Arrays.equals(actualSha256, expectedSha256)) {
			throw new Exception("Outer header data corrupted, SHA-256 hashes do not match");
		}

		final byte[] transformedKey = Utilities.concatArrays(headerFormat4.getMasterSeed(), decryptionKey);
		final byte[] finalKey = MessageDigest.getInstance("SHA-256").digest(transformedKey);

		final MessageDigest digest = MessageDigest.getInstance("SHA-512");
		digest.update(headerFormat4.getMasterSeed());
		digest.update(decryptionKey);
		final byte[] hmacKey = digest.digest(new byte[] { 0x01 });

		// HMAC-SHA-256 verification of headerBytes
		final byte[] expectedHeaderHMAC = new byte[32];
		final int readBytesExpectedHeaderHMAC = dataInputStream.read(expectedHeaderHMAC);
		if (readBytesExpectedHeaderHMAC != expectedHeaderHMAC.length) {
			throw new IllegalStateException("Cannot read HMAC code bytes");
		}
		final byte[] indexBytes = Utilities.getLittleEndianBytes(0xFFFFFFFF_FFFFFFFFL);
		final MessageDigest headerVerificationKeyDigest = MessageDigest.getInstance("SHA-512");
		headerVerificationKeyDigest.update(indexBytes);
		final byte[] headerVerificationHmacKey = headerVerificationKeyDigest.digest(hmacKey);

		final Mac sha256_HMAC = Mac.getInstance("HmacSHA256");
		sha256_HMAC.init(new SecretKeySpec(headerVerificationHmacKey, "HmacSHA256"));
		sha256_HMAC.update(headerFormat4.getHeaderBytes());
		final byte[] actualHeaderHMAC = sha256_HMAC.doFinal();
		if (!Arrays.equals(actualHeaderHMAC, expectedHeaderHMAC)) {
			// When SHA-256 checksum was valid, then this means, that the credentials for decryption are wrong.
			throw new Exception("KDBX database decryption failed. Maybe the given credentials are wrong.");
		}

		dataInputStream = new HmacInputStream(dataInputStream, hmacKey);

		final Cipher cipher;
		final SecretKeySpec secretKeySpec;
		final AlgorithmParameterSpec paramSpec;
		switch (headerFormat4.getOuterEncryptionAlgorithm()) {
			case AES_128:
			case AES_256:
				cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
				secretKeySpec = new SecretKeySpec(finalKey, "AES");
				paramSpec = new IvParameterSpec(headerFormat4.getEncryptionIV());
				break;
			case CHACHA20:
				Security.addProvider(new BouncyCastleProvider());
				cipher = Cipher.getInstance("ChaCha20");
				secretKeySpec = new SecretKeySpec(finalKey, "ChaCha20");
				paramSpec = new IvParameterSpec(headerFormat4.getEncryptionIV());
				break;
			case TWOFISH:
				throw new IllegalArgumentException("Cipher " + headerFormat4.getOuterEncryptionAlgorithm() + " is not implemented yet");
			default:
				throw new IllegalArgumentException("Unknown cipher " + headerFormat4.getOuterEncryptionAlgorithm());
		}
		cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, paramSpec);
		dataInputStream = new CipherInputStream(dataInputStream, cipher);

		if (headerFormat4.isCompressData()) {
			dataInputStream = new GZIPInputStream(dataInputStream);
		}

		headerFormat4.readInnerHeader(dataInputStream);
		database.setBinaryAttachments(headerFormat4.getBinaryAttachments());

		innerEncryptionCipher = createInnerEncryptionCipher(headerFormat4.getInnerEncryptionAlgorithm(), headerFormat4.getInnerEncryptionKeyBytes());

		final byte[] decryptedXmlPayloadData = Utilities.toByteArray(dataInputStream);
		document = Utilities.parseXmlFile(decryptedXmlPayloadData);

		final Node rootNode = document.getDocumentElement();
		if (!"KeePassFile".equals(rootNode.getNodeName())) {
			if (strictMode) {
				throw new Exception("Unexpected xml root node name: " + rootNode.getNodeName());
			}
		}
		final NodeList childNodes = rootNode.getChildNodes();
		for (int i = 0; i < childNodes.getLength(); i++) {
			final Node childNode = childNodes.item(i);
			if (childNode.getNodeType() != Node.TEXT_NODE) {
				if ("Meta".equals(childNode.getNodeName())) {
					database.setMeta(readKdbxMetaData(dataFormatVersion, childNode, null));
				} else if ("Root".equals(childNode.getNodeName())) {
					readRoot(dataFormatVersion, database, childNode);
				} else {
					if (strictMode) {
						throw new Exception("Unexpected data node name: " + childNode.getNodeName());
					}
				}
			}
		}

		database.getHeaderFormat().resetCryptoKeys();

		for (final KdbxEntry entry : database.getAllEntries()) {
			for (final KdbxEntryBinary binary : entry.getBinaries()) {
				if (binary.getRefId() != null) {
					final KdbxBinary databaseBinary = database.getBinaryAttachments().get(binary.getRefId());
					if (databaseBinary != null) {
						if (databaseBinary.isCompressed()) {
							binary.setCompressedData(databaseBinary.getData());
						} else {
							binary.setCompressedData(Utilities.gzip(databaseBinary.getData()));
						}
					} else {
						throw new Exception("Cannot find referenced binary id: " + binary.getRefId());
					}
				}
			}
		}

		return database;
	}

	private StreamCipher createInnerEncryptionCipher(final InnerEncryptionAlgorithm innerEncryptionAlgorithm, final byte[] innerEncryptionKeyBytes) throws Exception {
		switch (innerEncryptionAlgorithm) {
			case SALSA20:
				if (innerEncryptionKeyBytes == null || innerEncryptionKeyBytes.length == 0) {
					throw new Exception("innerEncryptionKeyBytes must not be null or empty");
				} else {
					final byte[] key = MessageDigest.getInstance("SHA-256").digest(innerEncryptionKeyBytes);
					final KeyParameter keyparam = new KeyParameter(key);
					final byte[] initialVector = new byte[] { (byte) 0xE8, 0x30, 0x09, 0x4B, (byte) 0x97, 0x20, 0x5D, 0x2A };
					final ParametersWithIV params = new ParametersWithIV(keyparam, initialVector);
					innerEncryptionCipher = new Salsa20Engine();
					innerEncryptionCipher.init(false, params);
					return innerEncryptionCipher;
				}
			case CHACHA20:
				if (innerEncryptionKeyBytes == null || innerEncryptionKeyBytes.length == 0) {
					throw new Exception("innerEncryptionKeyBytes must not be null or empty");
				} else {
					final byte[] key = MessageDigest.getInstance("SHA-512").digest(innerEncryptionKeyBytes);
					final byte[] actualKey = Arrays.copyOfRange(key, 0, 32);
					final byte[] initialVector = Arrays.copyOfRange(key, 32, 32 + 12);
					final KeyParameter keyparam = new KeyParameter(actualKey);
					final ParametersWithIV params = new ParametersWithIV(keyparam, initialVector);
					innerEncryptionCipher = new ChaCha7539Engine();
					innerEncryptionCipher.init(false, params);
					return innerEncryptionCipher;
				}
			case ARC4_VARIANT:
				throw new Exception("Unsupported algorithm ARC4_VARIANT");
			case NONE:
				throw new Exception("Undefined algorithm");
			default:
				// no inner encryption
				return null;
		}
	}

	private KdbxMeta readKdbxMetaData(final Version dataFormatVersion, final Node metaNode, final List<KdbxBinary> binaryAttachments) throws Exception {
		final KdbxMeta kdbxMeta = new KdbxMeta();
		final NodeList childNodes = metaNode.getChildNodes();
		for (int i = 0; i < childNodes.getLength(); i++) {
			final Node childNode = childNodes.item(i);
			if (childNode.getNodeType() != Node.TEXT_NODE) {
				if ("Generator".equals(childNode.getNodeName())) {
					kdbxMeta.setGenerator(parseStringValue(childNode));
				} else if ("HeaderHash".equals(childNode.getNodeName())) {
					kdbxMeta.setHeaderHash(parseStringValue(childNode));
				} else if ("SettingsChanged".equals(childNode.getNodeName())) {
					kdbxMeta.setSettingsChanged(parseDateTimeValue(dataFormatVersion, childNode));
				} else if ("DatabaseName".equals(childNode.getNodeName())) {
					kdbxMeta.setDatabaseName(parseStringValue(childNode));
				} else if ("DatabaseNameChanged".equals(childNode.getNodeName())) {
					kdbxMeta.setDatabaseNameChanged(parseDateTimeValue(dataFormatVersion, childNode));
				} else if ("DatabaseDescription".equals(childNode.getNodeName())) {
					kdbxMeta.setDatabaseDescription(parseStringValue(childNode));
				} else if ("DatabaseDescriptionChanged".equals(childNode.getNodeName())) {
					kdbxMeta.setDatabaseDescriptionChanged(parseDateTimeValue(dataFormatVersion, childNode));
				} else if ("DefaultUserName".equals(childNode.getNodeName())) {
					kdbxMeta.setDefaultUserName(parseStringValue(childNode));
				} else if ("DefaultUserNameChanged".equals(childNode.getNodeName())) {
					kdbxMeta.setDefaultUserNameChanged(parseDateTimeValue(dataFormatVersion, childNode));
				} else if ("MaintenanceHistoryDays".equals(childNode.getNodeName())) {
					kdbxMeta.setMaintenanceHistoryDays(parseIntegerValue(childNode));
				} else if ("Color".equals(childNode.getNodeName())) {
					kdbxMeta.setColor(parseStringValue(childNode));
				} else if ("MasterKeyChanged".equals(childNode.getNodeName())) {
					kdbxMeta.setMasterKeyChanged(parseDateTimeValue(dataFormatVersion, childNode));
				} else if ("MasterKeyChangeRec".equals(childNode.getNodeName())) {
					kdbxMeta.setMasterKeyChangeRec(parseIntegerValue(childNode));
				} else if ("MasterKeyChangeForce".equals(childNode.getNodeName())) {
					kdbxMeta.setMasterKeyChangeForce(parseIntegerValue(childNode));
				} else if ("MasterKeyChangeForceOnce".equals(childNode.getNodeName())) {
					kdbxMeta.setMasterKeyChangeForceOnce(parseBooleanValue(childNode));
				} else if ("RecycleBinEnabled".equals(childNode.getNodeName())) {
					kdbxMeta.setRecycleBinEnabled(parseBooleanValue(childNode));
				} else if ("RecycleBinUUID".equals(childNode.getNodeName())) {
					kdbxMeta.setRecycleBinUUID(parseUuidValue(childNode));
				} else if ("RecycleBinChanged".equals(childNode.getNodeName())) {
					kdbxMeta.setRecycleBinChanged(parseDateTimeValue(dataFormatVersion, childNode));
				} else if ("EntryTemplatesGroup".equals(childNode.getNodeName())) {
					kdbxMeta.setEntryTemplatesGroup(parseUuidValue(childNode));
				} else if ("EntryTemplatesGroupChanged".equals(childNode.getNodeName())) {
					kdbxMeta.setEntryTemplatesGroupChanged(parseDateTimeValue(dataFormatVersion, childNode));
				} else if ("HistoryMaxItems".equals(childNode.getNodeName())) {
					kdbxMeta.setHistoryMaxItems(parseIntegerValue(childNode));
				} else if ("HistoryMaxSize".equals(childNode.getNodeName())) {
					kdbxMeta.setHistoryMaxSize(parseIntegerValue(childNode));
				} else if ("LastSelectedGroup".equals(childNode.getNodeName())) {
					kdbxMeta.setLastSelectedGroup(parseUuidValue(childNode));
				} else if ("LastTopVisibleGroup".equals(childNode.getNodeName())) {
					kdbxMeta.setLastTopVisibleGroup(parseUuidValue(childNode));
				} else if ("Binaries".equals(childNode.getNodeName())) {
					binaryAttachments.addAll(parseBinariesData(childNode));
				} else if ("MemoryProtection".equals(childNode.getNodeName())) {
					kdbxMeta.setMemoryProtection(parseMemoryProtection(childNode));
				} else if ("CustomData".equals(childNode.getNodeName())) {
					kdbxMeta.setCustomData(parseCustomData(dataFormatVersion, childNode));
				} else if ("CustomIcons".equals(childNode.getNodeName())) {
					kdbxMeta.setCustomIcons(parseCustomIcons(childNode));
				} else {
					if (strictMode) {
						throw new Exception("Unexpected meta attribute node name: " + childNode.getNodeName());
					}
				}
			}
		}
		return kdbxMeta;
	}

	private KdbxMemoryProtection parseMemoryProtection(final Node memoryProtectionNode) throws Exception {
		final KdbxMemoryProtection memoryProtection = new KdbxMemoryProtection();
		final NodeList childNodes = memoryProtectionNode.getChildNodes();
		for (int i = 0; i < childNodes.getLength(); i++) {
			final Node childNode = childNodes.item(i);
			if (childNode.getNodeType() != Node.TEXT_NODE) {
				if ("ProtectTitle".equals(childNode.getNodeName())) {
					memoryProtection.setProtectTitle(parseBooleanValue(childNode));
				} else if ("ProtectUserName".equals(childNode.getNodeName())) {
					memoryProtection.setProtectUserName(parseBooleanValue(childNode));
				} else if ("ProtectPassword".equals(childNode.getNodeName())) {
					memoryProtection.setProtectPassword(parseBooleanValue(childNode));
				} else if ("ProtectURL".equals(childNode.getNodeName())) {
					memoryProtection.setProtectURL(parseBooleanValue(childNode));
				} else if ("ProtectNotes".equals(childNode.getNodeName())) {
					memoryProtection.setProtectNotes(parseBooleanValue(childNode));
				} else {
					if (strictMode) {
						throw new Exception("Unexpected meta memoryProtection node name: " + childNode.getNodeName());
					}
				}
			}
		}
		return memoryProtection;
	}

	private List<KdbxCustomDataItem> parseCustomData(final Version dataFormatVersion, final Node customDataNode)
			throws Exception {
		final List<KdbxCustomDataItem> customDataItems = new ArrayList<>();
		final NodeList childNodes = customDataNode.getChildNodes();
		for (int i = 0; i < childNodes.getLength(); i++) {
			final Node childNode = childNodes.item(i);
			if (childNode.getNodeType() != Node.TEXT_NODE) {
				if ("Item".equals(childNode.getNodeName())) {
					customDataItems.add(parseCustomDataItem(dataFormatVersion, childNode));
				} else {
					if (strictMode) {
						throw new Exception("Unexpected customData node name: " + childNode.getNodeName());
					}
				}
			}
		}
		return customDataItems;
	}

	private KdbxCustomDataItem parseCustomDataItem(final Version dataFormatVersion, final Node customDataItemNode)
			throws Exception {
		final KdbxCustomDataItem customDataItem = new KdbxCustomDataItem();
		final NodeList childNodes = customDataItemNode.getChildNodes();
		for (int i = 0; i < childNodes.getLength(); i++) {
			final Node childNode = childNodes.item(i);
			if (childNode.getNodeType() != Node.TEXT_NODE) {
				if ("Key".equals(childNode.getNodeName())) {
					customDataItem.setKey(parseStringValue(childNode));
				} else if ("Value".equals(childNode.getNodeName())) {
					customDataItem.setValue(parseStringValue(childNode));
				} else if ("LastModificationTime".equals(childNode.getNodeName())) {
					customDataItem.setLastModificationTime(parseDateTimeValue(dataFormatVersion, childNode));
				} else {
					if (strictMode) {
						throw new Exception("Unexpected customData attribute node name: " + childNode.getNodeName());
					}
				}
			}
		}
		return customDataItem;
	}

	private List<KdbxBinary> parseBinariesData(final Node binaryNode) throws Exception {
		final List<KdbxBinary> binaryItems = new ArrayList<>();
		final NodeList binaryNodes = binaryNode.getChildNodes();
		for (int i = 0; i < binaryNodes.getLength(); i++) {
			final Node binaryChildNode = binaryNodes.item(i);
			if (binaryChildNode.getNodeType() != Node.TEXT_NODE) {
				if ("Binary".equals(binaryChildNode.getNodeName())) {
					final int id = Integer.parseInt(Utilities.getAttributeValue(binaryChildNode, "ID"));
					final boolean compressed = "True".equals(Utilities.getAttributeValue(binaryChildNode, "Compressed"));
					final String dataBase64String = parseStringValue(binaryChildNode);
					final byte[] data = Base64.getDecoder().decode(dataBase64String);
					binaryItems.add(new KdbxBinary().setId(id).setCompressed(compressed).setData(data));
				} else {
					if (strictMode) {
						throw new Exception("Unexpected binary node name: " + binaryChildNode.getNodeName());
					}
				}
			}
		}
		return binaryItems;
	}

	private Map<KdbxUUID, byte[]> parseCustomIcons(final Node customIconsNode) throws Exception {
		final Map<KdbxUUID, byte[]> customIcons = new LinkedHashMap<>();
		final NodeList customIconsChildNodes = customIconsNode.getChildNodes();
		for (int i = 0; i < customIconsChildNodes.getLength(); i++) {
			final Node customIconNode = customIconsChildNodes.item(i);
			if (customIconNode.getNodeType() != Node.TEXT_NODE) {
				if ("Icon".equals(customIconNode.getNodeName())) {
					final KdbxUUID uuid = parseUuidValue(Utilities.getChildNodesMap(customIconNode).get("UUID"));
					final String dataBase64String = Utilities.getNodeValue(Utilities.getChildNodesMap(customIconNode).get("Data"));
					final byte[] data = Base64.getDecoder().decode(dataBase64String);
					customIcons.put(uuid, data);
				} else {
					if (strictMode) {
						throw new Exception("Unexpected custom icon node name: " + customIconNode.getNodeName());
					}
				}
			}
		}
		return customIcons;
	}

	private void readRoot(final Version dataFormatVersion, final KdbxDatabase database, final Node rootNode)
			throws Exception {
		final NodeList childNodes = rootNode.getChildNodes();
		for (int i = 0; i < childNodes.getLength(); i++) {
			final Node childNode = childNodes.item(i);
			if (childNode.getNodeType() != Node.TEXT_NODE) {
				if ("Group".equals(childNode.getNodeName())) {
					database.getGroups().add(readGroup(dataFormatVersion, childNode));
				} else if ("Entry".equals(childNode.getNodeName())) {
					database.getEntries().add(readEntry(dataFormatVersion, childNode));
				} else if ("DeletedObjects".equals(childNode.getNodeName())) {
					readDeletedObjects(dataFormatVersion, database, childNode);
				} else {
					if (strictMode) {
						throw new Exception("Unexpected root data node name: " + childNode.getNodeName());
					}
				}
			}
		}
	}

	private void readDeletedObjects(final Version dataFormatVersion, final KdbxDatabase database, final Node childNode)
			throws Exception {
		final NodeList deletedObjectsChildNodes = childNode.getChildNodes();
		for (int j = 0; j < deletedObjectsChildNodes.getLength(); j++) {
			final Node deletedObjectsChildNode = deletedObjectsChildNodes.item(j);
			if (deletedObjectsChildNode.getNodeType() != Node.TEXT_NODE) {
				if ("DeletedObject".equals(deletedObjectsChildNode.getNodeName())) {
					KdbxUUID uuid = null;
					ZonedDateTime deletionTime = null;
					final NodeList deletedObjectChildNodes = deletedObjectsChildNode.getChildNodes();
					for (int k = 0; k < deletedObjectChildNodes.getLength(); k++) {
						final Node deletedObjectChildNode = deletedObjectChildNodes.item(k);
						if (deletedObjectChildNode.getNodeType() != Node.TEXT_NODE) {
							if ("UUID".equals(deletedObjectChildNode.getNodeName())) {
								uuid = parseUuidValue(deletedObjectChildNode);
							} else if ("DeletionTime".equals(deletedObjectChildNode.getNodeName())) {
								deletionTime = parseDateTimeValue(dataFormatVersion, deletedObjectChildNode);
							} else {
								if (strictMode) {
									throw new Exception("Unexpected deleted objects data node name: " + childNode.getNodeName());
								}
							}
						}
					}
					if (uuid == null) {
						throw new Exception("Invalid deleted object node: Missing uuid");
					} else if (deletionTime == null) {
						throw new Exception("Invalid deleted object node for uuid '" + uuid.toHex() + "': Missing deletionTime");
					} else {
						database.getDeletedObjects().put(uuid, deletionTime);
					}
				} else {
					if (strictMode) {
						throw new Exception("Unexpected deleted object data node name: " + deletedObjectsChildNode.getNodeName());
					}
				}
			}
		}
	}

	private KdbxGroup readGroup(final Version dataFormatVersion, final Node groupNode) throws Exception {
		final KdbxGroup group = new KdbxGroup();
		final NodeList childNodes = groupNode.getChildNodes();
		for (int i = 0; i < childNodes.getLength(); i++) {
			final Node childNode = childNodes.item(i);
			if (childNode.getNodeType() != Node.TEXT_NODE) {
				if ("UUID".equals(childNode.getNodeName())) {
					group.setUuid(parseUuidValue(childNode));
				} else if ("Name".equals(childNode.getNodeName())) {
					group.setName(parseStringValue(childNode));
				} else if ("Notes".equals(childNode.getNodeName())) {
					group.setNotes(parseStringValue(childNode));
				} else if ("IconID".equals(childNode.getNodeName())) {
					group.setIconID(parseIntegerValue(childNode));
				} else if ("IsExpanded".equals(childNode.getNodeName())) {
					group.setExpanded(parseBooleanValue(childNode));
				} else if ("DefaultAutoTypeSequence".equals(childNode.getNodeName())) {
					group.setDefaultAutoTypeSequence(parseStringValue(childNode));
				} else if ("EnableAutoType".equals(childNode.getNodeName())) {
					group.setEnableAutoType(parseBooleanValue(childNode));
				} else if ("EnableSearching".equals(childNode.getNodeName())) {
					group.setEnableSearching(parseBooleanValue(childNode));
				} else if ("LastTopVisibleEntry".equals(childNode.getNodeName())) {
					group.setLastTopVisibleEntry(parseUuidValue(childNode));
				} else if ("Times".equals(childNode.getNodeName())) {
					group.setTimes(readKdbxTimes(dataFormatVersion, childNode));
				} else if ("Group".equals(childNode.getNodeName())) {
					group.getGroups().add(readGroup(dataFormatVersion, childNode));
				} else if ("Entry".equals(childNode.getNodeName())) {
					group.getEntries().add(readEntry(dataFormatVersion, childNode));
				} else if ("CustomIconUUID".equals(childNode.getNodeName())) {
					group.setCustomIconUuid(parseUuidValue(childNode));
				} else if ("CustomData".equals(childNode.getNodeName())) {
					group.setCustomData(parseCustomData(dataFormatVersion, childNode));
				} else {
					if (strictMode) {
						throw new Exception("Unexpected group data node name: " + childNode.getNodeName());
					}
				}
			}
		}
		return group;
	}

	private KdbxTimes readKdbxTimes(final Version dataFormatVersion, final Node timesNode) throws Exception {
		final KdbxTimes times = new KdbxTimes();
		final NodeList childNodes = timesNode.getChildNodes();
		for (int i = 0; i < childNodes.getLength(); i++) {
			final Node childNode = childNodes.item(i);
			if (childNode.getNodeType() != Node.TEXT_NODE) {
				if ("CreationTime".equals(childNode.getNodeName())) {
					times.setCreationTime(parseDateTimeValue(dataFormatVersion, childNode));
				} else if ("LastModificationTime".equals(childNode.getNodeName())) {
					times.setLastModificationTime(parseDateTimeValue(dataFormatVersion, childNode));
				} else if ("LastAccessTime".equals(childNode.getNodeName())) {
					times.setLastAccessTime(parseDateTimeValue(dataFormatVersion, childNode));
				} else if ("ExpiryTime".equals(childNode.getNodeName())) {
					times.setExpiryTime(parseDateTimeValue(dataFormatVersion, childNode));
				} else if ("Expires".equals(childNode.getNodeName())) {
					times.setExpires(parseBooleanValue(childNode));
				} else if ("UsageCount".equals(childNode.getNodeName())) {
					times.setUsageCount(parseIntegerValue(childNode));
				} else if ("LocationChanged".equals(childNode.getNodeName())) {
					times.setLocationChanged(parseDateTimeValue(dataFormatVersion, childNode));
				} else {
					if (strictMode) {
						throw new Exception("Unexpected times data node name: " + childNode.getNodeName());
					}
				}
			}
		}
		return times;
	}

	private KdbxEntry readEntry(final Version dataFormatVersion, final Node entryNode) throws Exception {
		final KdbxEntry entry = new KdbxEntry();
		final NodeList childNodes = entryNode.getChildNodes();
		for (int i = 0; i < childNodes.getLength(); i++) {
			final Node childNode = childNodes.item(i);
			if (childNode.getNodeType() != Node.TEXT_NODE) {
				if ("UUID".equals(childNode.getNodeName())) {
					entry.setUuid(parseUuidValue(childNode));
				} else if ("IconID".equals(childNode.getNodeName())) {
					entry.setIconID(parseIntegerValue(childNode));
				} else if ("ForegroundColor".equals(childNode.getNodeName())) {
					entry.setForegroundColor(parseStringValue(childNode));
				} else if ("BackgroundColor".equals(childNode.getNodeName())) {
					entry.setBackgroundColor(parseStringValue(childNode));
				} else if ("OverrideURL".equals(childNode.getNodeName())) {
					entry.setOverrideURL(parseStringValue(childNode));
				} else if ("Tags".equals(childNode.getNodeName())) {
					entry.setTags(parseStringValue(childNode));
				} else if ("Times".equals(childNode.getNodeName())) {
					entry.setTimes(readKdbxTimes(dataFormatVersion, childNode));
				} else if ("String".equals(childNode.getNodeName())) {
					parseKeyValue(entry.getItems(), childNode);
				} else if ("Binary".equals(childNode.getNodeName())) {
					entry.getBinaries().add(readEntryBinary(childNode));
				} else if ("AutoType".equals(childNode.getNodeName())) {
					boolean enabled = false;
					String dataTransferObfuscation = null;
					String defaultSequence = null;
					String window = null;
					String keystrokeSequence = null;
					final NodeList autoTypeChildNodes = childNode.getChildNodes();
					for (int j = 0; j < autoTypeChildNodes.getLength(); j++) {
						final Node autoTypeChildNode = autoTypeChildNodes.item(j);
						if (autoTypeChildNode.getNodeType() != Node.TEXT_NODE) {
							if ("Enabled".equals(autoTypeChildNode.getNodeName())) {
								enabled = parseBooleanValue(autoTypeChildNode);
							} else if ("DataTransferObfuscation".equals(autoTypeChildNode.getNodeName())) {
								dataTransferObfuscation = parseStringValue(autoTypeChildNode);
							} else if ("DefaultSequence".equals(autoTypeChildNode.getNodeName())) {
								defaultSequence = parseStringValue(autoTypeChildNode);
							} else if ("Association".equals(autoTypeChildNode.getNodeName())) {
								final NodeList associationChildNodes = autoTypeChildNode.getChildNodes();
								for (int k = 0; k < associationChildNodes.getLength(); k++) {
									final Node associationChildNode = associationChildNodes.item(k);
									if (associationChildNode.getNodeType() != Node.TEXT_NODE) {
										if ("Window".equals(associationChildNode.getNodeName())) {
											window = parseStringValue(autoTypeChildNode);
										} else if ("KeystrokeSequence".equals(associationChildNode.getNodeName())) {
											keystrokeSequence = parseStringValue(autoTypeChildNode);
										} else {
											if (strictMode) {
												throw new Exception("Unexpected association data node name: "
														+ associationChildNode.getNodeName());
											}
										}
									}
								}
							} else {
								if (strictMode) {
									throw new Exception("Unexpected autotype data node name: " + autoTypeChildNode.getNodeName());
								}
							}
						}
					}
					entry.setAutoType(enabled, dataTransferObfuscation, defaultSequence, window, keystrokeSequence);
				} else if ("History".equals(childNode.getNodeName())) {
					final NodeList historyChildNodes = childNode.getChildNodes();
					for (int j = 0; j < historyChildNodes.getLength(); j++) {
						final Node historyChildNode = historyChildNodes.item(j);
						if (historyChildNode.getNodeType() != Node.TEXT_NODE) {
							if ("Entry".equals(historyChildNode.getNodeName())) {
								entry.getHistory().add(readEntry(dataFormatVersion, historyChildNode));
							} else {
								if (strictMode) {
									throw new Exception("Unexpected history entry data node name: "
											+ historyChildNode.getNodeName());
								}
							}
						}
					}
				} else if ("CustomIconUUID".equals(childNode.getNodeName())) {
					entry.setCustomIconUuid(parseUuidValue(childNode));
				} else if ("CustomData".equals(childNode.getNodeName())) {
					entry.setCustomData(parseCustomData(dataFormatVersion, childNode));
				} else {
					if (strictMode) {
						throw new Exception("Unexpected entry data node name: " + childNode.getNodeName());
					}
				}
			}
		}
		return entry;
	}

	private static String parseStringValue(final Node stringValueNode) {
		if (stringValueNode.getNodeValue() != null) {
			return stringValueNode.getNodeValue();
		} else if (stringValueNode.getFirstChild() != null) {
			return parseStringValue(stringValueNode.getFirstChild());
		} else {
			return null;
		}
	}

	private void parseKeyValue(final Map<String, Object> items, final Node keyValueNode) throws Exception {
		String key = null;
		String value = null;
		final NodeList childNodes = keyValueNode.getChildNodes();
		for (int i = 0; i < childNodes.getLength(); i++) {
			final Node childNode = childNodes.item(i);
			if (childNode.getNodeType() != Node.TEXT_NODE) {
				if ("Key".equals(childNode.getNodeName())) {
					key = parseStringValue(childNode);
				} else if ("Value".equals(childNode.getNodeName())) {
					value = parseStringValue(childNode);
					boolean isProtected = false;
					final NamedNodeMap attributes = childNode.getAttributes();
					if (attributes != null) {
						for (int j = 0; j < attributes.getLength(); j++) {
							final Node attribute = attributes.item(j);
							if ("Protected".equals(attribute.getNodeName())) {
								isProtected = "True".equals(attribute.getNodeValue());
								break;
							}
						}
					}
					if (isProtected && value != null && innerEncryptionCipher != null) {
						final byte[] decrypedData = Base64.getDecoder().decode(value);
						final byte[] output = new byte[decrypedData.length];
						innerEncryptionCipher.processBytes(decrypedData, 0, decrypedData.length, output, 0);
						value = new String(output, StandardCharsets.UTF_8);
					}
				} else {
					if (strictMode) {
						throw new Exception("Unexpected key value data node name: " + childNode.getNodeName());
					}
				}
			}
		}
		if (key == null) {
			throw new Exception("Invalid key value data node: Missing key");
		} else {
			items.put(key, value);
		}
	}

	private KdbxEntryBinary readEntryBinary(final Node entryBinaryNode) throws Exception {
		String key = null;
		Integer refID = null;
		byte[] data = null;
		final NodeList entryBinaryNodes = entryBinaryNode.getChildNodes();
		for (int i = 0; i < entryBinaryNodes.getLength(); i++) {
			final Node entryBinaryChildNode = entryBinaryNodes.item(i);
			if (entryBinaryChildNode.getNodeType() != Node.TEXT_NODE) {
				if ("Key".equals(entryBinaryChildNode.getNodeName())) {
					key = parseStringValue(entryBinaryChildNode);
				} else if ("Value".equals(entryBinaryChildNode.getNodeName())) {
					final String refIdString = Utilities.getAttributeValue(entryBinaryChildNode, "Ref");
					if (Utilities.isNotBlank(refIdString)) {
						refID = Integer.parseInt(refIdString);
					} else {
						data = Base64.getDecoder().decode(parseStringValue(entryBinaryChildNode));
					}
				} else {
					if (strictMode) {
						throw new Exception("Unexpected binary data node name: " + entryBinaryChildNode.getNodeName());
					}
				}
			}
		}
		if (key == null) {
			throw new Exception("Invalid key value data node: Missing key");
		} else {
			if (strictMode && refID != null && data != null) {
				throw new Exception("Unexpected entry binary data using refID and data at the same time. ID: " + refID);
			}
			final KdbxEntryBinary entryBinary = new KdbxEntryBinary();
			entryBinary.setKey(key);
			if (refID != null) {
				entryBinary.setRefId(refID);
			}
			if (data != null) {
				entryBinary.setCompressedData(Utilities.gzip(data));
			}
			return entryBinary;
		}
	}

	private static ZonedDateTime parseDateTimeValue(final Version kdbxVersion, final Node node) throws Exception {
		final String stringValue = parseStringValue(node);
		if (stringValue == null || "".equals(stringValue.trim())) {
			return null;
		} else if (kdbxVersion.getMajorVersionNumber() < 4) {
			return ZonedDateTime.from(DateTimeFormatter.ISO_DATE_TIME.parse(stringValue));
		} else {
			try {
				final byte[] secondsArray = Base64.getDecoder().decode(stringValue);
				final long elapsedSeconds = Utilities.readLongFromLittleEndianBytes(secondsArray);
				if (elapsedSeconds < 0) {
					throw new Exception("Long value of seconds since 0001-01-001 00:00:00 UTC overflowed: " + elapsedSeconds);
				} else {
					return ZonedDateTime.of(1, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC")).plusSeconds(elapsedSeconds).withZoneSameInstant(ZoneId.systemDefault());
				}
			} catch (final Exception e) {
				throw new Exception("Invalid DateTime base64 value: " + stringValue, e);
			}
		}
	}

	private static boolean parseBooleanValue(final Node node) {
		return "True".equals(parseStringValue(node));
	}

	private static int parseIntegerValue(final Node node) throws Exception {
		final String stringValue = parseStringValue(node);
		if (stringValue == null || "".equals(stringValue.trim())) {
			throw new Exception("Invalid empty integer value");
		} else {
			try {
				return Integer.parseInt(stringValue);
			} catch (final NumberFormatException e) {
				throw new Exception("Invalid integer value: " + stringValue, e);
			}
		}
	}

	private static KdbxUUID parseUuidValue(final Node node) {
		return KdbxUUID.fromBase64(parseStringValue(node));
	}

	@Override
	public void close() throws Exception {
		if (inputStream != null) {
			inputStream.close();
		}
	}
}
