package de.soderer.utilities.kdbx;

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.util.Base64;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import de.soderer.utilities.kdbx.util.Utilities;

/**
 * KeyFile formats<br />
 * KeePass supports the following key file formats:<br />
 * XML (recommended, default). There is an XML format for key files. KeePass 2.x uses this format by default, i.e. when creating a key file in the master key dialog, an XML key file is created. The syntax and the semantics of the XML format allow to detect certain corruptions (especially such caused by faulty hardware or transfer problems), and a hash (in XML key files version 2.0 or higher) allows to verify the integrity of the key. This format is resistant to most encoding and new-line character changes (which is useful for instance when the user is opening and saving the key file or when transferring it from/to a server). Such a key file can be printed (as a backup on paper), and comments can be added in the file (with the usual XML syntax: <!-- ... -->). It is the most flexible format; new features can be added easily in the future.<br />
 * 32 bytes. If the key file contains exactly 32 bytes, these are used as a 256-bit cryptographic key. This format requires the least disk space.<br />
 * Hexadecimal. If the key file contains exactly 64 hexadecimal characters (0-9 and A-F, in UTF-8/ASCII encoding, one line, no spaces), these are decoded to a 256-bit cryptographic key.<br />
 * Hashed. If a key file does not match any of the formats above, its content is hashed using a cryptographic hash function in order to build a key (typically a 256-bit key with SHA-256). This allows to use arbitrary files as key files.<br />
 */
public class KdbxCredentials {
	private char[] password = null;
	private byte[] keyFileData = null;
	private String windowsUserAccount = null;

	public KdbxCredentials(final char[] password) {
		this.password = password;
	}

	public KdbxCredentials(final byte[] keyFileData) {
		this.keyFileData = keyFileData;
	}

	public KdbxCredentials(final String windowsUserAccount) {
		this.windowsUserAccount = windowsUserAccount;
	}

	public KdbxCredentials(final char[] password, final byte[] keyFileData) {
		this.password = password;
		this.keyFileData = keyFileData;
	}

	public KdbxCredentials(final char[] password, final byte[] keyFileData, final String windowsUserAccount) {
		this.password = password;
		this.keyFileData = keyFileData;
		this.windowsUserAccount = windowsUserAccount;
	}

	public byte[] createCompositeKeyHash() throws Exception {
		final ByteArrayOutputStream concat = new ByteArrayOutputStream();

		if (password != null) {
			final byte[] passwordHash = MessageDigest.getInstance("SHA-256").digest(Utilities.toBytes(password));
			concat.write(passwordHash, 0, passwordHash.length);
		}

		if (keyFileData != null) {
			if (Utilities.isXmlDocument(keyFileData)) {
				String version = null;
				final Document document = Utilities.parseXmlFile(keyFileData);
				final Element rootNode = document.getDocumentElement();
				final Node metaNode = Utilities.getChildNodesMap(rootNode).get("Meta");
				if (metaNode != null) {
					final Node versionNode = Utilities.getChildNodesMap(metaNode).get("Version");
					if (versionNode != null) {
						version = Utilities.getNodeValue(versionNode);
					}
				}
				if (version != null && version.startsWith("1.")) {
					final Node keyNode = Utilities.getChildNodesMap(rootNode).get("Key");
					if (keyNode != null) {
						final Node dataNode = Utilities.getChildNodesMap(keyNode).get("Data");
						if (dataNode != null) {
							final String dataBase64String = Utilities.getNodeValue(dataNode);
							final byte[] keyBytes = Base64.getDecoder().decode(dataBase64String);
							concat.write(keyBytes, 0, keyBytes.length);
						}
					}
				} else if (version != null && version.startsWith("2.")) {
					final Node keyNode = Utilities.getChildNodesMap(rootNode).get("Key");
					if (keyNode != null) {
						final Node dataNode = Utilities.getChildNodesMap(keyNode).get("Data");
						if (dataNode != null) {
							final String hashHexString = Utilities.getAttributeValue(dataNode, "Hash");
							final byte[] hashBytes = Utilities.fromHexString(hashHexString, true);

							final String dataHexString = Utilities.getNodeValue(dataNode);
							final byte[] keyBytes = Utilities.fromHexString(dataHexString, true);
							final byte[] keyBytesHash = MessageDigest.getInstance("SHA-256").digest(keyBytes);
							if (keyBytesHash[0] != hashBytes[0] || keyBytesHash[1] != hashBytes[1] || keyBytesHash[2] != hashBytes[2] || keyBytesHash[3] != hashBytes[3]) {
								throw new RuntimeException("Keyfile was tampered: Data hash is invalid" );
							}
							concat.write(keyBytes, 0, keyBytes.length);
						}
					}
				} else {
					throw new RuntimeException("Usupported key file version: " + version);
				}
			} else {
				final byte[] keyFileHash = MessageDigest.getInstance("SHA-256").digest(keyFileData);
				concat.write(keyFileHash, 0, keyFileHash.length);
			}
		}

		if (windowsUserAccount != null) {
			// TODO Implement windows user account credentials
			// final byte[] windowsUserAccountHash = null;// = MessageDigest.getInstance("sha256").digest(windowsUserAccount);
			// concat.write(windowsUserAccountHash, 0, windowsUserAccountHash.length);
			throw new RuntimeException("WindowsUserAccount not supported yet");
		}

		final byte[] compositeKeyBytes = concat.toByteArray();

		final byte[] compositeKeyHash = MessageDigest.getInstance("SHA-256").digest(compositeKeyBytes);
		return compositeKeyHash;
	}
}
