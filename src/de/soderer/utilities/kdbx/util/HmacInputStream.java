package de.soderer.utilities.kdbx.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * InputStream to check "Hashed Message Authentication Code" (HMAC) checksums.<br />
 * Default ByteOrder is Little Endian.<br />
 * Default HmacAlgorithmName is HmacSHA256.<br />
 * Default KeyHashDigestName is SHA-512.<br />
 */
public class HmacInputStream extends InputStream {
	private InputStream baseInputStream;
	private byte[] key;

	private ByteOrder byteOrder = ByteOrder.LITTLE_ENDIAN;
	private String hmacAlgorithmName = "HmacSHA256";
	private String keyHashDigestName = "SHA-512";

	private long hmacBlockIndex;
	private boolean eof;
	private ByteArrayInputStream bufferStream;

	public HmacInputStream(final InputStream inputStream, final byte[] key) {
		if (inputStream == null) {
			throw new IllegalArgumentException("Invalid empty inputStream parameter for HmacInputStream");
		} else if (key == null || key.length <= 0) {
			throw new IllegalArgumentException("Invalid empty key parameter for HmacInputStream");
		} else if (key.length != 64) {
			throw new IllegalArgumentException("Expected a 64-byte key but got " + key.length + " bytes");
		} else {
			baseInputStream = inputStream;
			this.key = key;
			hmacBlockIndex = 0;
			eof = false;
		}
	}

	public HmacInputStream setByteOrder(final ByteOrder byteOrder) {
		this.byteOrder = byteOrder;
		return this;
	}

	public HmacInputStream setHmacAlgorithmName(final String hmacAlgorithmName) {
		this.hmacAlgorithmName = hmacAlgorithmName;
		return this;
	}

	public HmacInputStream setKeyHashDigestName(final String keyHashDigestName) {
		this.keyHashDigestName = keyHashDigestName;
		return this;
	}

	@Override
	public int read() throws IOException {
		if (eof) {
			return -1;
		} else {
			if (bufferStream == null) {
				if (!readNextHmacBlock()) {
					return -1;
				}
			}
			final int readByte = bufferStream.read();
			if (readByte == -1) {
				if (!readNextHmacBlock()) {
					return -1;
				} else {
					return bufferStream.read();
				}
			} else {
				return readByte;
			}
		}
	}

	@Override
	public int read(final byte[] data) throws IOException {
		if (eof) {
			return -1;
		} else {
			if (bufferStream == null) {
				if (!readNextHmacBlock()) {
					return -1;
				}
			}
			int bytesRemaining = data.length;
			int writeIndex = 0;
			while (bytesRemaining > 0) {
				final int bytesRead = bufferStream.read(data, writeIndex, bytesRemaining);
				if (bytesRead == -1) {
					if (!readNextHmacBlock()) {
						return data.length - bytesRemaining;
					}
				} else {
					bytesRemaining = bytesRemaining - bytesRead;
					writeIndex = writeIndex + bytesRead;
					if (bytesRemaining > 0 && !readNextHmacBlock()) {
						return data.length - bytesRemaining;
					}
				}
			}
			return data.length;
		}
	}

	@Override
	public int read(final byte[] data, final int offset, final int length) throws IOException {
		if (eof) {
			return -1;
		} else {
			if (bufferStream == null) {
				if (!readNextHmacBlock()) {
					return -1;
				}
			}
			int bytesRemaining = length;
			int writeIndex = offset;
			while (bytesRemaining > 0) {
				final int bytesRead = bufferStream.read(data, writeIndex, bytesRemaining);
				if (bytesRead == -1) {
					if (!readNextHmacBlock()) {
						return data.length - bytesRemaining;
					}
				} else {
					bytesRemaining = bytesRemaining - bytesRead;
					writeIndex = writeIndex + bytesRead;
					if (bytesRemaining > 0 && !readNextHmacBlock()) {
						return data.length - bytesRemaining;
					}
				}
			}
			return data.length;
		}
	}

	private boolean readNextHmacBlock() throws IOException {
		if (eof) {
			return false;
		} else {
			bufferStream = null;

			final byte[] hmacBytes = new byte[32];
			final int hmacBytesBytesRead = baseInputStream.read(hmacBytes);
			if (hmacBytesBytesRead == -1) {
				eof = true;
				return false;
			} else if (hmacBytesBytesRead != 32) {
				throw new IOException("Cannot read HMAC code");
			}

			final byte[] blockLengthBytes = new byte[4];
			if (baseInputStream.read(blockLengthBytes) != 4) {
				throw new IOException("Cannot read HMAC block size");
			}

			final int nextBlockSize = ByteBuffer.wrap(blockLengthBytes).order(byteOrder).getInt();

			if (nextBlockSize < 0) {
				throw new IOException("Invalid HMAC block size: " + nextBlockSize);
			}

			final byte[] buffer = new byte[nextBlockSize];
			final int bytesRead = baseInputStream.read(buffer);

			if (bytesRead > 0 && bytesRead != nextBlockSize) {
				throw new IOException("Premature end of input, expected " + nextBlockSize + " bytes but got only " + bytesRead);
			}

			MessageDigest digest;
			try {
				digest = MessageDigest.getInstance(keyHashDigestName);
			} catch (final NoSuchAlgorithmException e) {
				throw new RuntimeException("Digest not available: " + keyHashDigestName, e);
			}
			digest.update(ByteBuffer.allocate(8).order(byteOrder).putLong(hmacBlockIndex).array());
			final byte[] hmacBlockKey = digest.digest(key);

			Mac hmac;
			try {
				hmac = Mac.getInstance(hmacAlgorithmName);
				hmac.init(new SecretKeySpec(hmacBlockKey, hmacAlgorithmName));
			} catch (final InvalidKeyException e) {
				throw new RuntimeException("Invalid key for digest: " + hmacAlgorithmName, e);
			} catch (final NoSuchAlgorithmException e) {
				throw new RuntimeException("Hmac algorithm not available: " + hmacAlgorithmName, e);
			}

			hmac.update(ByteBuffer.allocate(8).order(byteOrder).putLong(hmacBlockIndex).array());
			hmac.update(blockLengthBytes);
			hmac.update(buffer, 0, nextBlockSize);
			final byte[] blockHmacBytes = hmac.doFinal();
			if (!Arrays.equals(hmacBytes, blockHmacBytes)) {
				throw new IOException("HMAC check failed, data or hash value is corrupted at block " + hmacBlockIndex);
			} else {
				hmacBlockIndex++;

				if (bytesRead >= 0) {
					bufferStream = new ByteArrayInputStream(buffer);
					return true;
				} else {
					eof = true;
					return false;
				}
			}
		}
	}

	@Override
	public void close() throws IOException {
		if (baseInputStream != null) {
			try {
				baseInputStream.close();
			} finally {
				baseInputStream = null;
			}
		}
	}
}
