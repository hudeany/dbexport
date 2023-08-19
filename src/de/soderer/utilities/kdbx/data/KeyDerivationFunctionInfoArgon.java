package de.soderer.utilities.kdbx.data;

import java.io.ByteArrayOutputStream;
import java.security.SecureRandom;

import org.bouncycastle.crypto.params.Argon2Parameters;

import de.soderer.utilities.kdbx.data.KdbxConstants.KeyDerivationFunction;
import de.soderer.utilities.kdbx.util.VariantDictionary;
import de.soderer.utilities.kdbx.util.VariantDictionaryEntry;

public class KeyDerivationFunctionInfoArgon implements KeyDerivationFunctionInfo {
	public enum Argon2Type {
		Argon2_D(Argon2Parameters.ARGON2_d),
		Argon2_ID(Argon2Parameters.ARGON2_id);

		private final int argon2TypeID;

		Argon2Type(final int argon2TypeID) {
			this.argon2TypeID = argon2TypeID;
		}

		public int getArgon2TypeID() {
			return argon2TypeID;
		}
	}

	private Argon2Type type = Argon2Type.Argon2_D;
	private int iterations = 2;
	private long memoryInBytes = 64 * 1024 * 1024;
	private int parallelism = 2;
	private byte[] salt;
	private int version = 19;

	public Argon2Type getType() {
		return type;
	}

	public KeyDerivationFunctionInfoArgon setType(final Argon2Type type) {
		if (type == null) {
			this.type = Argon2Type.Argon2_D;
		} else {
			this.type = type;
		}
		return this;
	}

	public int getIterations() {
		return iterations;
	}

	public KeyDerivationFunctionInfoArgon setIterations(final int iterations) {
		this.iterations = iterations;
		return this;
	}

	public long getMemoryInBytes() {
		return memoryInBytes;
	}

	public KeyDerivationFunctionInfoArgon setMemoryInBytes(final long memoryInBytes) {
		this.memoryInBytes = memoryInBytes;
		return this;
	}

	public int getParallelism() {
		return parallelism;
	}

	public KeyDerivationFunctionInfoArgon setParallelism(final int parallelism) {
		this.parallelism = parallelism;
		return this;
	}

	public byte[] getSalt() {
		return salt;
	}

	public KeyDerivationFunctionInfoArgon setSalt(final byte[] salt) {
		this.salt = salt;
		return this;
	}

	public int getVersion() {
		return version;
	}

	public KeyDerivationFunctionInfoArgon setVersion(final int version) {
		this.version = version;
		return this;
	}

	@Override
	public byte[] getKdfParamsBytes() throws Exception {
		final VariantDictionary variantDictionary = new VariantDictionary();
		if (type == Argon2Type.Argon2_D) {
			variantDictionary.put(VariantDictionary.KDF_UUID, VariantDictionaryEntry.Type.BYTE_ARRAY, KeyDerivationFunction.ARGON2D.getId());
		} else {
			variantDictionary.put(VariantDictionary.KDF_UUID, VariantDictionaryEntry.Type.BYTE_ARRAY, KeyDerivationFunction.ARGON2ID.getId());
		}
		variantDictionary.put(VariantDictionary.KDF_ARGON2_VERSION, VariantDictionaryEntry.Type.UINT_32, version);
		variantDictionary.put(VariantDictionary.KDF_ARGON2_ITERATIONS, VariantDictionaryEntry.Type.UINT_64, (long) iterations);
		variantDictionary.put(VariantDictionary.KDF_ARGON2_MEMORY_IN_BYTES, VariantDictionaryEntry.Type.UINT_64, memoryInBytes);
		variantDictionary.put(VariantDictionary.KDF_ARGON2_PARALLELISM, VariantDictionaryEntry.Type.UINT_32, parallelism);
		if (salt == null) {
			salt = new byte[32];
			new SecureRandom().nextBytes(salt);
		}
		variantDictionary.put(VariantDictionary.KDF_ARGON2_SALT, VariantDictionaryEntry.Type.BYTE_ARRAY, salt);
		final ByteArrayOutputStream bufferStream = new ByteArrayOutputStream();
		variantDictionary.write(bufferStream);
		return bufferStream.toByteArray();
	}

	@Override
	public KeyDerivationFunctionInfoArgon setValues(final VariantDictionary variantDictionary) throws Exception {
		final KeyDerivationFunction keyDerivationFunction = KeyDerivationFunction.getById((byte[]) variantDictionary.get(VariantDictionary.KDF_UUID).getJavaValue());
		if (keyDerivationFunction == KeyDerivationFunction.ARGON2D) {
			setType(KeyDerivationFunctionInfoArgon.Argon2Type.Argon2_D);
		} else if (keyDerivationFunction == KeyDerivationFunction.ARGON2ID) {
			setType(KeyDerivationFunctionInfoArgon.Argon2Type.Argon2_ID);
		} else {
			throw new Exception("Invalid KeyDerivationFunction (KDF) for KeyDerivationFunctionInfoArgon: " + keyDerivationFunction);
		}
		setIterations(((Number) variantDictionary.get(VariantDictionary.KDF_ARGON2_ITERATIONS).getJavaValue()).intValue());
		setMemoryInBytes((((Number) variantDictionary.get(VariantDictionary.KDF_ARGON2_MEMORY_IN_BYTES).getJavaValue()).longValue()));
		setParallelism(((Number) variantDictionary.get(VariantDictionary.KDF_ARGON2_PARALLELISM).getJavaValue()).intValue());
		setSalt((byte[]) variantDictionary.get(VariantDictionary.KDF_ARGON2_SALT).getJavaValue());
		setVersion(((Number) variantDictionary.get(VariantDictionary.KDF_ARGON2_VERSION).getJavaValue()).intValue());
		return this;
	}

	@Override
	public void resetCryptoKeys() {
		salt = null;
	}
}
