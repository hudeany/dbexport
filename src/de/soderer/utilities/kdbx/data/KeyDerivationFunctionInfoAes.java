package de.soderer.utilities.kdbx.data;

import java.io.ByteArrayOutputStream;
import java.security.SecureRandom;

import de.soderer.utilities.kdbx.data.KdbxConstants.KeyDerivationFunction;
import de.soderer.utilities.kdbx.util.VariantDictionary;
import de.soderer.utilities.kdbx.util.VariantDictionaryEntry;

public class KeyDerivationFunctionInfoAes implements KeyDerivationFunctionInfo {
	public enum KdbxType {
		AES_KDBX3,
		AES_KDBX4;
	}

	private KdbxType aesKdbxType = KeyDerivationFunctionInfoAes.KdbxType.AES_KDBX3;
	private long aesTransformRounds = 60000;
	private byte[] aesTransformSeed;

	public KdbxType getAesKdbxType() {
		return aesKdbxType;
	}

	public void setAesKdbxType(final KdbxType aesKdbxType) {
		this.aesKdbxType = aesKdbxType;
	}

	public long getAesTransformRounds() {
		return aesTransformRounds;
	}

	public KeyDerivationFunctionInfoAes setAesTransformRounds(final long aesTransformRounds) {
		this.aesTransformRounds = aesTransformRounds;
		return this;
	}

	public byte[] getAesTransformSeed() {
		return aesTransformSeed;
	}

	public KeyDerivationFunctionInfoAes setAesTransformSeed(final byte[] aesTransformSeed) {
		this.aesTransformSeed = aesTransformSeed;
		return this;
	}

	@Override
	public byte[] getKdfParamsBytes() throws Exception {
		final VariantDictionary variantDictionary = new VariantDictionary();
		if (getAesKdbxType() == KdbxType.AES_KDBX3) {
			variantDictionary.put(VariantDictionary.KDF_UUID, VariantDictionaryEntry.Type.BYTE_ARRAY, KeyDerivationFunction.AES_KDBX3.getId());
		} else {
			variantDictionary.put(VariantDictionary.KDF_UUID, VariantDictionaryEntry.Type.BYTE_ARRAY, KeyDerivationFunction.AES_KDBX4.getId());
		}
		variantDictionary.put(VariantDictionary.KDF_AES_ROUNDS, VariantDictionaryEntry.Type.UINT_64, aesTransformRounds);
		if (aesTransformSeed == null) {
			aesTransformSeed = new byte[32];
			new SecureRandom().nextBytes(aesTransformSeed);
		}
		variantDictionary.put(VariantDictionary.KDF_AES_SEED, VariantDictionaryEntry.Type.BYTE_ARRAY, aesTransformSeed);
		final ByteArrayOutputStream bufferStream = new ByteArrayOutputStream();
		variantDictionary.write(bufferStream);
		return bufferStream.toByteArray();
	}

	@Override
	public KeyDerivationFunctionInfoAes setValues(final VariantDictionary variantDictionary) throws Exception {
		final KeyDerivationFunction keyDerivationFunction = KeyDerivationFunction.getById((byte[]) variantDictionary.get(VariantDictionary.KDF_UUID).getJavaValue());
		if (keyDerivationFunction == KeyDerivationFunction.AES_KDBX3) {
			setAesKdbxType(KeyDerivationFunctionInfoAes.KdbxType.AES_KDBX3);
		} else if (keyDerivationFunction == KeyDerivationFunction.AES_KDBX4) {
			setAesKdbxType(KeyDerivationFunctionInfoAes.KdbxType.AES_KDBX4);
		} else {
			throw new Exception("Invalid KeyDerivationFunction (KDF) for KeyDerivationFunctionInfoAes: " + keyDerivationFunction);
		}
		setAesTransformRounds((Long) variantDictionary.get(VariantDictionary.KDF_AES_ROUNDS).getJavaValue());
		setAesTransformSeed((byte[]) variantDictionary.get(VariantDictionary.KDF_AES_SEED).getJavaValue());
		return this;
	}

	@Override
	public void resetCryptoKeys() {
		aesTransformSeed = null;
	}
}
