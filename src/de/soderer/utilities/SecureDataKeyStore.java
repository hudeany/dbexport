package de.soderer.utilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

public class SecureDataKeyStore {
	private File keystoreFile;
	
	/**
	 * First key is class name of entries grouped by their class name, second key is the entry name
	 */
	private Map<String, Map<String, SecureDataEntry>> dataEntries = new HashMap<String, Map<String, SecureDataEntry>>();

	public SecureDataKeyStore(File keystoreFile) {
		this.keystoreFile = keystoreFile;
	}

	public void loadKeyStore(char[] passwordArray) throws Exception {
		FileInputStream fis = null;
		try {
			KeyStore keyStore = KeyStore.getInstance("JCEKS");

			if (keystoreFile.exists()) {
				fis = new FileInputStream(keystoreFile);
			}
			keyStore.load(fis, passwordArray);

			dataEntries = new HashMap<String, Map<String, SecureDataEntry>>();

			for (String keyAlias : Collections.list(keyStore.aliases())) {
				SecretKey secretKey = (SecretKey) keyStore.getKey(keyAlias, passwordArray);
				String data = new String(secretKey.getEncoded(), "UTF-8");
				List<String> dataParts = CsvReader.parseCsvLine(new CsvFormat().setSeparator(',').setStringQuote('"'), data);
				String secureDataEntryClassName = dataParts.get(0);
				try {
					Class.forName(secureDataEntryClassName);
					dataParts.remove(0);
				} catch (ClassNotFoundException e) {
					throw new Exception("Class of SecureDataEntry '" + secureDataEntryClassName + "' not found");
				}
				if (!SecureDataEntry.class.isAssignableFrom(Class.forName(secureDataEntryClassName))) {
					throw new Exception("Class '" + secureDataEntryClassName + "' is not derived from SecureDataEntry");
				}
				SecureDataEntry newDataEntry = (SecureDataEntry) Class.forName(secureDataEntryClassName).newInstance();
				newDataEntry.loadData(dataParts);
				if (dataEntries.get(secureDataEntryClassName) == null) {
					dataEntries.put(secureDataEntryClassName, new HashMap<String, SecureDataEntry>());
				}
				dataEntries.get(secureDataEntryClassName).put(newDataEntry.getEntryName(), newDataEntry);
			}
		} finally {
			Arrays.fill(passwordArray, '\u0000');
			Utilities.closeQuietly(fis);
		}
	}

	public void saveKeyStore(char[] passwordArray) {
		FileOutputStream fos = null;
		try {
			KeyStore keyStore = KeyStore.getInstance("JCEKS");

			keyStore.load(null, passwordArray);

			fos = new FileOutputStream(keystoreFile);
			for (Entry<String, Map<String, SecureDataEntry>> entryMap : dataEntries.entrySet()) {
				int i = 1;
				for (Entry<String, SecureDataEntry> entry : entryMap.getValue().entrySet()) {
					keyStore.setKeyEntry(entryMap.getKey() + "/" + (i++),
							new SecretKeySpec(("\"" + entry.getValue().getClass().getName() + "\"," + CsvWriter.getCsvLine(',', '"', (Object[]) entry.getValue().getStorageData())).getBytes("UTF-8"), "AES"),
							passwordArray, null);
				}
			}
			keyStore.store(fos, passwordArray);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			Arrays.fill(passwordArray, '\u0000');
			Utilities.closeQuietly(fos);
		}
	}

	public Set<String> getEntryNames(Class<?> classType) {
		if (dataEntries.get(classType.getName()) == null) {
			return new HashSet<String>();
		} else {
			return dataEntries.get(classType.getName()).keySet();
		}
	}

	public <T> T getEntry(Class<T> classType, String entryName) {
		if (dataEntries.get(classType.getName()) == null) {
			return null;
		}
		@SuppressWarnings("unchecked")
		T returnValue = (T) dataEntries.get(classType.getName()).get(entryName);
		return returnValue;
	}

	public void addEntry(SecureDataEntry entry) {
		if (dataEntries.get(entry.getClass().getName()) == null) {
			dataEntries.put(entry.getClass().getName(), new HashMap<String, SecureDataEntry>());
		}
		dataEntries.get(entry.getClass().getName()).put(entry.getEntryName(), entry);
	}

	public void remove(Class<?> classType, String entryName) {
		dataEntries.get(classType.getName()).remove(entryName);
	}

	public void remove(SecureDataEntry entry) {
		if (dataEntries.get(entry.getClass().getName()) != null) {
			dataEntries.get(entry.getClass().getName()).remove(entry.getEntryName());
		}
	}
}
