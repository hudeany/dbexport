package de.soderer.utilities;

import java.io.IOException;
import java.util.List;

public class SimpleCredentialsData extends SecureDataEntry {
	private String userName;
	private char[] password;

	protected SimpleCredentialsData() {
	}

	public SimpleCredentialsData(String entryname, String userName, char[] password) {
		entryName = entryname;
		this.userName = userName;
		this.password = password;
	}

	@Override
	public void loadData(List<String> dataParts) throws Exception {
		if (dataParts == null || dataParts.size() != 3) {
			throw new IOException("Invalid data in SimpleCredentialsEntry");
		} else {
			entryName = dataParts.get(0);
			userName = dataParts.get(1);
			password = dataParts.get(2).toCharArray();
		}
	}

	@Override
	public String[] getStorageData() {
		return new String[] { entryName, userName, new String(password) };
	}

	public boolean equals(SimpleCredentialsData otherData) {
		return entryName.equals(otherData.getEntryName());
	}

	public String getUserName() {
		return userName;
	}

	public char[] getPassword() {
		return password;
	}
}