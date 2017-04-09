package de.soderer.utilities;

import java.util.List;

public abstract class SecureDataEntry {
	public abstract void loadData(List<String> dataParts) throws Exception;

	public abstract String[] getStorageData();

	protected String entryName;

	public String getEntryName() {
		return entryName;
	}

	public void setEntryName(String entryName) {
		this.entryName = entryName;
	}
}
