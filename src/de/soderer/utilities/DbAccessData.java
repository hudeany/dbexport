package de.soderer.utilities;

import java.io.IOException;
import java.util.List;

public class DbAccessData extends SecureDataEntry {
	private boolean isOracle;
	private String dbServerHostname;
	private int dbServerPort;
	private String dbName;
	private String dbTable;
	private String userName;
	private String password;

	protected DbAccessData() {
	}

	public DbAccessData(String entryname, boolean isOracle, String dbServerHostname, int dbServerPort, String dbName, String dbTable, String userName, String password) {
		entryName = entryname;
		this.isOracle = isOracle;
		this.dbServerHostname = dbServerHostname;
		this.dbServerPort = dbServerPort;
		this.dbName = dbName;
		this.dbTable = dbTable;
		this.userName = userName;
		this.password = password;
	}

	@Override
	public void loadData(List<String> dataParts) throws Exception {
		if (dataParts == null || dataParts.size() != 8) {
			throw new IOException("Invalid data in DbEntry");
		} else {
			try {
				entryName = dataParts.get(0);
				isOracle = Utilities.interpretAsBool(dataParts.get(1));
				dbServerHostname = dataParts.get(2);
				dbServerPort = Integer.parseInt(dataParts.get(3));
				dbName = dataParts.get(4);
				dbTable = dataParts.get(5);
				userName = dataParts.get(6);
				password = dataParts.get(7);
			} catch (NumberFormatException e) {
				throw new IOException("Invalid data in DbEntry");
			}
		}
	}

	@Override
	public String[] getStorageData() {
		return new String[] { entryName, Boolean.toString(isOracle), dbServerHostname, Integer.toString(dbServerPort), dbName, dbTable, userName, password };
	}

	public boolean equals(DbAccessData otherData) {
		return entryName.equals(otherData.getEntryName());
	}

	public boolean isOracle() {
		return isOracle;
	}

	public String getDbServerHostname() {
		return dbServerHostname;
	}

	public int getDbServerPort() {
		return dbServerPort;
	}

	public String getDbName() {
		return dbName;
	}

	public String getDbTable() {
		return dbTable;
	}

	public String getUserName() {
		return userName;
	}

	public String getPassword() {
		return password;
	}
}