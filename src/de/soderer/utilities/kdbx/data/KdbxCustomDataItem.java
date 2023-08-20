package de.soderer.utilities.kdbx.data;

import java.time.ZonedDateTime;

public class KdbxCustomDataItem {
	public String key;
	public String value;
	public ZonedDateTime lastModificationTime = null;

	public KdbxCustomDataItem setKey(final String key) {
		this.key = key;
		return this;
	}

	public String getKey() {
		return key;
	}

	public KdbxCustomDataItem setValue(final String value) {
		this.value = value;
		return this;
	}

	public String getValue() {
		return value;
	}

	public KdbxCustomDataItem setLastModificationTime(final ZonedDateTime lastModificationTime) {
		this.lastModificationTime = lastModificationTime;
		return this;
	}

	public ZonedDateTime getLastModificationTime() {
		return lastModificationTime;
	}
}
