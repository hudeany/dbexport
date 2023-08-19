package de.soderer.utilities.kdbx.data;

import java.time.ZonedDateTime;

public class KdbxTimes {
	public ZonedDateTime lastModificationTime;
	public ZonedDateTime creationTime;
	public ZonedDateTime lastAccessTime;
	public ZonedDateTime expiryTime;
	public boolean expires;
	public int usageCount;
	public ZonedDateTime locationChanged;

	public KdbxTimes() {
		creationTime = ZonedDateTime.now();
		lastModificationTime = creationTime;
	}

	public ZonedDateTime getLastModificationTime() {
		return lastModificationTime;
	}

	public KdbxTimes setLastModificationTime(final ZonedDateTime lastModificationTime) {
		this.lastModificationTime = lastModificationTime;
		return this;
	}

	public ZonedDateTime getCreationTime() {
		return creationTime;
	}

	public KdbxTimes setCreationTime(final ZonedDateTime creationTime) {
		this.creationTime = creationTime;
		return this;
	}

	public ZonedDateTime getLastAccessTime() {
		return lastAccessTime;
	}

	public KdbxTimes setLastAccessTime(final ZonedDateTime lastAccessTime) {
		this.lastAccessTime = lastAccessTime;
		return this;
	}

	public ZonedDateTime getExpiryTime() {
		return expiryTime;
	}

	public KdbxTimes setExpiryTime(final ZonedDateTime expiryTime) {
		this.expiryTime = expiryTime;
		return this;
	}

	public boolean isExpires() {
		return expires;
	}

	public KdbxTimes setExpires(final boolean expires) {
		this.expires = expires;
		return this;
	}

	public int getUsageCount() {
		return usageCount;
	}

	public KdbxTimes setUsageCount(final int usageCount) {
		this.usageCount = usageCount;
		return this;
	}

	public ZonedDateTime getLocationChanged() {
		return locationChanged;
	}

	public KdbxTimes setLocationChanged(final ZonedDateTime locationChanged) {
		this.locationChanged = locationChanged;
		return this;
	}
}
