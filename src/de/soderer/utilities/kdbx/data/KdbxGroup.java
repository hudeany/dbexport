package de.soderer.utilities.kdbx.data;

import java.util.ArrayList;
import java.util.List;

public class KdbxGroup {
	public String name;
	public KdbxUUID uuid;
	private KdbxTimes times = new KdbxTimes();
	public String notes;
	public Integer iconID;
	public KdbxUUID customIconUuid;
	public boolean expanded;
	public String defaultAutoTypeSequence;
	public boolean enableAutoType;
	public boolean enableSearching;
	public KdbxUUID lastTopVisibleEntry;
	private List<KdbxCustomDataItem> customData;
	public List<KdbxGroup> groups = new ArrayList<>();
	public List<KdbxEntry> entries = new ArrayList<>();

	/**
	 * Name of this group
	 */
	public KdbxGroup setName(final String name) {
		this.name = name;
		return this;
	}

	/**
	 * Name of this group
	 */
	public String getName() {
		return name;
	}

	public KdbxGroup setUuid(final KdbxUUID uuid) {
		this.uuid = uuid;
		return this;
	}

	public KdbxUUID getUuid() {
		if (uuid == null) {
			uuid = new KdbxUUID();
		}
		return uuid;
	}

	/**
	 * KDBX times of this group
	 */
	public KdbxGroup setTimes(final KdbxTimes times) {
		if (times == null) {
			throw new IllegalArgumentException("Group's times may not be null");
		} else {
			this.times = times;
			return this;
		}
	}

	/**
	 * KDBX times of this group
	 */
	public KdbxTimes getTimes() {
		return times;
	}

	/**
	 * Notes for this group
	 */
	public KdbxGroup setNotes(final String notes) {
		this.notes = notes;
		return this;
	}

	/**
	 * Notes for this group
	 */
	public String getNotes() {
		return notes;
	}

	/**
	 * Standard icon id for this group
	 */
	public KdbxGroup setIconID(final Integer iconID) {
		this.iconID = iconID;
		return this;
	}

	/**
	 * Standard icon id for this group
	 */
	public Integer getIconID() {
		return iconID;
	}

	/**
	 * Custom icon uuid for this group, which is stored in meta data of its database
	 */
	public KdbxGroup setCustomIconUuid(final KdbxUUID customIconUuid) {
		this.customIconUuid = customIconUuid;
		return this;
	}

	/**
	 * Custom icon uuid for this group, which is stored in meta data of its database
	 */
	public KdbxUUID getCustomIconUuid() {
		return customIconUuid;
	}

	/**
	 * This group is shown in expanded state in a GUI
	 */
	public KdbxGroup setExpanded(final boolean isExpanded) {
		expanded = isExpanded;
		return this;
	}

	/**
	 * This group is shown in expanded state in a GUI
	 */
	public boolean isExpanded() {
		return expanded;
	}

	/**
	 * Default auto type sequence for this group
	 */
	public KdbxGroup setDefaultAutoTypeSequence(final String defaultAutoTypeSequence) {
		this.defaultAutoTypeSequence = defaultAutoTypeSequence;
		return this;
	}

	/**
	 * Default auto type sequence for this group
	 */
	public String getDefaultAutoTypeSequence() {
		return defaultAutoTypeSequence;
	}

	/**
	 * Auto type is enabled for this group
	 */
	public KdbxGroup setEnableAutoType(final boolean enableAutoType) {
		this.enableAutoType = enableAutoType;
		return this;
	}

	/**
	 * Auto type is enabled for this group
	 */
	public boolean isEnableAutoType() {
		return enableAutoType;
	}

	/**
	 * Include this group is search operations
	 */
	public KdbxGroup setEnableSearching(final boolean enableSearching) {
		this.enableSearching = enableSearching;
		return this;
	}

	/**
	 * Include this group is search operations
	 */
	public boolean isEnableSearching() {
		return enableSearching;
	}

	/**
	 * UUID of the last scroll visible entry
	 */
	public KdbxGroup setLastTopVisibleEntry(final KdbxUUID lastTopVisibleEntry) {
		this.lastTopVisibleEntry = lastTopVisibleEntry;
		return this;
	}

	/**
	 * UUID of the last scroll visible entry
	 */
	public KdbxUUID getLastTopVisibleEntry() {
		return lastTopVisibleEntry;
	}

	/**
	 * Binary data items of stored files for this group.
	 * MAY ONLY be present in KDBX 4.0 or higher.
	 */
	public KdbxGroup setCustomData(final List<KdbxCustomDataItem> customData) {
		this.customData = customData;
		return this;
	}

	/**
	 * Binary data items of stored files for this group.
	 * MAY ONLY be present in KDBX 4.0 or higher.
	 */
	public List<KdbxCustomDataItem> getCustomData() {
		return customData;
	}

	/**
	 * Sub groups of this group
	 */
	public KdbxGroup setGroups(final List<KdbxGroup> groups) {
		this.groups = groups;
		return this;
	}

	/**
	 * Sub groups of this group
	 */
	public List<KdbxGroup> getGroups() {
		return groups;
	}

	/**
	 * Entries of this group
	 */
	public KdbxGroup setEntries(final List<KdbxEntry> entries) {
		this.entries = entries;
		return this;
	}

	/**
	 * Entries of this group
	 */
	public List<KdbxEntry> getEntries() {
		return entries;
	}

	public KdbxGroup getGroupByUUID(final KdbxUUID groupUuid) {
		for (final KdbxGroup group : groups) {
			if (group.getUuid().equals(groupUuid)) {
				return group;
			}
		}
		return null;
	}

	public KdbxEntry getEntryByUUID(final KdbxUUID entryUuid) {
		for (final KdbxEntry entry : entries) {
			if (entry.getUuid().equals(entryUuid)) {
				return entry;
			}
		}
		for (final KdbxGroup group : groups) {
			final KdbxEntry entry = group.getEntryByUUID(entryUuid);
			if (entry != null) {
				return entry;
			}
		}
		return null;
	}

	public List<KdbxUUID> getUuidPath(final KdbxUUID uuidToSearch) {
		for (final KdbxEntry entry : entries) {
			if (entry.getUuid().equals(uuidToSearch)) {
				final List<KdbxUUID> pathUuids = new ArrayList<>();
				pathUuids.add(entry.getUuid());
				return pathUuids;
			}
		}
		for (final KdbxGroup group : groups) {
			if (group.getUuid().equals(uuidToSearch)) {
				final List<KdbxUUID> pathUuids = new ArrayList<>();
				pathUuids.add(group.getUuid());
				return pathUuids;
			} else {
				final List<KdbxUUID> subPathUuids = group.getUuidPath(uuidToSearch);
				if (subPathUuids != null) {
					subPathUuids.add(0, getUuid());
					return subPathUuids;
				}
			}
		}
		return null;
	}

	public List<KdbxGroup> getAllGroups() {
		final List<KdbxGroup> groupsList = new ArrayList<>();
		for (final KdbxGroup group : groups) {
			groupsList.add(group);
		}
		for (final KdbxGroup group : groups) {
			groupsList.addAll(group.getAllGroups());
		}
		return groupsList;
	}

	public List<KdbxEntry> getAllEntries() {
		final List<KdbxEntry> entriesList = new ArrayList<>();
		for (final KdbxEntry entry : entries) {
			entriesList.add(entry);
		}
		for (final KdbxGroup group : groups) {
			entriesList.addAll(group.getAllEntries());
		}
		return entriesList;
	}
}
