package de.soderer.utilities.kdbx.data;

import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class KdbxMeta {
	private String generator;
	private String headerHash;
	private ZonedDateTime settingsChanged;
	private String databaseName;
	private ZonedDateTime databaseNameChanged;
	private String databaseDescription;
	private ZonedDateTime databaseDescriptionChanged;
	private String defaultUserName;
	private ZonedDateTime defaultUserNameChanged;
	private int maintenanceHistoryDays;
	private String color;
	private ZonedDateTime masterKeyChanged;
	private int masterKeyChangeRec;
	private int masterKeyChangeForce;
	private boolean masterKeyChangeForceOnce;
	private boolean recycleBinEnabled;
	private KdbxUUID recycleBinUUID;
	private ZonedDateTime recycleBinChanged;
	private KdbxUUID entryTemplatesGroup;
	private ZonedDateTime entryTemplatesGroupChanged;
	private int historyMaxItems;
	private int historyMaxSize;
	private KdbxUUID lastSelectedGroup;
	private KdbxUUID lastTopVisibleGroup;
	private KdbxMemoryProtection memoryProtection;
	private List<KdbxCustomDataItem> customData;
	public Map<KdbxUUID, byte[]> customIcons = new LinkedHashMap<>();

	/**
	 * Name of the program, which created the kdbx file
	 */
	public KdbxMeta setGenerator(final String generator) {
		this.generator = generator;
		return this;
	}

	/**
	 * Name of the program, which created the kdbx file
	 */
	public String getGenerator() {
		return generator;
	}

	/**
	 * SHA-256 hash of the KDBX header data as BLOB.
	 * Only utilized in KDBX 3.1 or lower.
	 * MAY also be present in KDBX 4.0 or higher.
	 */
	public KdbxMeta setHeaderHash(final String headerHash) {
		this.headerHash = headerHash;
		return this;
	}

	/**
	 * SHA-256 hash of the KDBX header data as BLOB.
	 * Only utilized in KDBX 3.1 or lower.
	 * MAY also be present in KDBX 4.0 or higher.
	 */
	public String getHeaderHash() {
		return headerHash;
	}

	/**
	 *  Datetime of change of settings or meta  data change
	 *  May only be present in KDBX 4.0 or higher.
	 */
	public KdbxMeta setSettingsChanged(final ZonedDateTime settingsChanged) {
		this.settingsChanged = settingsChanged;
		return this;
	}

	/**
	 *  Datetime of change of settings or meta  data change
	 *  May only be present in KDBX 4.0 or higher.
	 */
	public ZonedDateTime getSettingsChanged() {
		return settingsChanged;
	}

	/**
	 * Name of the database
	 */
	public KdbxMeta setDatabaseName(final String databaseName) {
		this.databaseName = databaseName;
		return this;
	}

	/**
	 * Name of the database
	 */
	public String getDatabaseName() {
		return databaseName;
	}

	/**
	 *  Datetime of database name change
	 */
	public KdbxMeta setDatabaseNameChanged(final ZonedDateTime databaseNameChanged) {
		this.databaseNameChanged = databaseNameChanged;
		return this;
	}

	/**
	 *  Datetime of database name change
	 */
	public ZonedDateTime getDatabaseNameChanged() {
		return databaseNameChanged;
	}

	/**
	 *  Database description
	 */
	public KdbxMeta setDatabaseDescription(final String databaseDescription) {
		this.databaseDescription = databaseDescription;
		return this;
	}

	/**
	 *  Database description
	 */
	public String getDatabaseDescription() {
		return databaseDescription;
	}

	/**
	 *  Datetime of database description change
	 */
	public KdbxMeta setDatabaseDescriptionChanged(final ZonedDateTime databaseDescriptionChanged) {
		this.databaseDescriptionChanged = databaseDescriptionChanged;
		return this;
	}

	/**
	 *  Datetime of database description change
	 */
	public ZonedDateTime getDatabaseDescriptionChanged() {
		return databaseDescriptionChanged;
	}

	/**
	 * Default username for new entries
	 */
	public KdbxMeta setDefaultUserName(final String defaultUserName) {
		this.defaultUserName = defaultUserName;
		return this;
	}

	/**
	 * Default username for new entries
	 */
	public String getDefaultUserName() {
		return defaultUserName;
	}

	/**
	 *  Datetime of default username change
	 */
	public KdbxMeta setDefaultUserNameChanged(final ZonedDateTime defaultUserNameChanged) {
		this.defaultUserNameChanged = defaultUserNameChanged;
		return this;
	}

	/**
	 *  Datetime of default username change
	 */
	public ZonedDateTime getDefaultUserNameChanged() {
		return defaultUserNameChanged;
	}

	/**
	 * Maximum age in days of history entries
	 */
	public KdbxMeta setMaintenanceHistoryDays(final int maintenanceHistoryDays) {
		this.maintenanceHistoryDays = maintenanceHistoryDays;
		return this;
	}

	/**
	 * Maximum age in days of history entries
	 */
	public int getMaintenanceHistoryDays() {
		return maintenanceHistoryDays;
	}

	/**
	 * Color for GUI display of database
	 * Six-digit hexadecimal RGB color code with a # prefix character
	 */
	public KdbxMeta setColor(final String color) {
		this.color = color;
		return this;
	}

	/**
	 * Color for GUI display of database
	 * Six-digit hexadecimal RGB color code with a # prefix character
	 */
	public String getColor() {
		return color;
	}

	/**
	 * Datetime of last master key change
	 */
	public KdbxMeta setMasterKeyChanged(final ZonedDateTime masterKeyChanged) {
		this.masterKeyChanged = masterKeyChanged;
		return this;
	}

	/**
	 * Datetime of last master key change
	 */
	public ZonedDateTime getMasterKeyChanged() {
		return masterKeyChanged;
	}

	/**
	 * Master key expiration in days for change recommendation (-1 => Unlimited)
	 */
	public KdbxMeta setMasterKeyChangeRec(final int masterKeyChangeRec) {
		this.masterKeyChangeRec = masterKeyChangeRec;
		return this;
	}

	/**
	 * Master key expiration in days for change recommendation (-1 => Unlimited)
	 */
	public int getMasterKeyChangeRec() {
		return masterKeyChangeRec;
	}

	/**
	 * Master key expiration in days for forced change (-1 => Unlimited)
	 */
	public KdbxMeta setMasterKeyChangeForce(final int masterKeyChangeForce) {
		this.masterKeyChangeForce = masterKeyChangeForce;
		return this;
	}

	/**
	 * Master key expiration in days for forced change (-1 => Unlimited)
	 */
	public int getMasterKeyChangeForce() {
		return masterKeyChangeForce;
	}

	/**
	 * Enforce master key change on next database open
	 */
	public KdbxMeta setMasterKeyChangeForceOnce(final boolean masterKeyChangeForceOnce) {
		this.masterKeyChangeForceOnce = masterKeyChangeForceOnce;
		return this;
	}

	/**
	 * Enforce master key change on next database open
	 */
	public boolean isMasterKeyChangeForceOnce() {
		return masterKeyChangeForceOnce;
	}

	/**
	 * Activation state of the recycling bin
	 */
	public KdbxMeta setRecycleBinEnabled(final boolean recycleBinEnabled) {
		this.recycleBinEnabled = recycleBinEnabled;
		if (recycleBinEnabled && recycleBinUUID == null) {
			recycleBinUUID = new KdbxUUID();
		}
		return this;
	}

	/**
	 * Activation state of the recycling bin
	 */
	public boolean isRecycleBinEnabled() {
		return recycleBinEnabled;
	}

	/**
	 * UUID of the recycling bin group
	 */
	public KdbxMeta setRecycleBinUUID(final KdbxUUID recycleBinUUID) {
		this.recycleBinUUID = recycleBinUUID;
		return this;
	}

	/**
	 * UUID of the recycling bin group
	 */
	public KdbxUUID getRecycleBinUUID() {
		return recycleBinUUID;
	}

	/**
	 * Datetime of recycling bin group change
	 */
	public KdbxMeta setRecycleBinChanged(final ZonedDateTime recycleBinChanged) {
		this.recycleBinChanged = recycleBinChanged;
		return this;
	}

	/**
	 * Datetime of recycling bin group change
	 */
	public ZonedDateTime getRecycleBinChanged() {
		return recycleBinChanged;
	}

	/**
	 * UUID of the group containing entry templates
	 */
	public KdbxMeta setEntryTemplatesGroup(final KdbxUUID entryTemplatesGroup) {
		this.entryTemplatesGroup = entryTemplatesGroup;
		return this;
	}

	/**
	 * UUID of the group containing entry templates
	 */
	public KdbxUUID getEntryTemplatesGroup() {
		return entryTemplatesGroup;
	}

	/**
	 * Datetime of entry templates group change
	 */
	public KdbxMeta setEntryTemplatesGroupChanged(final ZonedDateTime entryTemplatesGroupChanged) {
		this.entryTemplatesGroupChanged = entryTemplatesGroupChanged;
		return this;
	}

	/**
	 * Datetime of entry templates group change
	 */
	public ZonedDateTime getEntryTemplatesGroupChanged() {
		return entryTemplatesGroupChanged;
	}

	/**
	 * Maximum number of items in the history of entries
	 */
	public KdbxMeta setHistoryMaxItems(final int historyMaxItems) {
		this.historyMaxItems = historyMaxItems;
		return this;
	}

	/**
	 * Maximum number of items in the history of entries
	 */
	public int getHistoryMaxItems() {
		return historyMaxItems;
	}

	/**
	 * Maximum size in bytes of items in the history of entries
	 */
	public KdbxMeta setHistoryMaxSize(final int historyMaxSize) {
		this.historyMaxSize = historyMaxSize;
		return this;
	}

	/**
	 * Maximum size in bytes of items in the history of entries
	 */
	public int getHistoryMaxSize() {
		return historyMaxSize;
	}

	/**
	 * UUID of the last selected group
	 */
	public KdbxMeta setLastSelectedGroup(final KdbxUUID lastSelectedGroup) {
		this.lastSelectedGroup = lastSelectedGroup;
		return this;
	}

	/**
	 * UUID of the last selected group
	 */
	public KdbxUUID getLastSelectedGroup() {
		return lastSelectedGroup;
	}

	/**
	 * UUID of the last scroll visible group
	 */
	public KdbxMeta setLastTopVisibleGroup(final KdbxUUID lastTopVisibleGroup) {
		this.lastTopVisibleGroup = lastTopVisibleGroup;
		return this;
	}

	/**
	 * UUID of the last scroll visible group
	 */
	public KdbxUUID getLastTopVisibleGroup() {
		return lastTopVisibleGroup;
	}

	/**
	 * Structure containing configuration of value protection
	 */
	public KdbxMeta setMemoryProtection(final KdbxMemoryProtection memoryProtection) {
		this.memoryProtection = memoryProtection;
		return this;
	}

	/**
	 * Structure containing configuration of value protection
	 */
	public KdbxMemoryProtection getMemoryProtection() {
		if (memoryProtection == null) {
			memoryProtection = new KdbxMemoryProtection();
		}
		return memoryProtection;
	}

	/**
	 * Binary data items of stored files.
	 * Only utilized in KDBX 3.1 or lower.
	 * MAY not be present in KDBX 4.0 or higher.
	 */
	public KdbxMeta setCustomData(final List<KdbxCustomDataItem> customData) {
		this.customData = customData;
		return this;
	}

	/**
	 * Binary data items of stored files.
	 * Only utilized in KDBX 3.1 or lower.
	 * MAY not be present in KDBX 4.0 or higher.
	 */
	public List<KdbxCustomDataItem> getCustomData() {
		return customData;
	}

	/**
	 * Binary data of custom configured icons
	 */
	public KdbxMeta setCustomIcons(final Map<KdbxUUID, byte[]> customIcons) {
		this.customIcons = customIcons;
		return this;
	}

	/**
	 * Binary data of custom configured icons
	 */
	public Map<KdbxUUID, byte[]> getCustomIcons() {
		return customIcons;
	}
}
