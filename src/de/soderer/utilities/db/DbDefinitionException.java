package de.soderer.utilities.db;

/**
 * The Class DbExportException.
 */
public class DbDefinitionException extends Exception {
	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 6039775378389122712L;

	/**
	 * Instantiates a new db definition exception.
	 *
	 * @param errorMessage
	 *            the error message
	 */
	public DbDefinitionException(final String errorMessage) {
		super(errorMessage);
	}

	public DbDefinitionException(final String errorMessage, final Exception e) {
		super(errorMessage, e);
	}
}
