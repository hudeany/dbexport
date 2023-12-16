package de.soderer.dbexport;

/**
 * The Class DbExportException.
 */
public class DbExportException extends Exception {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 6039775378389122712L;

	/**
	 * Instantiates a new database csv export exception.
	 *
	 * @param errorMessage
	 *            the error message
	 */
	public DbExportException(final String errorMessage) {
		super(errorMessage);
	}

	public DbExportException(final String errorMessage, final Exception e) {
		super(errorMessage, e);
	}
}
