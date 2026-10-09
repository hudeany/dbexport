package de.soderer.dbexport;

/**
 * Exception for errors of the DbExport application, e.g. invalid parameters or export settings.
 */
public class DbExportException extends Exception {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 6039775378389122712L;

	/**
	 * Creates an exception with an error message.
	 *
	 * @param errorMessage the error message
	 */
	public DbExportException(final String errorMessage) {
		super(errorMessage);
	}

	/**
	 * Creates an exception with an error message and its cause.
	 *
	 * @param errorMessage the error message
	 * @param e the cause
	 */
	public DbExportException(final String errorMessage, final Exception e) {
		super(errorMessage, e);
	}
}
