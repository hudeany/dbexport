package de.soderer.dbimport;

/**
 * The Class DbImportException.
 */
public class DbImportException extends Exception {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 6039775378389122712L;

	/**
	 * Instantiates a new db csv import exception.
	 *
	 * @param errorMessage
	 *            the error message
	 */
	public DbImportException(String errorMessage) {
		super(errorMessage);
	}
	
	public DbImportException(String errorMessage, Exception e) {
		super(errorMessage, e);
	}
}
