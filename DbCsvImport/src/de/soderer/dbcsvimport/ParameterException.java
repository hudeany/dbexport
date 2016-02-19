package de.soderer.dbcsvimport;

/**
 * The Class ParameterException.
 */
public class ParameterException extends Exception {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -4435007822821471026L;

	/** The parameter. */
	private String parameter;

	/**
	 * Gets the parameter.
	 *
	 * @return the parameter
	 */
	public String getParameter() {
		return parameter;
	}

	/**
	 * Instantiates a new parameter exception.
	 *
	 * @param parameter
	 *            the parameter
	 * @param errorMessage
	 *            the error message
	 */
	public ParameterException(String parameter, String errorMessage) {
		super(errorMessage + " Parameter: " + parameter);
		this.parameter = parameter;
	}
}
