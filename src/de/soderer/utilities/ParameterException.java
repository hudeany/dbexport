package de.soderer.utilities;

/**
 * The Class ParameterException.
 */
public class ParameterException extends Exception {

	/** The Constant serialVersionUID */
	private static final long serialVersionUID = -4435007822821471026L;

	/** The parameter */
	private String parameter = null;

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
	 * @param errorMessage
	 *            the error message
	 */
	public ParameterException(final String errorMessage) {
		super(errorMessage);
	}

	/**
	 * Instantiates a new parameter exception.
	 *
	 * @param parameter
	 *            the parameter
	 * @param errorMessage
	 *            the error message
	 */
	public ParameterException(final String parameter, final String errorMessage) {
		super(errorMessage + " (Parameter: " + parameter + ")");
		this.parameter = parameter;
	}
}
