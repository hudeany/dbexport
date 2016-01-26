package de.soderer.dbcsvexport;

public class ParameterException extends Exception {
	private static final long serialVersionUID = -4435007822821471026L;
	 
	private String parameter;
	
	public String getParameter() {
		return parameter;
	}
	
	public ParameterException(String parameter, String errorMessage) {
		super(errorMessage + " Parameter: " + parameter);
		this.parameter = parameter;
	}
}
