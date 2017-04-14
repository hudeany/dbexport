package de.soderer.utilities.json.schema;

public class JsonSchemaDataValidationError extends Exception {
	private static final long serialVersionUID = -4849599671599546633L;
	
	private String jsonDataPath;
	
	public JsonSchemaDataValidationError(String message, String jsonDataPath) {
		super(message);
		
		this.jsonDataPath = jsonDataPath;
	}
	
	public String getJsonDataPath() {
		return jsonDataPath;
	}
	
	@Override
	public String getMessage() {
		return "Invalid JSON data: " + super.getMessage() + (jsonDataPath == null ? "" : " at JSON path: " + jsonDataPath);
	}
}
