package de.soderer.utilities.json.schema.validator;

import de.soderer.utilities.json.JsonArray;
import de.soderer.utilities.json.JsonNode;
import de.soderer.utilities.json.schema.JsonSchemaDataValidationError;
import de.soderer.utilities.json.schema.JsonSchemaDefinitionError;
import de.soderer.utilities.json.schema.JsonSchemaDependencyResolver;

public class MinItemsValidator extends BaseJsonSchemaValidator {
	public MinItemsValidator(JsonSchemaDependencyResolver jsonSchemaDependencyResolver, String jsonSchemaPath, Object validatorData, JsonNode jsonNode, String jsonPath) throws JsonSchemaDefinitionError {
		super(jsonSchemaDependencyResolver, jsonSchemaPath, validatorData, jsonNode, jsonPath);
		
		if (validatorData == null) {
			throw new JsonSchemaDefinitionError("Data for minimum items is 'null'", jsonSchemaPath);
		} else if (validatorData instanceof Integer) {
			if (((Integer) validatorData) < 0) {
				throw new JsonSchemaDefinitionError("Data for minimum items amount is  negative", jsonSchemaPath);
	    	}
    	} else if (validatorData instanceof String) {
    		try {
    			this.validatorData = Integer.parseInt((String) validatorData);
			} catch (NumberFormatException e) {
				throw new JsonSchemaDefinitionError("Data for minimum items '" + validatorData + "' is not a number", jsonSchemaPath);
			}
    	}
	}
	
	@Override
	public void validate() throws JsonSchemaDefinitionError, JsonSchemaDataValidationError {
		if (!(jsonNode.isJsonArray())) {
			if (!jsonSchemaDependencyResolver.isUseDraftV4Mode()) {
				throw new JsonSchemaDataValidationError("Expected data type 'array' but was '" + jsonNode.getJsonDataType().getName() + "'", jsonPath);
			}
		} else {
			if (((JsonArray) jsonNode.getValue()).size() < ((Integer) validatorData)) {
				throw new JsonSchemaDataValidationError("Required minimum number of items is '" + ((Integer) validatorData) + "' but was '" + ((JsonArray) jsonNode.getValue()).size()  + "'", jsonPath);
			}
		}
    }
}
