package de.soderer.utilities.json.schema.validator;

import de.soderer.utilities.json.JsonNode;
import de.soderer.utilities.json.schema.JsonSchemaDataValidationError;
import de.soderer.utilities.json.schema.JsonSchemaDefinitionError;
import de.soderer.utilities.json.schema.JsonSchemaDependencyResolver;

public class MaxLengthValidator extends BaseJsonSchemaValidator {
	public MaxLengthValidator(JsonSchemaDependencyResolver jsonSchemaDependencyResolver, String jsonSchemaPath, Object validatorData, JsonNode jsonNode, String jsonPath) throws JsonSchemaDefinitionError {
		super(jsonSchemaDependencyResolver, jsonSchemaPath, validatorData, jsonNode, jsonPath);
		
		if (!(validatorData instanceof Integer)) {
			throw new JsonSchemaDefinitionError("Data for maxLength is not an integer", jsonSchemaPath);
    	} else if (validatorData instanceof String) {
    		try {
    			this.validatorData = Integer.parseInt((String) validatorData);
			} catch (NumberFormatException e) {
				throw new JsonSchemaDefinitionError("Data for maxLength '" + validatorData + "' is not an integer", jsonSchemaPath);
			}
    	} else if (((Integer) validatorData) < 0) {
			throw new JsonSchemaDefinitionError("Data for maxLength is negative", jsonSchemaPath);
    	}
	}
	
	@Override
	public void validate() throws JsonSchemaDefinitionError, JsonSchemaDataValidationError {
		if (!(jsonNode.isString())) {
			if (!jsonSchemaDependencyResolver.isUseDraftV4Mode()) {
				throw new JsonSchemaDataValidationError("Expected data type 'string' but was '" + jsonNode.getJsonDataType().getName() + "'", jsonPath);
			}
		} else {
			if (((String) jsonNode.getValue()).length() > ((Integer) validatorData)) {
				throw new JsonSchemaDataValidationError("String maxLength is '" + ((Integer) validatorData) + "' but was '" + ((String) jsonNode.getValue()).length()  + "'", jsonPath);
			}
		}
    }
}
