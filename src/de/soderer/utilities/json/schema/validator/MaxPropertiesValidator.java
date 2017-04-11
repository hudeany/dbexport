package de.soderer.utilities.json.schema.validator;

import de.soderer.utilities.json.JsonNode;
import de.soderer.utilities.json.JsonObject;
import de.soderer.utilities.json.schema.JsonSchemaDataValidationError;
import de.soderer.utilities.json.schema.JsonSchemaDefinitionError;
import de.soderer.utilities.json.schema.JsonSchemaDependencyResolver;

public class MaxPropertiesValidator extends BaseJsonSchemaValidator {
	public MaxPropertiesValidator(JsonSchemaDependencyResolver jsonSchemaDependencyResolver, String jsonSchemaPath, Object validatorData, JsonNode jsonNode, String jsonPath) throws JsonSchemaDefinitionError {
		super(jsonSchemaDependencyResolver, jsonSchemaPath, validatorData, jsonNode, jsonPath);
		
		if (!(validatorData instanceof Integer)) {
			throw new JsonSchemaDefinitionError("Data for maximum property keys amount is not an integer", jsonSchemaPath);
    	} else if (validatorData instanceof String) {
    		try {
    			this.validatorData = Integer.parseInt((String) validatorData);
			} catch (NumberFormatException e) {
				throw new JsonSchemaDefinitionError("Data for maximum property keys amount '" + validatorData + "' is not a number", jsonSchemaPath);
			}
    	} else if (((Integer) validatorData) < 0) {
			throw new JsonSchemaDefinitionError("Data for maximum property keys amount is negative", jsonSchemaPath);
    	}
	}
	
	@Override
	public void validate() throws JsonSchemaDefinitionError, JsonSchemaDataValidationError {
		if (!(jsonNode.isJsonObject())) {
			if (!jsonSchemaDependencyResolver.isUseDraftV4Mode()) {
				throw new JsonSchemaDataValidationError("Expected data type 'object' but was '" + jsonNode.getJsonDataType().getName() + "'", jsonPath);
			}
		} else {
			if (((JsonObject) jsonNode.getValue()).keySet().size() > ((Integer) validatorData)) {
				throw new JsonSchemaDataValidationError("Required maximum number of properties is '" + ((Integer) validatorData) + "' but was '" + ((JsonObject) jsonNode.getValue()).keySet().size()  + "'", jsonPath);
			}
		}
    }
}
