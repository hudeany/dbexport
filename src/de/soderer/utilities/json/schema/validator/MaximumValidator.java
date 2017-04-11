package de.soderer.utilities.json.schema.validator;

import de.soderer.utilities.NumberUtilities;
import de.soderer.utilities.json.JsonNode;
import de.soderer.utilities.json.JsonObject;
import de.soderer.utilities.json.schema.JsonSchemaDataValidationError;
import de.soderer.utilities.json.schema.JsonSchemaDefinitionError;
import de.soderer.utilities.json.schema.JsonSchemaDependencyResolver;

public class MaximumValidator extends ExtendedBaseJsonSchemaValidator {
	public MaximumValidator(JsonObject parentValidatorData, JsonSchemaDependencyResolver jsonSchemaDependencyResolver, String jsonSchemaPath, Object validatorData, JsonNode jsonNode, String jsonPath) throws JsonSchemaDefinitionError {
		super(parentValidatorData, jsonSchemaDependencyResolver, jsonSchemaPath, validatorData, jsonNode, jsonPath);

		if (validatorData == null) {
			throw new JsonSchemaDefinitionError("Data for maximum is null", jsonSchemaPath);
    	} else if (validatorData instanceof String) {
    		try {
    			this.validatorData = NumberUtilities.parseNumber((String) validatorData);
			} catch (NumberFormatException e) {
				throw new JsonSchemaDefinitionError("Data for maximum '" + validatorData + "' is not a number", jsonSchemaPath);
			}
    	} else if (!(validatorData instanceof Number)) {
			throw new JsonSchemaDefinitionError("Data for maximum '" + validatorData + "' is not a number", jsonSchemaPath);
    	}
	}
	
	@Override
	public void validate() throws JsonSchemaDefinitionError, JsonSchemaDataValidationError {
		if (!(jsonNode.isNumber())) {
			if (!jsonSchemaDependencyResolver.isUseDraftV4Mode()) {
				throw new JsonSchemaDataValidationError("Expected data type 'number' but was '" + jsonNode.getJsonDataType().getName() + "'", jsonPath);
			}
		} else {
			Number dataValue = ((Number) jsonNode.getValue()).doubleValue();
			Number maximumValue = ((Number) validatorData).doubleValue();
			
			if (NumberUtilities.compare(dataValue, maximumValue) > 0) {
				throw new JsonSchemaDataValidationError("Maximum number is '" + ((Number) validatorData) + "' but value was '" + ((Number) jsonNode.getValue())  + "'", jsonPath);
			}
			
			if (parentValidatorData.containsPropertyKey("exclusiveMaximum")) {
				Object exclusiveMaximumRaw = parentValidatorData.get("exclusiveMaximum");
				if (exclusiveMaximumRaw == null) {
					throw new JsonSchemaDefinitionError("Property 'exclusiveMaximum' is 'null'", jsonSchemaPath);
				} else if (exclusiveMaximumRaw instanceof Boolean) {
					if ((Boolean) exclusiveMaximumRaw) {
						if (NumberUtilities.compare(dataValue, maximumValue) == 0) {
							throw new JsonSchemaDataValidationError("Exclusive maximum number is '" + ((Number) validatorData) + "' but value was '" + ((Number) jsonNode.getValue())  + "'", jsonPath);
						}
					}
				} else {
					throw new JsonSchemaDefinitionError("ExclusiveMaximum data is not 'boolean'", jsonSchemaPath);
				}
			}
		}
    }
}
