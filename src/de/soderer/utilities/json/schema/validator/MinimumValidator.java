package de.soderer.utilities.json.schema.validator;

import de.soderer.utilities.NumberUtilities;
import de.soderer.utilities.json.JsonNode;
import de.soderer.utilities.json.JsonObject;
import de.soderer.utilities.json.schema.JsonSchemaDataValidationError;
import de.soderer.utilities.json.schema.JsonSchemaDefinitionError;
import de.soderer.utilities.json.schema.JsonSchemaDependencyResolver;

public class MinimumValidator extends ExtendedBaseJsonSchemaValidator {
	public MinimumValidator(JsonObject parentValidatorData, JsonSchemaDependencyResolver jsonSchemaDependencyResolver, String jsonSchemaPath, Object validatorData, JsonNode jsonNode, String jsonPath) throws JsonSchemaDefinitionError {
		super(parentValidatorData, jsonSchemaDependencyResolver, jsonSchemaPath, validatorData, jsonNode, jsonPath);

		if (validatorData == null) {
			throw new JsonSchemaDefinitionError("Data for minimum is null", jsonSchemaPath);
    	} else if (validatorData instanceof String) {
    		try {
    			this.validatorData = NumberUtilities.parseNumber((String) validatorData);
			} catch (NumberFormatException e) {
				throw new JsonSchemaDefinitionError("Data for minimum '" + validatorData + "' is not a number", jsonSchemaPath);
			}
    	} else if (!(validatorData instanceof Number)) {
			throw new JsonSchemaDefinitionError("Data for minimum '" + validatorData + "' is not a number", jsonSchemaPath);
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
			Number minimumValue = ((Number) validatorData).doubleValue();
			
			if (NumberUtilities.compare(dataValue, minimumValue) < 0) {
				throw new JsonSchemaDataValidationError("Minimum number is '" + ((Number) validatorData) + "' but value was '" + ((Number) jsonNode.getValue())  + "'", jsonPath);
			}
			
			if (parentValidatorData.containsPropertyKey("exclusiveMinimum")) {
				Object exclusiveMinimumRaw = parentValidatorData.get("exclusiveMinimum");
				if (exclusiveMinimumRaw == null) {
					throw new JsonSchemaDefinitionError("Property 'exclusiveMinimum' is 'null'", jsonSchemaPath);
				} else if (exclusiveMinimumRaw instanceof Boolean) {
					if ((Boolean) exclusiveMinimumRaw) {
						if (NumberUtilities.compare(dataValue, minimumValue) == 0) {
							throw new JsonSchemaDataValidationError("Exclusive minimum number is '" + ((Number) validatorData) + "' but value was '" + ((Number) jsonNode.getValue())  + "'", jsonPath);
						}
					}
				} else {
					throw new JsonSchemaDefinitionError("ExclusiveMinimum data is not 'boolean'", jsonSchemaPath);
				}
			}
		}
    }
}
