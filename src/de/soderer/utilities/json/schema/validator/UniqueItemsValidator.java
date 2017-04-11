package de.soderer.utilities.json.schema.validator;

import de.soderer.utilities.json.JsonArray;
import de.soderer.utilities.json.JsonNode;
import de.soderer.utilities.json.schema.JsonSchemaDataValidationError;
import de.soderer.utilities.json.schema.JsonSchemaDefinitionError;
import de.soderer.utilities.json.schema.JsonSchemaDependencyResolver;

public class UniqueItemsValidator extends BaseJsonSchemaValidator {
	public UniqueItemsValidator(JsonSchemaDependencyResolver jsonSchemaDependencyResolver, String jsonSchemaPath, Object validatorData, JsonNode jsonNode, String jsonPath) throws JsonSchemaDefinitionError {
		super(jsonSchemaDependencyResolver, jsonSchemaPath, validatorData, jsonNode, jsonPath);
		
		if (validatorData == null) {
			throw new JsonSchemaDefinitionError("Data for 'uniqueItems' items is 'null'", jsonSchemaPath);
		} else if (validatorData instanceof Boolean) {
			this.validatorData = validatorData;
    	} else if (validatorData instanceof String) {
    		try {
				this.validatorData = Boolean.parseBoolean((String) validatorData);
			} catch (NumberFormatException e) {
				throw new JsonSchemaDefinitionError("Data for 'uniqueItems' items is '" + validatorData + "' and not 'boolean'", jsonSchemaPath);
			}
    	} else {
    		throw new JsonSchemaDefinitionError("Data for 'uniqueItems' is not 'boolean'", jsonSchemaPath);
    	}
	}
	
	@Override
	public void validate() throws JsonSchemaDefinitionError, JsonSchemaDataValidationError {
		if (!(jsonNode.isJsonArray())) {
			if (!jsonSchemaDependencyResolver.isUseDraftV4Mode()) {
				throw new JsonSchemaDataValidationError("Expected data type 'array' but was '" + jsonNode.getJsonDataType().getName() + "'", jsonPath);
			}
		} else {
			if ((Boolean) validatorData) {
				JsonArray jsonArray = (JsonArray) jsonNode.getValue();
				for (int i = 0; i < jsonArray.size(); i++) {
					for (int j = i + 1; j < jsonArray.size(); j++) {
						if ((jsonArray.get(i) == jsonArray.get(j))
							|| (jsonArray.get(i) != null && jsonArray.get(i).equals(jsonArray.get(j)))) {
							throw new JsonSchemaDataValidationError("Item '" + jsonArray.get(i) + "' of array is not unique", jsonPath);
						}
					}
				}
			}
		}
    }
}
