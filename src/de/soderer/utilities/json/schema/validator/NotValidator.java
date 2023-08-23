package de.soderer.utilities.json.schema.validator;

import java.util.List;

import de.soderer.utilities.json.JsonNode;
import de.soderer.utilities.json.JsonObject;
import de.soderer.utilities.json.schema.JsonSchema;
import de.soderer.utilities.json.schema.JsonSchemaDataValidationError;
import de.soderer.utilities.json.schema.JsonSchemaDefinitionError;
import de.soderer.utilities.json.schema.JsonSchemaDependencyResolver;

public class NotValidator extends BaseJsonSchemaValidator {
	private List<BaseJsonSchemaValidator> subValidators = null;

	public NotValidator(final JsonSchemaDependencyResolver jsonSchemaDependencyResolver, final String jsonSchemaPath, final Object validatorData, final JsonNode jsonNode, final String jsonPath) throws JsonSchemaDefinitionError {
		super(jsonSchemaDependencyResolver, jsonSchemaPath, validatorData, jsonNode, jsonPath);

		if (validatorData == null) {
			throw new JsonSchemaDefinitionError("Not-validation data is 'null'", jsonSchemaPath);
		} else if (validatorData instanceof JsonObject) {
			subValidators = JsonSchema.createValidators((JsonObject) validatorData, jsonSchemaDependencyResolver, jsonSchemaPath, jsonNode, jsonPath);
			if (subValidators == null || subValidators.size() == 0) {
				throw new JsonSchemaDefinitionError("Not-validation JsonObject is empty", jsonSchemaPath);
			}
		} else {
			throw new JsonSchemaDefinitionError("Not-validation property does not have an JsonObject value", jsonSchemaPath);
		}
	}

	@Override
	public void validate() throws JsonSchemaDefinitionError, JsonSchemaDataValidationError {
		boolean didNotApply = false;
		try {
			for (final BaseJsonSchemaValidator subValidator : subValidators) {
				subValidator.validate();
			}
		} catch (@SuppressWarnings("unused") final JsonSchemaDataValidationError e) {
			didNotApply = true;
		}
		if (!didNotApply) {
			throw new JsonSchemaDataValidationError("The 'not' property did apply to JsonNode", jsonPath);
		}
	}
}
