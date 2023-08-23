package de.soderer.utilities.json.schema.validator;

import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.NetworkUtilities;
import de.soderer.utilities.TextUtilities;
import de.soderer.utilities.json.JsonNode;
import de.soderer.utilities.json.schema.JsonSchemaDataValidationError;
import de.soderer.utilities.json.schema.JsonSchemaDefinitionError;
import de.soderer.utilities.json.schema.JsonSchemaDependencyResolver;

public class FormatValidator extends BaseJsonSchemaValidator {
	public FormatValidator(final JsonSchemaDependencyResolver jsonSchemaDependencyResolver, final String jsonSchemaPath, final Object validatorData, final JsonNode jsonNode, final String jsonPath) throws JsonSchemaDefinitionError {
		super(jsonSchemaDependencyResolver, jsonSchemaPath, validatorData, jsonNode, jsonPath);

		if (validatorData == null) {
			throw new JsonSchemaDefinitionError("Format value is 'null'", jsonSchemaPath);
		} else if (!(validatorData instanceof String)) {
			throw new JsonSchemaDefinitionError("Format value is not a string", jsonSchemaPath);
		}
	}

	@Override
	public void validate() throws JsonSchemaDefinitionError, JsonSchemaDataValidationError {
		if (!jsonNode.isString()) {
			throw new JsonSchemaDataValidationError("Expected a 'string' value for formatcheck but was '" + jsonNode.getJsonDataType().getName() + "'", jsonPath);
		} else if ("email".equalsIgnoreCase((String) validatorData)) {
			if (!NetworkUtilities.isValidEmail((String) jsonNode.getValue())) {
				throw new JsonSchemaDataValidationError("Invalid data for format '" + ((String) validatorData) + "' was '" + jsonNode.getValue() + "'", jsonPath);
			}
		} else if ("date-time".equalsIgnoreCase((String) validatorData)) {
			try {
				DateUtilities.parseLocalDateTime(DateUtilities.ISO_8601_DATETIME_FORMAT, (String) jsonNode.getValue());
			} catch (final DateTimeParseException e) {
				throw new JsonSchemaDataValidationError("Invalid data for format '" + ((String) validatorData) + "' was '" + jsonNode.getValue() + "'", jsonPath, e);
			}
		} else if ("hostname".equalsIgnoreCase((String) validatorData)) {
			if (!NetworkUtilities.isValidHostname((String) jsonNode.getValue())) {
				throw new JsonSchemaDataValidationError("Invalid data for format '" + ((String) validatorData) + "' was '" + jsonNode.getValue() + "'", jsonPath);
			}
		} else if ("ipv4".equalsIgnoreCase((String) validatorData)) {
			if (!NetworkUtilities.isValidIpV4((String) jsonNode.getValue())) {
				throw new JsonSchemaDataValidationError("Invalid data for format '" + ((String) validatorData) + "' was '" + jsonNode.getValue() + "'", jsonPath);
			}
		} else if ("ipv6".equalsIgnoreCase((String) validatorData)) {
			if (!NetworkUtilities.isValidIpV6((String) jsonNode.getValue())) {
				throw new JsonSchemaDataValidationError("Invalid data for format '" + ((String) validatorData) + "' was '" + jsonNode.getValue() + "'", jsonPath);
			}
		} else if ("uri".equalsIgnoreCase((String) validatorData)) {
			if (!NetworkUtilities.isValidUri((String) jsonNode.getValue())) {
				throw new JsonSchemaDataValidationError("Invalid data for format '" + ((String) validatorData) + "' was '" + jsonNode.getValue() + "'", jsonPath);
			}
		} else if ("regex".equalsIgnoreCase((String) validatorData)) {
			try {
				Pattern.compile((String) jsonNode.getValue());
			} catch (final Exception e) {
				throw new JsonSchemaDataValidationError("Invalid data for format '" + ((String) validatorData) + "' was '" + jsonNode.getValue() + "'", jsonPath, e);
			}
		} else if ("base64".equalsIgnoreCase((String) validatorData)) {
			if (!TextUtilities.isValidBase64((String) jsonNode.getValue())) {
				throw new JsonSchemaDataValidationError("Invalid data for format '" + ((String) validatorData) + "' was '" + TextUtilities.trimStringToMaximumLength((String) jsonNode.getValue(), 20, " ...") + "'", jsonPath);
			}
		} else {
			throw new JsonSchemaDefinitionError("Unknown format name '" + validatorData + "'", jsonSchemaPath);
		}
	}
}
