/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netstream.avro.proxy.integration;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.springframework.cloud.stream.schema.avro.AvroSchemaRegistryClientMessageConverter;
import org.springframework.cloud.stream.schema.client.ConfluentSchemaRegistryClient;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.util.MimeType;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.regex.Pattern;

/**
 * A {@link org.springframework.messaging.converter.MessageConverter}
 * for Apache Avro messages with the ability to publish and retrieve schemas
 * stored in a Confluent Schema Registry server, allowing for schema evolution
 * in applications.
 * This converter can handle messages both serialized by Confluent's KafkaAvroSerializer and
 * by the default {@link org.apache.kafka.common.serialization.ByteArraySerializer} used by
 * Spring Cloud Stream Kafka.
 * The supported content types are in the form `application/*+avro`.
 *
 * During the conversion to a message, the converter will set the 'contentType'
 * header to 'application/[prefix].[subject].v[version]+avro', where:
 *
 * <li>
 * <ul><i>prefix</i> is a configurable prefix (default 'vnd');</ul>
 * <ul><i>subject</i> is a subject derived from the type of the outgoing object - typically the class name;</ul>
 * <ul><i>version</i> is the schema version for the given subject;</ul>
 * </li>
 *
 * When converting from a message, the converter will parse the content-type
 * and use it to fetch and cache the writer schema using the provided
 * {@link ConfluentSchemaRegistryClient}.
 * 
 * @author Julian Hanhart
 * @author Dominik Meister
 */
public class ConfluentSchemaRegistryClientMessageConverter extends AvroSchemaRegistryClientMessageConverter {

	private class MessageContent {

		private final byte[] payload;
		private final Integer schemaId;
		private Integer schemaVersion;

		private MessageContent(byte[] payload, Integer schemaId, Integer schemaVersion) {
			this.payload = payload;
			this.schemaId = schemaId;
			this.schemaVersion = schemaVersion;
		}

		private MessageContent(byte[] payload, Integer schemaId) {
			this(payload, schemaId, null);
		}

		private MessageContent(byte[] payload) {
			this(payload, null, null);
		}
	}

	private static final Pattern versionWildcardPattern = Pattern.compile("vnd\\.\\w+\\.\\*\\+avro");
	private static final String SCHEMA_PROP_VERSION = "version";
	private static final String SCHEMA_PROP_MIME_TYPE = "mimeType";

	private final ConfluentSchemaRegistryClient schemaRegistryClient;

	/**
	 * Creates a new instance, configuring it with a {@link ConfluentSchemaRegistryClient}.
	 *
	 * @param schemaRegistryClient the {@link ConfluentSchemaRegistryClient} used to interact with the schema registry server.
	 */
	public ConfluentSchemaRegistryClientMessageConverter(ConfluentSchemaRegistryClient schemaRegistryClient) {
		super(schemaRegistryClient);
		this.schemaRegistryClient = schemaRegistryClient;
	}

	/**
	 * Checks whether the given {@link MimeType} specifies a  {@link Schema} version or not
	 *
	 * @param mimeType The {@link MimeType} type to check
	 * @return Whether the given {@link MimeType} describes a unspecific or a versioned  {@link Schema}
	 */
	public static boolean isWildcardMimeType(MimeType mimeType) {
		return versionWildcardPattern.matcher(mimeType.getSubtype()).matches();
	}

	/**
	 * Extracts a {@link Schema}'s version from the "version" property.
	 *
	 * @param schema The {@link Schema} to check
	 * @return The {@Schema} version (if present in the properties)
	 */
	public static Integer getSchemaVersion(Schema schema) {
		Integer version = null;
		if ((schema != null)
				&& (schema.getObjectProp(SCHEMA_PROP_VERSION) != null)
				&& (schema.getObjectProp(SCHEMA_PROP_VERSION) instanceof Integer)) {
			version = (Integer) schema.getObjectProp(SCHEMA_PROP_VERSION);
		}
		return version;
	}

	/**
	 * Extracts a {@link Schema}'s {@link MimeType} from the "mimeType" property.
	 *
	 * @param schema The {@link Schema} to check
	 * @return The {@Schema} {@link MimeType} (if present in the properties)
	 */
	public static MimeType getSchemaMimeType(Schema schema) {
		final String mimeType = schema.getProp(SCHEMA_PROP_MIME_TYPE);
		return !StringUtils.isEmpty(mimeType) ? MimeType.valueOf(mimeType) : null;
	}

	@Override
	protected boolean supportsMimeType(MessageHeaders headers) {
		if (super.supportsMimeType(headers)) {
			return true;
		}
		MimeType mimeType = getContentTypeResolver().resolve(headers);
		Schema schema = resolveWriterSchemaForDeserialization(mimeType);
		return versionWildcardPattern.matcher(mimeType.getSubtype()).matches() || (schema != null);
	}

	@Override
	protected Object convertFromInternal(Message<?> message, Class<?> targetClass, Object conversionHint) {
		Object result = null;
		try {
			MimeType mimeType = getContentTypeResolver().resolve(message.getHeaders());
			if (mimeType == null) {
				if (conversionHint instanceof MimeType) {
					mimeType = (MimeType) conversionHint;
				} else {
					return null;
				}
			}

			final MessageContent content = removeConfluentHeaderIfPresent(
					(byte[]) message.getPayload(), mimeType);
			final byte[] payload = content.payload;

			Schema messageSchema;
			if (content.schemaId != null) {
				Schema.Parser schemaParser = new Schema.Parser();
				messageSchema = schemaParser.parse(
						schemaRegistryClient.fetch(content.schemaId));
			} else {
				messageSchema = resolveSchemaForWriting(targetClass, message.getHeaders(), mimeType);
			}
			content.schemaVersion = getSchemaVersion(messageSchema);

			GenericContainer target = null;
			Schema expectedSchema = messageSchema;
			if (GenericContainer.class.isAssignableFrom(targetClass)) {
				target = (GenericContainer) targetClass.newInstance();
				expectedSchema = target.getSchema();
			}
			final MimeType targetMimeType = getSchemaMimeType(expectedSchema);

			final DatumReader<Object> reader = getDatumReader((Class<Object>) targetClass, expectedSchema, messageSchema);
			final Decoder decoder = DecoderFactory.get().binaryDecoder(payload, null);
			result = reader.read((Object) target, decoder);
		} catch (IllegalAccessException | InstantiationException | IOException e) {
			throw new MessageConversionException(message, "Failed to read payload", e);
		}
		return result;
	}

	/**
	 * Removes the Schema Id header Confluent's Kafka REST Proxy adds before the actual payload.
	 * {@see https://groups.google.com/forum/#!topic/confluent-platform/rI1WNPp8DJU}
	 *
	 * A Message is considered to be of the Confluent format, if no version is specified in the
	 * content type and therefore has a wildcard MIME type (i.e. 'application/vnd.<topic>.*+avro').
	 *
	 * @param payload The binary, Avro encoded {@link Message} payload
	 * @param mimeType The {@link Message}'s {@link MimeType}
	 * @return The actual {@link Message} payload
	 */
	private MessageContent removeConfluentHeaderIfPresent(byte[] payload, MimeType mimeType) {
		if (isWildcardMimeType(mimeType)) {
			// read first 4 bytes and copy the rest to the new byte array
			final byte[] actualPayload = new byte[payload.length - (Integer.SIZE / Byte.SIZE)];
			final ByteBuffer buffer = ByteBuffer.wrap(payload);
			final int schemaId = buffer.getInt();
			buffer.get(actualPayload);
			return (new MessageContent(actualPayload, schemaId));
		}
		else {
			return (new MessageContent(payload));
		}
	}
}
