/*
 * Copyright 2026 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.risingwave.connector.cdc.debezium.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.Properties;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Converts PostgreSQL array columns (e.g. _float4, _int4, _text[]) into plain strings.
 *
 * <p>Debezium's default handler for array columns uses an ARRAY schema. When an array column is
 * unchanged and TOAST'd in an UPDATE, Debezium sends a bare {@code java.lang.Object} sentinel that
 * cannot be placed into an ARRAY-typed schema slot, causing {@code Struct.put} to throw {@code
 * DataException: Invalid Java object for schema with type ARRAY: class java.lang.Object}.
 *
 * <p>This converter bypasses that by registering an OPTIONAL STRING schema for all array columns
 * ({@code jdbcType == Types.ARRAY}). Normal values are passed through as the Postgres text array
 * representation (e.g. {@code {1.5,2.3}}), and the TOAST sentinel is converted to the canonical
 * {@code __debezium_unavailable_value} string. The downstream RisingWave JSON parser handles both
 * forms, and the TOAST replacement logic in the materialize executor takes over from there.
 */
public class PgArrayToStringConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    // Must match `DEBEZIUM_UNAVAILABLE_VALUE` on the Rust side (src/common/src/types/mod.rs).
    // For ARRAY-schema columns, Debezium sends a bare `java.lang.Object` singleton as the TOAST
    // sentinel (instead of the usual placeholder string, which would fail ARRAY schema validation).
    // We translate that sentinel to the canonical string here.
    private static final String UNAVAILABLE_VALUE_PLACEHOLDER = "__debezium_unavailable_value";

    @Override
    public void configure(Properties props) {}

    @Override
    public void converterFor(
            RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        if (column.jdbcType() == Types.ARRAY) {
            registration.register(SchemaBuilder.string().optional(), this::convertArray);
        }
    }

    private String convertArray(Object value) {
        if (value == null) {
            return null;
        }
        // For ARRAY-schema columns, Debezium's TOAST sentinel is a bare `java.lang.Object`
        // singleton. Normal values arrive as concrete subclasses (String, byte[], etc.), so a
        // value whose runtime class is exactly Object can only be the sentinel.
        if (value.getClass() == Object.class) {
            return UNAVAILABLE_VALUE_PLACEHOLDER;
        }
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        if (value instanceof ByteBuffer buffer) {
            var readOnly = buffer.asReadOnlyBuffer();
            var bytes = new byte[readOnly.remaining()];
            readOnly.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return value.toString();
    }
}
