// Copyright 2022 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::sync::Arc;

use clap::ValueEnum;
use percent_encoding::percent_decode_str;
use risingwave_common::bail;
use risingwave_common::catalog::{
    DEFAULT_SCHEMA_NAME, RISINGWAVE_ICEBERG_ROW_ID, ROW_ID_COLUMN_NAME,
};
use risingwave_common::config::MetaBackend;
use risingwave_common::session_config::sink_decouple::SinkDecouple;
use risingwave_connector::sink::decouple_checkpoint_log_sink::COMMIT_CHECKPOINT_INTERVAL;
use risingwave_connector::sink::iceberg::{
    COMMIT_CHECKPOINT_SIZE_THRESHOLD_MB,
    COMPACTION_DELETE_EQUALITY_RECORDS_COUNT_THRESHOLD, COMPACTION_DELETE_FILES_COUNT_THRESHOLD,
    COMPACTION_DELETE_POSITION_RECORDS_COUNT_THRESHOLD, COMPACTION_INTERVAL_SEC,
    COMPACTION_MAX_SNAPSHOTS_NUM, COMPACTION_SMALL_FILES_THRESHOLD_MB,
    COMPACTION_TARGET_FILE_SIZE_MB, COMPACTION_TRIGGER_SNAPSHOT_COUNT, COMPACTION_TYPE,
    COMPACTION_WRITE_PARQUET_COMPRESSION, COMPACTION_WRITE_PARQUET_MAX_ROW_GROUP_BYTES,
    COMPACTION_WRITE_PARQUET_MAX_ROW_GROUP_ROWS, CompactionType, ENABLE_COMPACTION, ENABLE_PK_INDEX,
    ENABLE_SNAPSHOT_EXPIRATION, FORMAT_VERSION, ICEBERG_WRITE_MODE_COPY_ON_WRITE,
    ICEBERG_WRITE_MODE_MERGE_ON_READ, IcebergWriteMode, ORDER_KEY,
    SNAPSHOT_EXPIRATION_CLEAR_EXPIRED_FILES, SNAPSHOT_EXPIRATION_CLEAR_EXPIRED_META_DATA,
    SNAPSHOT_EXPIRATION_MAX_AGE_MILLIS, SNAPSHOT_EXPIRATION_RETAIN_LAST,
    SNAPSHOT_EXPIRATION_RETAIN_MAX, WRITE_MODE, parse_partition_by_exprs, validate_order_key_columns,
};
use risingwave_connector::AUTO_SCHEMA_CHANGE_KEY;
use risingwave_pb::catalog::connection::Info as ConnectionInfo;
use risingwave_pb::catalog::connection_params::ConnectionType;
use risingwave_pb::catalog::PbSource;
use risingwave_sqlparser::ast::{ConnectionRefValue, Ident, ObjectName};

use crate::error::{ErrorCode, Result, RwError};
use crate::handler::HandlerArgs;
use crate::session::SessionImpl;
use thiserror_ext::AsReport;

use crate::{Binder, TableCatalog};

/// Result of resolving an iceberg engine connection.
pub(crate) struct ResolvedIcebergConnection {
    pub with_common: BTreeMap<String, String>,
    pub connection_ref: BTreeMap<String, ConnectionRefValue>,
}

/// Resolves the iceberg engine connection from WITH clause or session variable,
/// building the common `with_common` properties and `connection_ref` map.
pub(crate) async fn resolve_iceberg_connection(
    session: &Arc<SessionImpl>,
    handler_args: &HandlerArgs,
    table: &TableCatalog,
    table_name: &ObjectName,
    entity_kind: &str,
) -> Result<ResolvedIcebergConnection> {
    let rw_db_name = session
        .env()
        .catalog_reader()
        .read_guard()
        .get_database_by_id(table.database_id)?
        .name()
        .to_owned();
    let rw_schema_name = session
        .env()
        .catalog_reader()
        .read_guard()
        .get_schema_by_id(table.database_id, table.schema_id)?
        .name()
        .clone();
    let iceberg_catalog_name = rw_db_name.clone();
    let iceberg_database_name = rw_schema_name.clone();
    let iceberg_table_name = table_name.0.last().unwrap().real_value();

    // Resolve iceberg engine connection: WITH clause takes precedence over session variable.
    let iceberg_engine_connection: String = if let Some(conn_ref) =
        handler_args.with_options.connection_ref().get("connection")
    {
        let (schema_name, connection_name) = Binder::resolve_schema_qualified_name(
            &session.database(),
            &conn_ref.connection_name,
        )?;
        format!(
            "{}.{}",
            schema_name.unwrap_or_else(|| DEFAULT_SCHEMA_NAME.to_owned()),
            connection_name
        )
    } else {
        session.config().iceberg_engine_connection()
    };

    let sink_decouple = session.config().sink_decouple();
    if matches!(sink_decouple, SinkDecouple::Disable) {
        bail!(
            "Iceberg engine {entity_kind} only supports with sink decouple, try `set sink_decouple = true` to resolve it"
        );
    }

    let mut connection_ref = BTreeMap::new();
    let with_common = if iceberg_engine_connection.is_empty() {
        bail!("to use iceberg engine {entity_kind}, please either set the session variable `iceberg_engine_connection` or specify `connection` in the WITH clause.");
    } else {
        let parts: Vec<&str> = iceberg_engine_connection.split('.').collect();
        if parts.len() != 2 {
            bail!(
                "iceberg_engine_connection must be in 'schema.connection_name' format, got: {}",
                iceberg_engine_connection
            );
        }
        let connection_catalog =
            session.get_connection_by_name(Some(parts[0].to_owned()), parts[1])?;
        if let ConnectionInfo::ConnectionParams(params) = &connection_catalog.info {
            if params.connection_type == ConnectionType::Iceberg as i32 {
                // With iceberg engine connection:
                connection_ref.insert(
                    "connection".to_owned(),
                    ConnectionRefValue {
                        connection_name: ObjectName::from(vec![
                            Ident::from(parts[0]),
                            Ident::from(parts[1]),
                        ]),
                    },
                );

                let mut with_common = BTreeMap::new();
                with_common.insert("connector".to_owned(), "iceberg".to_owned());
                with_common.insert("database.name".to_owned(), iceberg_database_name.clone());
                with_common.insert("table.name".to_owned(), iceberg_table_name.clone());

                let hosted_catalog = params
                    .properties
                    .get("hosted_catalog")
                    .map(|s| s.eq_ignore_ascii_case("true"))
                    .unwrap_or(false);
                if hosted_catalog {
                    let meta_client = session.env().meta_client();
                    let meta_store_endpoint = meta_client.get_meta_store_endpoint().await?;

                    let meta_store_endpoint =
                        url::Url::parse(&meta_store_endpoint).map_err(|_| {
                            ErrorCode::InternalError(
                                "failed to parse the meta store endpoint".to_owned(),
                            )
                        })?;
                    let meta_store_backend = meta_store_endpoint.scheme().to_owned();
                    let meta_store_user = meta_store_endpoint.username().to_owned();
                    let meta_store_password = match meta_store_endpoint.password() {
                        Some(password) => percent_decode_str(password)
                            .decode_utf8()
                            .map_err(|_| {
                                ErrorCode::InternalError(
                                    "failed to parse password from meta store endpoint"
                                        .to_owned(),
                                )
                            })?
                            .into_owned(),
                        None => "".to_owned(),
                    };
                    let meta_store_host = meta_store_endpoint.host_str().ok_or_else(|| {
                        ErrorCode::InternalError(
                            "failed to parse host from meta store endpoint".to_owned(),
                        )
                    })?;
                    let meta_store_port = meta_store_endpoint.port().ok_or_else(|| {
                        ErrorCode::InternalError(
                            "failed to parse port from meta store endpoint".to_owned(),
                        )
                    })?;
                    let meta_store_database = meta_store_endpoint
                        .path()
                        .trim_start_matches('/')
                        .to_owned();

                    let Ok(meta_backend) = MetaBackend::from_str(&meta_store_backend, true) else {
                        bail!("failed to parse meta backend: {}", meta_store_backend);
                    };

                    let catalog_uri = match meta_backend {
                        MetaBackend::Postgres => {
                            format!(
                                "jdbc:postgresql://{}:{}/{}",
                                meta_store_host, meta_store_port, meta_store_database
                            )
                        }
                        MetaBackend::Mysql => {
                            format!(
                                "jdbc:mysql://{}:{}/{}",
                                meta_store_host, meta_store_port, meta_store_database
                            )
                        }
                        MetaBackend::Sqlite | MetaBackend::Sql | MetaBackend::Mem => {
                            bail!(
                                "Unsupported meta backend for iceberg engine {entity_kind}: {}",
                                meta_store_backend
                            );
                        }
                    };

                    with_common.insert("catalog.type".to_owned(), "jdbc".to_owned());
                    with_common.insert("catalog.uri".to_owned(), catalog_uri);
                    with_common.insert("catalog.jdbc.user".to_owned(), meta_store_user);
                    with_common
                        .insert("catalog.jdbc.password".to_owned(), meta_store_password);
                    with_common.insert("catalog.name".to_owned(), iceberg_catalog_name);
                }

                with_common
            } else {
                return Err(RwError::from(ErrorCode::InvalidParameterValue(
                    "Only iceberg connection could be used in iceberg engine".to_owned(),
                )));
            }
        } else {
            return Err(RwError::from(ErrorCode::InvalidParameterValue(
                "Private Link Service has been deprecated. Please create a new connection instead."
                    .to_owned(),
            )));
        }
    };

    Ok(ResolvedIcebergConnection {
        with_common,
        connection_ref,
    })
}

/// Context for building iceberg sink WITH options.
pub(crate) struct BuildSinkOptionsCtx<'a> {
    pub handler_args: &'a HandlerArgs,
    pub table: &'a TableCatalog,
    pub pks: &'a [String],
    /// For the MV path this is always None; for the table path it is Some and we remove
    /// WITH keys from source.with_properties after reading them.
    pub source: Option<&'a mut PbSource>,
    pub is_mv: bool,
    pub entity_kind: &'static str,
    /// Whether sink decouple is enabled in session config.
    pub sink_decouple_enabled: bool,
}

impl<'a> BuildSinkOptionsCtx<'a> {
    fn source_remove(&mut self, key: &str) {
        if let Some(source) = self.source.as_mut() {
            source.with_properties.remove(key);
        }
    }
}

/// Builds the sink WITH options map for iceberg engine tables/materialized views.
pub(crate) fn build_iceberg_sink_with_options(
    mut ctx: BuildSinkOptionsCtx<'_>,
) -> Result<BTreeMap<String, String>> {
    let mut sink_with = BTreeMap::new();
    let handler_args = ctx.handler_args;
    let table = ctx.table;

    // 1. enable_pk_index — DIVERGENT
    let enable_pk_index = if ctx.is_mv {
        sink_with.insert(ENABLE_PK_INDEX.to_owned(), "true".to_owned());
        true
    } else {
        handler_args
            .with_options
            .get(ENABLE_PK_INDEX)
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    };
    if !enable_pk_index {
        sink_with.insert(AUTO_SCHEMA_CHANGE_KEY.to_owned(), "true".to_owned());
    }

    if table.append_only {
        sink_with.insert("type".to_owned(), "append-only".to_owned());
    } else {
        sink_with.insert("type".to_owned(), "upsert".to_owned());
        if !enable_pk_index {
            sink_with.insert("primary_key".to_owned(), ctx.pks.join(","));
        }
    }

    // 3. commit_checkpoint_interval
    let commit_checkpoint_interval = handler_args
        .with_options
        .get(COMMIT_CHECKPOINT_INTERVAL)
        .map(|v| v.to_owned())
        .unwrap_or_else(|| "60".to_owned());
    let commit_checkpoint_interval = commit_checkpoint_interval.parse::<u32>().map_err(|_| {
        ErrorCode::InvalidInputSyntax(format!(
            "commit_checkpoint_interval must be a non-negative integer: {}",
            commit_checkpoint_interval
        ))
    })?;

    if commit_checkpoint_interval == 0 {
        bail!("commit_checkpoint_interval must be greater than 0");
    }

    // C4: sink_decouple conflict check
    if !ctx.sink_decouple_enabled && commit_checkpoint_interval > 1 {
        bail!(
            "config conflict: `commit_checkpoint_interval` larger than 1 means that sink decouple must be enabled, but session config sink_decouple is disabled"
        )
    }

    sink_with.insert(
        COMMIT_CHECKPOINT_INTERVAL.to_owned(),
        commit_checkpoint_interval.to_string(),
    );
    ctx.source_remove(COMMIT_CHECKPOINT_INTERVAL);

    // 4. commit_checkpoint_size_threshold_mb
    if let Some(commit_checkpoint_size_threshold_mb) = handler_args
        .with_options
        .get(COMMIT_CHECKPOINT_SIZE_THRESHOLD_MB)
    {
        let threshold_mb: u64 = commit_checkpoint_size_threshold_mb.parse().map_err(|_| {
            ErrorCode::InvalidInputSyntax(format!(
                "{} must be a positive integer: {}",
                COMMIT_CHECKPOINT_SIZE_THRESHOLD_MB, commit_checkpoint_size_threshold_mb
            ))
        })?;
        // Setting to 0 disables size-based early commits.
        sink_with.insert(
            COMMIT_CHECKPOINT_SIZE_THRESHOLD_MB.to_owned(),
            threshold_mb.to_string(),
        );
        ctx.source_remove(COMMIT_CHECKPOINT_SIZE_THRESHOLD_MB);
    }

    // 5. create_table_if_not_exists = true
    sink_with.insert("create_table_if_not_exists".to_owned(), "true".to_owned());

    // 6. is_exactly_once = true
    sink_with.insert("is_exactly_once".to_owned(), "true".to_owned());

    // 7. enable_compaction (with default true if absent)
    if let Some(enable_compaction) = handler_args.with_options.get(ENABLE_COMPACTION) {
        match enable_compaction.to_lowercase().as_str() {
            "true" => {
                sink_with.insert(ENABLE_COMPACTION.to_owned(), "true".to_owned());
            }
            "false" => {
                sink_with.insert(ENABLE_COMPACTION.to_owned(), "false".to_owned());
            }
            _ => {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "enable_compaction must be true or false: {}",
                    enable_compaction
                ))
                .into());
            }
        }
        ctx.source_remove(ENABLE_COMPACTION);
    } else {
        sink_with.insert(ENABLE_COMPACTION.to_owned(), "true".to_owned());
    }

    // 8. compaction_interval_sec
    if let Some(compaction_interval_sec) = handler_args.with_options.get(COMPACTION_INTERVAL_SEC) {
        let compaction_interval_sec = compaction_interval_sec.parse::<u64>().map_err(|_| {
            ErrorCode::InvalidInputSyntax(format!(
                "compaction_interval_sec must be greater than 0: {}",
                compaction_interval_sec
            ))
        })?;
        if compaction_interval_sec == 0 {
            bail!("compaction_interval_sec must be greater than 0");
        }
        sink_with.insert(
            "compaction_interval_sec".to_owned(),
            compaction_interval_sec.to_string(),
        );
        ctx.source_remove(COMPACTION_INTERVAL_SEC);
    }

    // 9. snapshot expiration (6 options)
    let has_enabled_snapshot_expiration = if let Some(enable_snapshot_expiration) =
        handler_args.with_options.get(ENABLE_SNAPSHOT_EXPIRATION)
    {
        ctx.source_remove(ENABLE_SNAPSHOT_EXPIRATION);
        match enable_snapshot_expiration.to_lowercase().as_str() {
            "true" => {
                sink_with.insert(ENABLE_SNAPSHOT_EXPIRATION.to_owned(), "true".to_owned());
                true
            }
            "false" => {
                sink_with.insert(ENABLE_SNAPSHOT_EXPIRATION.to_owned(), "false".to_owned());
                false
            }
            _ => {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "enable_snapshot_expiration must be true or false: {}",
                    enable_snapshot_expiration
                ))
                .into());
            }
        }
    } else {
        sink_with.insert(ENABLE_SNAPSHOT_EXPIRATION.to_owned(), "true".to_owned());
        true
    };

    if has_enabled_snapshot_expiration {
        if let Some(snapshot_expiration_retain_last) = handler_args
            .with_options
            .get(SNAPSHOT_EXPIRATION_RETAIN_LAST)
        {
            sink_with.insert(
                SNAPSHOT_EXPIRATION_RETAIN_LAST.to_owned(),
                snapshot_expiration_retain_last.to_owned(),
            );
            ctx.source_remove(SNAPSHOT_EXPIRATION_RETAIN_LAST);
        }

        if let Some(snapshot_expiration_retain_max) = handler_args
            .with_options
            .get(SNAPSHOT_EXPIRATION_RETAIN_MAX)
        {
            sink_with.insert(
                SNAPSHOT_EXPIRATION_RETAIN_MAX.to_owned(),
                snapshot_expiration_retain_max.to_owned(),
            );
            ctx.source_remove(SNAPSHOT_EXPIRATION_RETAIN_MAX);
        }

        if let Some(snapshot_expiration_max_age) = handler_args
            .with_options
            .get(SNAPSHOT_EXPIRATION_MAX_AGE_MILLIS)
        {
            sink_with.insert(
                SNAPSHOT_EXPIRATION_MAX_AGE_MILLIS.to_owned(),
                snapshot_expiration_max_age.to_owned(),
            );
            ctx.source_remove(SNAPSHOT_EXPIRATION_MAX_AGE_MILLIS);
        }

        if let Some(snapshot_expiration_clear_expired_files) = handler_args
            .with_options
            .get(SNAPSHOT_EXPIRATION_CLEAR_EXPIRED_FILES)
        {
            sink_with.insert(
                SNAPSHOT_EXPIRATION_CLEAR_EXPIRED_FILES.to_owned(),
                snapshot_expiration_clear_expired_files.to_owned(),
            );
            ctx.source_remove(SNAPSHOT_EXPIRATION_CLEAR_EXPIRED_FILES);
        }

        if let Some(snapshot_expiration_clear_expired_meta_data) = handler_args
            .with_options
            .get(SNAPSHOT_EXPIRATION_CLEAR_EXPIRED_META_DATA)
        {
            sink_with.insert(
                SNAPSHOT_EXPIRATION_CLEAR_EXPIRED_META_DATA.to_owned(),
                snapshot_expiration_clear_expired_meta_data.to_owned(),
            );
            ctx.source_remove(SNAPSHOT_EXPIRATION_CLEAR_EXPIRED_META_DATA);
        }
    } else {
        // Snapshots disabled: strip any snapshot-expiration keys from source
        // so they are not forwarded to the iceberg source as unknown fields.
        ctx.source_remove(SNAPSHOT_EXPIRATION_RETAIN_LAST);
        ctx.source_remove(SNAPSHOT_EXPIRATION_RETAIN_MAX);
        ctx.source_remove(SNAPSHOT_EXPIRATION_MAX_AGE_MILLIS);
        ctx.source_remove(SNAPSHOT_EXPIRATION_CLEAR_EXPIRED_FILES);
        ctx.source_remove(SNAPSHOT_EXPIRATION_CLEAR_EXPIRED_META_DATA);
    }

    // 10. format_version
    if let Some(format_version) = handler_args.with_options.get(FORMAT_VERSION) {
        let format_version = format_version.parse::<u8>().map_err(|_| {
            ErrorCode::InvalidInputSyntax(format!(
                "format_version must be 1, 2 or 3: {}",
                format_version
            ))
        })?;
        if format_version != 1 && format_version != 2 && format_version != 3 {
            bail!("format_version must be 1, 2 or 3");
        }
        sink_with.insert(FORMAT_VERSION.to_owned(), format_version.to_string());
        ctx.source_remove(FORMAT_VERSION);
    }

    // 11. write_mode (with entity_kind-specific CopyOnWrite append-only error)
    if let Some(write_mode) = handler_args.with_options.get(WRITE_MODE) {
        let write_mode = IcebergWriteMode::try_from(write_mode.as_str()).map_err(|_| {
            ErrorCode::InvalidInputSyntax(format!(
                "invalid write_mode: {}, must be one of: {}, {}",
                write_mode, ICEBERG_WRITE_MODE_MERGE_ON_READ, ICEBERG_WRITE_MODE_COPY_ON_WRITE
            ))
        })?;

        match write_mode {
            IcebergWriteMode::MergeOnRead => {
                sink_with.insert(WRITE_MODE.to_owned(), write_mode.as_str().to_owned());
            }
            IcebergWriteMode::CopyOnWrite => {
                if table.append_only {
                    return Err(ErrorCode::NotSupported(
                        format!(
                            "COPY ON WRITE is not supported for append-only iceberg {}",
                            ctx.entity_kind
                        ),
                        "Please use MERGE ON READ instead".to_owned(),
                    )
                    .into());
                }
                sink_with.insert(WRITE_MODE.to_owned(), write_mode.as_str().to_owned());
            }
        }
        ctx.source_remove(WRITE_MODE);
    } else {
        sink_with.insert(
            WRITE_MODE.to_owned(),
            ICEBERG_WRITE_MODE_MERGE_ON_READ.to_owned(),
        );
    }

    // 12. enable_pk_index user-override validation
    if let Some(enable_pk_index_val) = handler_args.with_options.get(ENABLE_PK_INDEX) {
        if ctx.is_mv {
            if enable_pk_index_val.eq_ignore_ascii_case("true") {
                // Already forced true above; silently accept the no-op and strip from source.
                ctx.source_remove(ENABLE_PK_INDEX);
            } else {
                return Err(ErrorCode::InvalidParameterValue(format!(
                    "enable_pk_index cannot be set to '{}' for iceberg engine materialized view (forced to true)",
                    enable_pk_index_val
                ))
                .into());
            }
        } else {
            // For table: insert user's value (true/false) or error on invalid
            match enable_pk_index_val.to_lowercase().as_str() {
                "true" => {
                    sink_with.insert(ENABLE_PK_INDEX.to_owned(), "true".to_owned());
                }
                "false" => {
                    sink_with.insert(ENABLE_PK_INDEX.to_owned(), "false".to_owned());
                }
                _ => {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "enable_pk_index must be true or false: {}",
                        enable_pk_index_val
                    ))
                    .into());
                }
            }
            ctx.source_remove(ENABLE_PK_INDEX);
        }
    }

    // 13. compaction_max_snapshots_num
    if let Some(max_snapshots_num_before_compaction) =
        handler_args.with_options.get(COMPACTION_MAX_SNAPSHOTS_NUM)
    {
        let max_snapshots_num_before_compaction = max_snapshots_num_before_compaction
            .parse::<u32>()
            .map_err(|_| {
                ErrorCode::InvalidInputSyntax(format!(
                    "{} must be greater than 0: {}",
                    COMPACTION_MAX_SNAPSHOTS_NUM, max_snapshots_num_before_compaction
                ))
            })?;

        if max_snapshots_num_before_compaction == 0 {
            bail!(format!(
                "{} must be greater than 0",
                COMPACTION_MAX_SNAPSHOTS_NUM
            ));
        }

        sink_with.insert(
            COMPACTION_MAX_SNAPSHOTS_NUM.to_owned(),
            max_snapshots_num_before_compaction.to_string(),
        );
        ctx.source_remove(COMPACTION_MAX_SNAPSHOTS_NUM);
    }

    // 14. compaction.small_files_threshold_mb
    if let Some(small_files_threshold_mb) = handler_args
        .with_options
        .get(COMPACTION_SMALL_FILES_THRESHOLD_MB)
    {
        let small_files_threshold_mb = small_files_threshold_mb.parse::<u64>().map_err(|_| {
            ErrorCode::InvalidInputSyntax(format!(
                "{} must be greater than 0: {}",
                COMPACTION_SMALL_FILES_THRESHOLD_MB, small_files_threshold_mb
            ))
        })?;
        if small_files_threshold_mb == 0 {
            bail!(format!(
                "{} must be a greater than 0",
                COMPACTION_SMALL_FILES_THRESHOLD_MB
            ));
        }
        sink_with.insert(
            COMPACTION_SMALL_FILES_THRESHOLD_MB.to_owned(),
            small_files_threshold_mb.to_string(),
        );
        ctx.source_remove(COMPACTION_SMALL_FILES_THRESHOLD_MB);
    }

    // 15. compaction.delete_files_count_threshold
    if let Some(delete_files_count_threshold) = handler_args
        .with_options
        .get(COMPACTION_DELETE_FILES_COUNT_THRESHOLD)
    {
        let delete_files_count_threshold =
            delete_files_count_threshold.parse::<usize>().map_err(|_| {
                ErrorCode::InvalidInputSyntax(format!(
                    "{} must be greater than 0: {}",
                    COMPACTION_DELETE_FILES_COUNT_THRESHOLD, delete_files_count_threshold
                ))
            })?;
        if delete_files_count_threshold == 0 {
            bail!(format!(
                "{} must be greater than 0",
                COMPACTION_DELETE_FILES_COUNT_THRESHOLD
            ));
        }
        sink_with.insert(
            COMPACTION_DELETE_FILES_COUNT_THRESHOLD.to_owned(),
            delete_files_count_threshold.to_string(),
        );
        ctx.source_remove(COMPACTION_DELETE_FILES_COUNT_THRESHOLD);
    }

    // 16. compaction.delete_position_records_count_threshold
    if let Some(delete_position_records_count_threshold) = handler_args
        .with_options
        .get(COMPACTION_DELETE_POSITION_RECORDS_COUNT_THRESHOLD)
    {
        let delete_position_records_count_threshold =
            delete_position_records_count_threshold.parse::<u64>().map_err(|_| {
                ErrorCode::InvalidInputSyntax(format!(
                    "{} must be greater than 0: {}",
                    COMPACTION_DELETE_POSITION_RECORDS_COUNT_THRESHOLD, delete_position_records_count_threshold
                ))
            })?;
        if delete_position_records_count_threshold == 0 {
            bail!(format!(
                "{} must be greater than 0",
                COMPACTION_DELETE_POSITION_RECORDS_COUNT_THRESHOLD
            ));
        }
        sink_with.insert(
            COMPACTION_DELETE_POSITION_RECORDS_COUNT_THRESHOLD.to_owned(),
            delete_position_records_count_threshold.to_string(),
        );
        ctx.source_remove(COMPACTION_DELETE_POSITION_RECORDS_COUNT_THRESHOLD);
    }

    // 17. compaction.delete_equality_records_count_threshold
    if let Some(delete_equality_records_count_threshold) = handler_args
        .with_options
        .get(COMPACTION_DELETE_EQUALITY_RECORDS_COUNT_THRESHOLD)
    {
        let delete_equality_records_count_threshold = delete_equality_records_count_threshold
            .parse::<u64>()
            .map_err(|_| {
                ErrorCode::InvalidInputSyntax(format!(
                    "{} must be greater than 0: {}",
                    COMPACTION_DELETE_EQUALITY_RECORDS_COUNT_THRESHOLD,
                    delete_equality_records_count_threshold
                ))
            })?;
        if delete_equality_records_count_threshold == 0 {
            bail!(format!(
                "{} must be greater than 0",
                COMPACTION_DELETE_EQUALITY_RECORDS_COUNT_THRESHOLD
            ));
        }
        sink_with.insert(
            COMPACTION_DELETE_EQUALITY_RECORDS_COUNT_THRESHOLD.to_owned(),
            delete_equality_records_count_threshold.to_string(),
        );
        ctx.source_remove(COMPACTION_DELETE_EQUALITY_RECORDS_COUNT_THRESHOLD);
    }

    // 18. compaction.trigger_snapshot_count
    if let Some(trigger_snapshot_count) = handler_args
        .with_options
        .get(COMPACTION_TRIGGER_SNAPSHOT_COUNT)
    {
        let trigger_snapshot_count = trigger_snapshot_count.parse::<usize>().map_err(|_| {
            ErrorCode::InvalidInputSyntax(format!(
                "{} must be greater than 0: {}",
                COMPACTION_TRIGGER_SNAPSHOT_COUNT, trigger_snapshot_count
            ))
        })?;
        if trigger_snapshot_count == 0 {
            bail!(format!(
                "{} must be greater than 0",
                COMPACTION_TRIGGER_SNAPSHOT_COUNT
            ));
        }
        sink_with.insert(
            COMPACTION_TRIGGER_SNAPSHOT_COUNT.to_owned(),
            trigger_snapshot_count.to_string(),
        );
        ctx.source_remove(COMPACTION_TRIGGER_SNAPSHOT_COUNT);
    }

    // 19. compaction.target_file_size_mb
    if let Some(target_file_size_mb) = handler_args
        .with_options
        .get(COMPACTION_TARGET_FILE_SIZE_MB)
    {
        let target_file_size_mb = target_file_size_mb.parse::<u64>().map_err(|_| {
            ErrorCode::InvalidInputSyntax(format!(
                "{} must be greater than 0: {}",
                COMPACTION_TARGET_FILE_SIZE_MB, target_file_size_mb
            ))
        })?;
        if target_file_size_mb == 0 {
            bail!(format!(
                "{} must be greater than 0",
                COMPACTION_TARGET_FILE_SIZE_MB
            ));
        }
        sink_with.insert(
            COMPACTION_TARGET_FILE_SIZE_MB.to_owned(),
            target_file_size_mb.to_string(),
        );
        ctx.source_remove(COMPACTION_TARGET_FILE_SIZE_MB);
    }

    // 20. compaction.type
    if let Some(compaction_type) = handler_args.with_options.get(COMPACTION_TYPE) {
        let compaction_type = CompactionType::try_from(compaction_type.as_str()).map_err(|_| {
            ErrorCode::InvalidInputSyntax(format!(
                "invalid compaction_type: {}, must be one of {:?}",
                compaction_type,
                [
                    CompactionType::Full,
                    CompactionType::SmallFiles,
                    CompactionType::FilesWithDelete,
                    CompactionType::SmallFilesWithDelete
                ]
            ))
        })?;

        sink_with.insert(
            COMPACTION_TYPE.to_owned(),
            compaction_type.as_str().to_owned(),
        );
        ctx.source_remove(COMPACTION_TYPE);
    }

    // 21. compaction.write_parquet_compression
    if let Some(write_parquet_compression) = handler_args
        .with_options
        .get(COMPACTION_WRITE_PARQUET_COMPRESSION)
    {
        sink_with.insert(
            COMPACTION_WRITE_PARQUET_COMPRESSION.to_owned(),
            write_parquet_compression.to_owned(),
        );
        ctx.source_remove(COMPACTION_WRITE_PARQUET_COMPRESSION);
    }

    // 22. compaction.write_parquet_max_row_group_rows
    if let Some(write_parquet_max_row_group_rows) = handler_args
        .with_options
        .get(COMPACTION_WRITE_PARQUET_MAX_ROW_GROUP_ROWS)
    {
        let write_parquet_max_row_group_rows = write_parquet_max_row_group_rows
            .parse::<usize>()
            .map_err(|_| {
                ErrorCode::InvalidInputSyntax(format!(
                    "{} must be a positive integer: {}",
                    COMPACTION_WRITE_PARQUET_MAX_ROW_GROUP_ROWS, write_parquet_max_row_group_rows
                ))
            })?;
        if write_parquet_max_row_group_rows == 0 {
            bail!(format!(
                "{} must be greater than 0",
                COMPACTION_WRITE_PARQUET_MAX_ROW_GROUP_ROWS
            ));
        }
        sink_with.insert(
            COMPACTION_WRITE_PARQUET_MAX_ROW_GROUP_ROWS.to_owned(),
            write_parquet_max_row_group_rows.to_string(),
        );
        ctx.source_remove(COMPACTION_WRITE_PARQUET_MAX_ROW_GROUP_ROWS);
    }

    // 23. compaction.write_parquet_max_row_group_bytes
    if let Some(write_parquet_max_row_group_bytes) = handler_args
        .with_options
        .get(COMPACTION_WRITE_PARQUET_MAX_ROW_GROUP_BYTES)
    {
        let write_parquet_max_row_group_bytes = write_parquet_max_row_group_bytes
            .parse::<usize>()
            .map_err(|_| {
                ErrorCode::InvalidInputSyntax(format!(
                    "{} must be a positive integer: {}",
                    COMPACTION_WRITE_PARQUET_MAX_ROW_GROUP_BYTES, write_parquet_max_row_group_bytes
                ))
            })?;
        if write_parquet_max_row_group_bytes == 0 {
            bail!(format!(
                "{} must be greater than 0",
                COMPACTION_WRITE_PARQUET_MAX_ROW_GROUP_BYTES
            ));
        }
        sink_with.insert(
            COMPACTION_WRITE_PARQUET_MAX_ROW_GROUP_BYTES.to_owned(),
            write_parquet_max_row_group_bytes.to_string(),
        );
        ctx.source_remove(COMPACTION_WRITE_PARQUET_MAX_ROW_GROUP_BYTES);
    }

    // 24. partition_by (validate against ctx.pks — for MV these come from stream_key)
    let partition_by = handler_args
        .with_options
        .get("partition_by")
        .map(|v| v.to_owned());

    if let Some(partition_by) = &partition_by {
        let mut partition_columns = vec![];
        for (column, _) in parse_partition_by_exprs(partition_by.clone())? {
            table
                .columns()
                .iter()
                .find(|col| col.name().eq_ignore_ascii_case(&column))
                .ok_or_else(|| {
                    ErrorCode::InvalidInputSyntax(format!(
                        "Partition source column does not exist in schema: {}",
                        column
                    ))
                })?;

            partition_columns.push(column);
        }

        ensure_partition_columns_are_prefix_of_primary_key(&partition_columns, ctx.pks).map_err(
            |_| {
                ErrorCode::InvalidInputSyntax(
                    "The partition columns should be the prefix of the primary key".to_owned(),
                )
            },
        )?;

        sink_with.insert("partition_by".to_owned(), partition_by.to_owned());
        ctx.source_remove("partition_by");
    }

    // 25. order_key
    let order_key = handler_args
        .with_options
        .get(ORDER_KEY)
        .map(|v| v.to_owned());
    if let Some(order_key) = &order_key {
        validate_order_key_columns(order_key, table.columns().iter().map(|col| col.name()))
            .map_err(|err| ErrorCode::InvalidInputSyntax(err.to_report_string()))?;

        sink_with.insert(ORDER_KEY.to_owned(), order_key.to_owned());
        ctx.source_remove(ORDER_KEY);
    }

    Ok(sink_with)
}

/// Derive primary key column names for an iceberg engine table.
///
/// Renames `_row_id` to `iceberg_row_id` for tables without explicit primary keys.
pub(crate) fn derive_table_pks(table: &TableCatalog) -> Vec<String> {
    let pks: Vec<String> = table
        .pk_column_names()
        .iter()
        .map(|c| c.to_string())
        .collect();

    // For the table without primary key. We will use `_row_id` as primary key.
    if pks.len() == 1 && pks[0].eq(ROW_ID_COLUMN_NAME) {
        vec![RISINGWAVE_ICEBERG_ROW_ID.to_owned()]
    } else {
        pks
    }
}

/// Derive primary key column names for an iceberg engine materialized view.
///
/// MVs derive their key from the query's stream_key, not from a declared PK.
pub(crate) fn derive_mv_pks(table: &TableCatalog) -> Vec<String> {
    table
        .stream_key()
        .iter()
        .map(|&idx| table.columns()[idx].name().to_string())
        .collect()
}

fn ensure_partition_columns_are_prefix_of_primary_key(
    partition_columns: &[String],
    primary_key_columns: &[String],
) -> std::result::Result<(), String> {
    if partition_columns.len() > primary_key_columns.len() {
        return Err("Partition columns cannot be longer than primary key columns.".to_owned());
    }

    for (i, partition_col) in partition_columns.iter().enumerate() {
        if !primary_key_columns
            .get(i)
            .is_some_and(|pk| pk.eq_ignore_ascii_case(partition_col))
        {
            return Err(format!(
                "Partition column '{}' is not a prefix of the primary key.",
                partition_col
            ));
        }
    }

    Ok(())
}
