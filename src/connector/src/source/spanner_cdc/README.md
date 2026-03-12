# Spanner CDC Source Connector

**Main Documentation**: See [`SPANNER_CDC.md`](../../../../SPANNER_CDC.md) for comprehensive documentation.

## Quick Reference

### Connector Name

```sql
connector = 'spanner-cdc'  -- Use hyphen, not underscore
```

### Required Parameters

- `spanner.project` - GCP project ID
- `spanner.instance` - Spanner instance ID
- `database.name` - Spanner database ID
- `spanner.change_stream.name` - Change stream name

### Key Features

- **Shared Reader Architecture**: Multiple tables from the same source share a single reader instance
- **Full Type Support**: All Spanner types supported (PROTO→BYTEA, ENUM→VARCHAR, STRUCT→JSONB)
- **Automatic Partition Management**: Parent-child coordination with watermark-based scheduling
- **Schema Evolution**: Automatic ADD COLUMN detection
- **Production Ready**: Checkpointing, retry with exponential backoff, graceful shutdown

## Example

```sql
CREATE SOURCE spanner_source WITH (
    connector = 'spanner-cdc',
    spanner.project = 'my-project',
    spanner.instance = 'my-instance',
    database.name = 'my-database',
    spanner.change_stream.name = 'my_stream',
    spanner.credentials_path = '/path/to/service-account.json'
) FORMAT PLAIN ENCODE JSON;

CREATE TABLE users FROM spanner_source TABLE 'users';
CREATE TABLE orders FROM spanner_source TABLE 'orders';
```

## Architecture Overview

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│ Table Actor │  │ Table Actor │  │ Table Actor │
│  (users)    │  │ (products)  │  │  (orders)   │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       └────────────────┼────────────────┘
                        ▼
         ┌──────────────────────────────┐
         │   Shared Reader (Arc)        │
         │   broadcast::Sender          │
         └──────────────────────────────┘
                        │
                        ▼
              Google Cloud Spanner
         (single query per partition)
```

## Testing

```bash
# Run e2e tests (remote builder required)
make -C scripts/remote run

# With production Spanner
make -C scripts/remote run profile=spanner-real
```

## Type Mapping

| Spanner | RisingWave | Notes |
|---------|------------|-------|
| BOOL | BOOLEAN | |
| INT64 | BIGINT | |
| STRING | VARCHAR | |
| BYTES | BYTEA | |
| TIMESTAMP | TIMESTAMPTZ | |
| NUMERIC | DECIMAL | |
| JSON | JSONB | |
| PROTO | BYTEA | Raw bytes preserved |
| ENUM | VARCHAR | Enum name preserved |
| STRUCT | JSONB | Serialized |
| ARRAY\<T\> | LIST/JSONB | Element-wise |

See [`SPANNER_CDC.md`](../../../../SPANNER_CDC.md) for the complete type mapping table.
