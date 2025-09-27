# SQL DB Utils

A powerful SQL database utilities package for Python developers that provides declarative model generation and session management.

## Features

- Automatic declarative model generation for SQL databases
- Schema-aware session management with precreate and postcreate hooks
- Support for both synchronous and asynchronous operations
- Project-based database isolation
- Security-enabled database connections

## Installation

Install using pip:

```bash
pip install sql-db-utils
```

### Optional Dependencies

- For Polars support: `pip install sql-db-utils[polars]`
- For Pandas support: `pip install sql-db-utils[pandas]`
- For async support: `pip install sql-db-utils[async]`
- For binary protocol: `pip install sql-db-utils[binary]`
- For codegen features: `pip install sql-db-utils[codegen]`

## Requirements

- Python >= 3.13
- SQLAlchemy >= 2.0.38
- Additional dependencies based on optional features

## Session Management Hooks

The `SQLSessionManager` provides two types of hooks for customizing database setup:

### Precreate Hooks

Precreate hooks execute **before** database tables and meta information are created. These are useful for:
- Setting up database extensions
- Creating custom schemas
- Preparing database environment
- Initial database configuration

#### Automatic Precreate
Returns SQL statements to be executed automatically:

```python
from sql_db_utils import SQLSessionManager

session_manager = SQLSessionManager()

@session_manager.register_precreate("my_database")
def setup_database_extensions(tenant_id):
    return "CREATE EXTENSION IF NOT EXISTS uuid-ossp;"

# Or for multiple databases
@session_manager.register_precreate(["db1", "db2"])
def setup_multiple_databases(tenant_id):
    return [
        "CREATE EXTENSION IF NOT EXISTS uuid-ossp;",
        "CREATE EXTENSION IF NOT EXISTS pgcrypto;"
    ]
```

#### Manual Precreate
Receives a session object for custom operations:

```python
@session_manager.register_precreate_manual("my_database")
def custom_precreate_setup(session, tenant_id):
    # Custom logic with session
    session.execute("CREATE SCHEMA IF NOT EXISTS custom_schema;")
    # Additional setup logic here
```

### Postcreate Hooks

Postcreate hooks execute **after** database tables and meta information are created. These are useful for:
- Seeding initial data
- Creating triggers and procedures
- Setting up initial user permissions
- Post-creation optimizations

#### Automatic Postcreate
Returns SQL statements to be executed automatically:

```python
@session_manager.register_postcreate("my_database")
def seed_initial_data(tenant_id):
    return "INSERT INTO users (name) VALUES ('admin') ON CONFLICT DO NOTHING;"

# Or for multiple databases
@session_manager.register_postcreate(["db1", "db2"])
def setup_multiple_databases(tenant_id):
    return [
        "INSERT INTO settings (key, value) VALUES ('version', '1.0') ON CONFLICT DO NOTHING;",
        "INSERT INTO roles (name) VALUES ('admin') ON CONFLICT DO NOTHING;"
    ]
```

#### Manual Postcreate
Receives a session object for custom operations:

```python
@session_manager.register_postcreate_manual("my_database")
def custom_postcreate_setup(session, tenant_id):
    # Custom logic with session
    session.execute("INSERT INTO initial_data (tenant_id) VALUES (:tenant_id);", {"tenant_id": tenant_id})
    # Additional setup logic here
```

### Execution Order

1. **Precreate hooks** (before table creation)
   - `register_precreate` functions are executed
   - `register_precreate_manual` functions are executed
2. **Database table creation** (`create_default_psql_dependencies`)
3. **Postcreate hooks** (after table creation)
   - `register_postcreate` functions are executed
   - `register_postcreate_manual` functions are executed

### Async Support

All hooks work identically with the async session manager:

```python
from sql_db_utils.asyncio import SQLSessionManager

async_session_manager = SQLSessionManager()

@async_session_manager.register_precreate("my_database")
def setup_extensions(tenant_id):
    return "CREATE EXTENSION IF NOT EXISTS uuid-ossp;"

@async_session_manager.register_postcreate_manual("my_database")
async def seed_data(session, tenant_id):
    await session.execute("INSERT INTO users (tenant_id) VALUES (:tenant_id);", {"tenant_id": tenant_id})
```

## Authors

- Faizan (faizanazim11@gmail.com)
