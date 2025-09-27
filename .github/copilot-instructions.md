# sql-db-utils - GitHub Copilot Instructions

**SQL database utilities package for Python developers that provides declarative model generation and session management.**

Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.

## Working Effectively

### Bootstrap, Build, and Test Repository:
- `pip install -e .` -- installs the package in development mode. NEVER CANCEL: Takes 10-60 seconds, may timeout due to network issues. Set timeout to 120+ seconds (extra margin for slow mirrors or network issues).
- `pip install coverage pre-commit pytest pytest-cov ruff` -- installs development dependencies. NEVER CANCEL: Takes 30-120 seconds. Set timeout to 180+ seconds (extra margin for slow mirrors or network issues).
- `python -m pytest tests/ -v` -- runs unit tests (when tests are available)
- `ruff check .` -- runs linting (takes ~0.01 seconds)
- `ruff format --check .` -- checks code formatting (takes ~0.01 seconds)

### Environment Configuration:
- Package requires Python >= 3.13
- Core dependencies: SQLAlchemy >= 2.0.38, sqlalchemy-utils, psycopg, python-dateutil, whenever
- Optional dependencies available: polars, pandas, async, binary, codegen
- Database support: PostgreSQL (primary), with pooling and connection management
- Configuration via environment variables and PostgreSQL connection strings

### Run Integration Tests with Real Database:
- Start PostgreSQL: `docker run -d --name test-postgres -p 5432:5432 -e POSTGRES_PASSWORD=test postgres:15-alpine` (NEVER CANCEL: Takes 30-60 seconds for first download)
- Wait for startup: `sleep 10`
- Run integration tests: `POSTGRES_URI=postgresql://postgres:test@localhost:5432/test python -c "from sql_db_utils import SQLSessionManager; print('SQL integration test passed')"` (basic session management test)
- Clean up: `docker stop test-postgres && docker rm test-postgres`

## Validation Scenarios

### Always Test After Making Changes:
1. **Import Test**: `python -c "from sql_db_utils import SQLSessionManager; from sql_db_utils.asyncio import SQLSessionManager as AsyncSQLSessionManager; print('Import successful')"`
2. **Basic Session Management Test** (requires PostgreSQL running):
   ```bash
   POSTGRES_URI=postgresql://postgres:test@localhost:5432/test python -c "
   from sql_db_utils import SQLSessionManager
   manager = SQLSessionManager()
   print('Session Manager created successfully')
   "
   ```
3. **Precreate/Postcreate Functionality Test**:
   ```bash
   python -c "
   from sql_db_utils import SQLSessionManager
   manager = SQLSessionManager()
   
   @manager.register_precreate('test_db')
   def test_precreate(tenant_id):
       return 'SELECT 1;'
   
   @manager.register_postcreate('test_db')
   def test_postcreate(tenant_id):
       return 'SELECT 2;'
   
   print('Precreate/Postcreate registration successful')
   "
   ```
4. **Run Full Test Suite**: `python -m pytest tests/ -v --cov=sql_db_utils --cov-report=term-missing` (when tests are available)
5. **Linting**: `ruff check . && ruff format --check .`

### Manual Testing Requirements:
- ALWAYS test session management functionality after code changes
- Test both synchronous and asynchronous implementations
- Verify precreate and postcreate hooks work correctly
- Test with different PostgreSQL configurations and connection parameters
- Test database creation, connection pooling, and engine management

## Common Tasks

### Repository Structure:
```
sql-db-utils/
├── .github/workflows/         # CI/CD pipelines
├── sql_db_utils/             # Main package source
│   ├── __init__.py           # Main exports
│   ├── config.py             # Configuration settings
│   ├── constants.py          # Package constants
│   ├── datetime_utils.py     # Date/time utilities
│   ├── session_management.py # Core session management (sync)
│   ├── sql_creations.py      # SQL table creation utilities
│   ├── sql_extras.py         # Additional SQL utilities
│   ├── sql_retry_handler.py  # Query retry mechanisms
│   ├── sql_utils.py          # General SQL utilities
│   ├── declarative_utils.py  # Declarative model utilities
│   ├── declaratives.py       # Base declarative classes
│   ├── codegen.py           # Code generation utilities
│   ├── aggrid/              # AG Grid integration utilities
│   │   ├── date_filters.py
│   │   ├── number_filters.py
│   │   └── text_filters.py
│   └── asyncio/             # Asynchronous implementations
│       ├── __init__.py
│       ├── session_management.py    # Async session management
│       ├── sql_creations.py         # Async SQL creation utilities
│       ├── sql_creation_helper.py   # Async creation helpers
│       ├── sql_retry_handler.py     # Async retry mechanisms
│       ├── sql_utils.py            # Async SQL utilities
│       ├── declarative_utils.py    # Async declarative utilities
│       ├── declaratives.py         # Async base classes
│       ├── codegen.py             # Async code generation
│       └── inspector_utils.py      # Database inspection utilities
├── tests/                    # Test files (when available)
├── pyproject.toml           # Project configuration
└── README.md               # Documentation
```

### Key Files to Check After Changes:
- Always verify `sql_db_utils/__init__.py` after changing main exports
- Check `sql_db_utils/config.py` after modifying configuration handling
- Verify sync/async parity between `sql_db_utils/session_management.py` and `sql_db_utils/asyncio/session_management.py`
- Update `sql_db_utils/sql_creations.py` and `sql_db_utils/asyncio/sql_creations.py` for SQL creation changes
- Test declarative utilities in both sync and async versions
- Verify codegen functionality if making changes to code generation features
- Update tests when adding new functionality
- Run integration tests with real PostgreSQL database

### Development Dependencies:
- **Testing**: pytest, pytest-cov, coverage
- **Linting**: ruff (replaces black, flake8, isort)
- **Git hooks**: pre-commit 
- **Type checking**: Built into package development
- **Core Dependencies**: SQLAlchemy, sqlalchemy-utils, psycopg, python-dateutil, whenever

### Build and Package:
- `python -m build` -- builds distribution packages. NEVER CANCEL: May fail due to network timeouts depending on the configured build backend and network environment. Package requires Python >= 3.13.
- Package metadata in `pyproject.toml`
- Uses hatchling as build backend
- **Note**: Package requires specific Python version (>=3.13) which may not be available in all environments

### Session Management Features:
- **Precreate Hooks**: Execute before database/table creation for setup tasks
- **Postcreate Hooks**: Execute after database/table creation for initialization
- **Auto Hooks**: Return SQL statements to be executed automatically
- **Manual Hooks**: Receive session objects for custom operations
- **Multi-database Support**: Register hooks for single or multiple databases
- **Tenant Support**: All hooks receive tenant_id parameter for multi-tenant applications

## Database Features and Testing

### Supported Database Features:
- **PostgreSQL**: Primary database with full feature support
- **Connection Pooling**: Configurable via PostgresConfig.PG_ENABLE_POOLING
- **Connection Retry**: Built-in retry mechanisms with PostgresConfig.PG_MAX_RETRY
- **Database Creation**: Automatic database creation if not exists
- **Schema Support**: Multi-schema support with declarative utilities
- **Transaction Management**: Automatic transaction handling in hooks

### Session Management Testing:
- **Engine Creation**: Test `_get_engine()` method with various configurations
- **Hook Execution**: Verify precreate/postcreate hooks execute in correct order
- **Connection Pooling**: Test with/without pooling enabled
- **Multi-tenant**: Test with different tenant_id values
- **Error Handling**: Test connection failures and retry mechanisms

### Setting up Test Database with Docker:
- PostgreSQL: `docker run -d --name test-postgres -p 5432:5432 -e POSTGRES_PASSWORD=test postgres:15-alpine`
- Create test database: `docker exec -it test-postgres psql -U postgres -c "CREATE DATABASE test;"`

## CI/CD Pipeline (.github/workflows)

### Linter Pipeline:
- Runs on pull requests and pushes
- Uses ruff for linting and formatting
- ALWAYS run `ruff check .` and `ruff format --check .` before committing
- Pre-commit hooks should handle formatting automatically

### Package Publishing:
- Triggers on git tags
- Builds with hatchling backend
- Publishes to PyPI
- Requires Python >= 3.13 environment

## Critical Notes

### Session Management Execution Order:
1. **Engine Creation**: Database connection and engine setup
2. **Precreate Hooks**: Execute custom setup before table creation
3. **Table Creation**: `create_default_psql_dependencies()` creates tables/metadata
4. **Postcreate Hooks**: Execute initialization after table creation

### Hook Implementation Patterns:
- **Auto Hooks**: Return SQL strings or lists of SQL strings
- **Manual Hooks**: Receive (session, tenant_id) parameters for custom logic
- **Registration**: Use `@manager.register_precreate()` or `@manager.register_precreate_manual()`
- **Multi-Database**: Pass list of database names to register for multiple databases

### Async/Sync Parity:
- Both implementations must have identical API and functionality
- Async version uses `async`/`await` patterns appropriately
- Session types differ: `Session` vs `AsyncSession`
- Engine types differ: `Engine` vs `AsyncEngine`

### Configuration and Environment:
- PostgresConfig class manages all database configuration
- Environment variables control connection parameters
- ModuleConfig handles application-level settings
- Connection pooling and retry settings are configurable

## Troubleshooting

### Common Issues:
1. **Import Error**: Check Python version (>=3.13 required)
2. **Connection Failures**: Verify PostgreSQL is running and accessible
3. **Linting Failures**: Run `ruff format .` to auto-fix formatting issues
4. **Missing Dependencies**: Run `pip install -e .` to reinstall package
5. **Hook Execution**: Verify hooks are registered before engine creation

### Session Management Issues:
- **Engine Not Created**: Check database URI and connection parameters
- **Hooks Not Executing**: Ensure registration occurs before `get_session()` calls
- **Transaction Errors**: Verify database permissions and connection state
- **Pooling Issues**: Check PostgresConfig.PG_ENABLE_POOLING setting

### Development Environment:
- Python 3.13+ required (check with `python --version`)
- PostgreSQL server required for integration testing
- Docker recommended for consistent database testing
- Pre-commit hooks help maintain code quality

### Async/Sync Coordination:
- Changes to sync version should be mirrored in async version
- Test both implementations when making session management changes
- Verify async patterns use proper `await` keywords
- Check that both versions handle errors consistently