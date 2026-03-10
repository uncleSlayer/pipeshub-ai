"""
ClickHouse Connector

Syncs databases, tables, and views from ClickHouse.
"""
import asyncio
import hashlib
import json
import os
import re
import uuid
from dataclasses import dataclass, field
from logging import Logger
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

from aiolimiter import AsyncLimiter

from app.config.configuration_service import ConfigurationService
from app.config.constants.arangodb import (
    Connectors,
    MimeTypes,
    OriginTypes,
)
from app.connectors.core.base.connector.connector_service import BaseConnector
from app.connectors.core.base.data_processor.data_source_entities_processor import (
    DataSourceEntitiesProcessor,
)
from app.connectors.core.base.data_store.data_store import DataStoreProvider
from app.connectors.core.registry.connector_builder import (
    AuthField,
    CommonFields,
    ConnectorBuilder,
    ConnectorScope,
    DocumentationLink,
)
from app.connectors.core.registry.auth_builder import AuthBuilder, AuthType
from app.connectors.core.registry.filters import (
    FilterCategory,
    FilterCollection,
    FilterField,
    FilterOption,
    FilterOptionsResponse,
    FilterType,
    IndexingFilterKey,
    MultiselectOperator,
    OptionSourceType,
    load_connector_filters,
)
from app.connectors.core.base.sync_point.sync_point import (
    SyncDataPointType,
    SyncPoint,
)
from app.connectors.sources.clickhouse.common.apps import ClickHouseApp
from app.models.entities import (
    AppUser,
    IndexingStatus,
    Record,
    RecordGroup,
    RecordGroupType,
    RecordType,
    SQLTableRecord,
    SQLViewRecord,
    User,
)
from app.models.permission import EntityType, Permission, PermissionType
from app.sources.client.clickhouse.clickhouse import ClickHouseClient
from app.sources.external.clickhouse.clickhouse import ClickHouseDataSource
from app.utils.streaming import create_stream_record_response
from app.utils.time_conversion import get_epoch_timestamp_in_ms
from fastapi import HTTPException
from fastapi.responses import StreamingResponse

CLICKHOUSE_TABLE_ROW_LIMIT = int(os.getenv("CLICKHOUSE_TABLE_ROW_LIMIT", "1000"))

SYSTEM_DATABASES = ('system', 'INFORMATION_SCHEMA', 'information_schema')

VIEW_ENGINES = ('View', 'MaterializedView')


@dataclass
class SyncStats:
    databases_synced: int = 0
    tables_new: int = 0
    tables_updated: int = 0
    views_new: int = 0
    views_updated: int = 0
    errors: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            'databases_synced': self.databases_synced,
            'tables_new': self.tables_new,
            'tables_updated': self.tables_updated,
            'views_new': self.views_new,
            'views_updated': self.views_updated,
            'errors': self.errors,
        }

    def log_summary(self, logger) -> None:
        logger.info(
            f"Sync Stats: "
            f"Databases={self.databases_synced}, Tables(new={self.tables_new}, updated={self.tables_updated}), "
            f"Views(new={self.views_new}, updated={self.views_updated}) | "
            f"Errors={self.errors}"
        )


@dataclass
class ClickHouseDatabase:
    name: str


@dataclass
class ClickHouseTable:
    name: str
    database_name: str
    engine: str = ""
    row_count: Optional[int] = None
    total_bytes: Optional[int] = None
    columns: List[Dict[str, Any]] = field(default_factory=list)
    primary_keys: List[str] = field(default_factory=list)
    order_by: List[str] = field(default_factory=list)
    comment: str = ""
    metadata_modification_time: Optional[str] = None

    @property
    def fqn(self) -> str:
        return f"{self.database_name}.{self.name}"


@ConnectorBuilder("ClickHouse")\
    .in_group("ClickHouse")\
    .with_description("Sync databases, tables, and views from ClickHouse")\
    .with_categories(["Database"])\
    .with_scopes([ConnectorScope.PERSONAL.value])\
    .with_auth([
        AuthBuilder.type(AuthType.BASIC_AUTH).fields([
            AuthField(
                name="host",
                display_name="Host",
                placeholder="localhost",
                description="ClickHouse server host",
                field_type="TEXT",
                max_length=500,
                is_secret=False,
                required=True
            ),
            AuthField(
                name="port",
                display_name="Port",
                placeholder="8123",
                description="ClickHouse HTTP interface port",
                field_type="TEXT",
                max_length=10,
                is_secret=False,
                required=True
            ),
            AuthField(
                name="database",
                display_name="Database",
                placeholder="default",
                description="Database name to connect to",
                field_type="TEXT",
                max_length=200,
                is_secret=False,
                required=True
            ),
            AuthField(
                name="username",
                display_name="Username",
                placeholder="default",
                description="ClickHouse username",
                field_type="TEXT",
                max_length=200,
                is_secret=False,
                required=True
            ),
            AuthField(
                name="password",
                display_name="Password",
                placeholder="Enter password",
                description="ClickHouse password",
                field_type="PASSWORD",
                max_length=500,
                is_secret=True,
                required=True
            ),
            AuthField(
                name="secure",
                display_name="Use HTTPS",
                placeholder="",
                description="Enable secure connection (HTTPS)",
                field_type="CHECKBOX",
                max_length=0,
                is_secret=False,
                required=False
            ),
        ]),
        AuthBuilder.type(AuthType.BEARER_TOKEN).fields([
            AuthField(
                name="host",
                display_name="Host",
                placeholder="localhost",
                description="ClickHouse server host",
                field_type="TEXT",
                max_length=500,
                is_secret=False,
                required=True
            ),
            AuthField(
                name="port",
                display_name="Port",
                placeholder="8443",
                description="ClickHouse HTTP interface port",
                field_type="TEXT",
                max_length=10,
                is_secret=False,
                required=True
            ),
            AuthField(
                name="database",
                display_name="Database",
                placeholder="default",
                description="Database name to connect to",
                field_type="TEXT",
                max_length=200,
                is_secret=False,
                required=True
            ),
            AuthField(
                name="token",
                display_name="Access Token",
                placeholder="Enter access token",
                description="ClickHouse access token",
                field_type="PASSWORD",
                max_length=1000,
                is_secret=True,
                required=True
            ),
            AuthField(
                name="secure",
                display_name="Use HTTPS",
                placeholder="",
                description="Enable secure connection (HTTPS)",
                field_type="CHECKBOX",
                max_length=0,
                is_secret=False,
                required=False
            ),
        ])
    ])\
    .configure(lambda builder: builder
        .with_icon("/assets/icons/connectors/clickhouse.svg")
        .add_documentation_link(DocumentationLink(
            "ClickHouse Setup",
            "https://clickhouse.com/docs/",
            "setup"
        ))
        .add_filter_field(FilterField(
            name="databases",
            display_name="Databases",
            filter_type=FilterType.MULTISELECT,
            category=FilterCategory.SYNC,
            description="Select specific databases to sync",
            option_source_type=OptionSourceType.DYNAMIC,
            default_value=[],
            default_operator=MultiselectOperator.IN.value
        ))
        .add_filter_field(FilterField(
            name="tables",
            display_name="Tables",
            filter_type=FilterType.MULTISELECT,
            category=FilterCategory.SYNC,
            description="Select specific tables to sync",
            option_source_type=OptionSourceType.DYNAMIC,
            default_value=[],
            default_operator=MultiselectOperator.IN.value
        ))
        .add_filter_field(FilterField(
            name="views",
            display_name="Views",
            filter_type=FilterType.MULTISELECT,
            category=FilterCategory.SYNC,
            description="Select specific views to sync",
            option_source_type=OptionSourceType.DYNAMIC,
            default_value=[],
            default_operator=MultiselectOperator.IN.value
        ))
        .add_filter_field(FilterField(
            name=IndexingFilterKey.TABLES.value,
            display_name="Index Tables",
            filter_type=FilterType.BOOLEAN,
            category=FilterCategory.INDEXING,
            description="Enable indexing of tables",
            default_value=True
        ))
        .add_filter_field(CommonFields.enable_manual_sync_filter())
        .with_sync_strategies(["SCHEDULED", "MANUAL"])
        .with_scheduled_config(True, 120)
        .with_sync_support(True)
        .with_agent_support(False)
    )\
    .build_decorator()
class ClickHouseConnector(BaseConnector):

    def __init__(
        self,
        logger: Logger,
        data_entities_processor: DataSourceEntitiesProcessor,
        data_store_provider: DataStoreProvider,
        config_service: ConfigurationService,
        connector_id: str,
    ) -> None:
        super().__init__(
            ClickHouseApp(connector_id),
            logger,
            data_entities_processor,
            data_store_provider,
            config_service,
            connector_id,
        )
        self.connector_id = connector_id
        self.connector_name = Connectors.CLICKHOUSE
        self.data_source: Optional[ClickHouseDataSource] = None
        self.database_name: Optional[str] = None
        self.batch_size = 100
        self.rate_limiter = AsyncLimiter(25, 1)
        self.connector_scope: Optional[str] = None
        self.created_by: Optional[str] = None
        self.sync_filters: FilterCollection = FilterCollection()
        self.indexing_filters: FilterCollection = FilterCollection()
        self._record_id_cache: Dict[str, str] = {}
        self.sync_stats: SyncStats = SyncStats()
        self._clickhouse_base_url: str = ""

        # Initialize sync point for incremental sync
        org_id = self.data_entities_processor.org_id
        self.tables_sync_point = SyncPoint(
            connector_id=self.connector_id,
            org_id=org_id,
            sync_data_point_type=SyncDataPointType.RECORDS,
            data_store_provider=data_store_provider
        )

    def get_app_users(self, users: List[User]) -> List[AppUser]:
        """Convert User objects to AppUser objects for ClickHouse connector."""
        return [
            AppUser(
                app_name=self.connector_name,
                connector_id=self.connector_id,
                source_user_id=user.source_user_id or user.id or user.email,
                org_id=user.org_id or self.data_entities_processor.org_id,
                email=user.email,
                full_name=user.full_name or user.email,
                is_active=user.is_active if user.is_active is not None else True,
                title=user.title,
            )
            for user in users
            if user.email
        ]

    async def _create_app_users(self) -> None:
        """Create AppUser entries for all active users in the organization."""
        try:
            all_active_users = await self.data_entities_processor.get_all_active_users()
            app_users = self.get_app_users(all_active_users)
            await self.data_entities_processor.on_new_app_users(app_users)
            self.logger.info(f"Created {len(app_users)} app users for ClickHouse connector")
        except Exception as e:
            self.logger.error(f"Error creating app users: {e}", exc_info=True)
            raise

    async def init(self) -> bool:
        try:
            config = await self.config_service.get_config(
                f"/services/connectors/{self.connector_id}/config"
            )
            if not config:
                self.logger.error("ClickHouse configuration not found")
                return False

            client = await ClickHouseClient.build_from_services(
                self.logger, self.config_service, self.connector_id
            )
            self.data_source = ClickHouseDataSource(client)

            auth_config = config.get("auth") or {}
            self.database_name = auth_config.get("database", "default")
            self.connector_scope = auth_config.get("connectorScope", config.get("scope", ConnectorScope.PERSONAL.value))
            self.created_by = config.get("created_by")

            # Build base URL for ClickHouse dashboard links
            host = auth_config.get("host", "localhost")
            port = int(auth_config.get("port", 8123))
            secure = auth_config.get("secure", False)
            if isinstance(secure, str):
                secure = secure.lower() in ("true", "1", "yes")
            scheme = "https" if secure else "http"
            self._clickhouse_base_url = f"{scheme}://{host}:{port}"

            self.sync_filters, self.indexing_filters = await load_connector_filters(
                self.config_service, "clickhouse", self.connector_id, self.logger
            )

            self.logger.info("ClickHouse connector initialized successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to initialize ClickHouse connector: {e}", exc_info=True)
            return False

    # ── Filter helpers ──────────────────────────────────────────────────

    def _get_filter_values(self) -> Tuple[Optional[List[str]], Optional[List[str]], Optional[List[str]]]:
        db_filter = self.sync_filters.get("databases")
        selected_databases = db_filter.value if db_filter and db_filter.value else None

        table_filter = self.sync_filters.get("tables")
        selected_tables = table_filter.value if table_filter and table_filter.value else None

        view_filter = self.sync_filters.get("views")
        selected_views = view_filter.value if view_filter and view_filter.value else None

        return selected_databases, selected_tables, selected_views

    def _build_dashboard_url(self, database: str, table: str) -> str:
        """Build a ClickHouse Play UI URL for the given database and table."""
        if not self._clickhouse_base_url:
            return ""
        query = f"SELECT * FROM `{database}`.`{table}` LIMIT 100"
        from urllib.parse import quote
        return f"{self._clickhouse_base_url}/play#query={quote(query)}"

    async def _get_permissions(self) -> List[Permission]:
        return [Permission(
            type=PermissionType.OWNER,
            entity_type=EntityType.ORG,
        )]

    # ── Data fetching ───────────────────────────────────────────────────

    _IDENTIFIER_RE = re.compile(r"^[a-zA-Z0-9_]+$")

    def _validate_identifier(self, name: str) -> str:
        """Validate a database/table identifier to prevent SQL injection."""
        if not self._IDENTIFIER_RE.match(name):
            raise ValueError(f"Invalid identifier: {name!r}")
        return name

    def _fetch_databases(self) -> List[ClickHouseDatabase]:
        """Fetch non-system databases from ClickHouse."""
        response = self.data_source.list_databases()
        if not response.success:
            self.logger.error(f"Failed to fetch databases: {response.error}")
            return []

        return [
            ClickHouseDatabase(name=db["name"])
            for db in (response.data or [])
        ]

    def _fetch_tables(self, database_name: str) -> List[ClickHouseTable]:
        """Fetch tables for a database including column metadata."""
        response = self.data_source.list_tables(database=database_name)
        if not response.success:
            self.logger.error(f"Failed to fetch tables for {database_name}: {response.error}")
            return []

        tables = []
        for row_dict in (response.data or []):
            columns = self._fetch_columns(database_name, row_dict["name"])

            primary_keys = [column["name"] for column in columns if column.get("is_in_primary_key")]
            order_by = [column["name"] for column in columns if column.get("is_in_sorting_key")]

            tables.append(ClickHouseTable(
                name=row_dict["name"],
                database_name=database_name,
                engine=row_dict.get("engine", ""),
                row_count=row_dict.get("total_rows"),
                total_bytes=row_dict.get("total_bytes"),
                columns=columns,
                primary_keys=primary_keys,
                order_by=order_by,
                comment=row_dict.get("comment", "") or "",
                metadata_modification_time=str(row_dict.get("metadata_modification_time", "")),
            ))
        return tables

    def _fetch_views(self, database_name: str) -> List[ClickHouseTable]:
        """Fetch views (View and MaterializedView) for a database including column metadata."""
        response = self.data_source.list_views(database=database_name)
        if not response.success:
            self.logger.error(f"Failed to fetch views for {database_name}: {response.error}")
            return []

        views = []
        for row_dict in (response.data or []):
            columns = self._fetch_columns(database_name, row_dict["name"])

            views.append(ClickHouseTable(
                name=row_dict["name"],
                database_name=database_name,
                engine=row_dict.get("engine", ""),
                columns=columns,
                comment=row_dict.get("comment", "") or "",
                metadata_modification_time=str(row_dict.get("metadata_modification_time", "")),
            ))
        return views

    def _fetch_columns(self, database: str, table: str) -> List[Dict[str, Any]]:
        """Fetch column definitions for a table from system.columns."""
        response = self.data_source.get_table_schema(database=database, table=table)
        if not response.success:
            self.logger.warning(f"Failed to fetch columns for {database}.{table}: {response.error}")
            return []

        return response.data or []

    def _fetch_table_rows(
        self, database: str, table: str, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Fetch sample rows from a table."""
        row_limit = limit if limit is not None else CLICKHOUSE_TABLE_ROW_LIMIT
        safe_db = self._validate_identifier(database)
        safe_tbl = self._validate_identifier(table)
        response = self.data_source.query(
            query=f"SELECT * FROM `{safe_db}`.`{safe_tbl}` LIMIT {int(row_limit)}"
        )
        if not response.success:
            self.logger.warning(f"Failed to fetch rows for {database}.{table}: {response.error}")
            return []

        col_names = response.data.get("column_names", [])
        return [
            dict(zip(col_names, row))
            for row in response.data.get("result_rows", [])
        ]

    # ── Sync entity methods ─────────────────────────────────────────────

    async def _sync_databases(self, databases: List[ClickHouseDatabase]) -> None:
        """Create a RecordGroup per database."""
        if not databases:
            return
        permissions = await self._get_permissions()
        groups = []
        for db in databases:
            rg = RecordGroup(
                name=db.name,
                external_group_id=db.name,
                group_type=RecordGroupType.SQL_DATABASE,
                connector_name=self.connector_name,
                connector_id=self.connector_id,
                description=f"ClickHouse Database: {db.name}",
            )
            groups.append((rg, permissions))
        await self.data_entities_processor.on_new_record_groups(groups)
        self.logger.info(f"Synced {len(groups)} database record groups")

    async def _process_tables_generator(
        self,
        database_name: str,
        tables: List[ClickHouseTable],
    ) -> AsyncGenerator[Tuple[Record, List[Permission]], None]:
        """Yield (SQLTableRecord, permissions) for each table."""
        for table in tables:
            try:
                fqn = table.fqn
                record_id = str(uuid.uuid4())
                self._record_id_cache[fqn] = record_id

                weburl = self._build_dashboard_url(database_name, table.name)

                current_time = get_epoch_timestamp_in_ms()
                record = SQLTableRecord(
                    id=record_id,
                    record_name=table.name,
                    record_type=RecordType.SQL_TABLE,
                    record_group_type=RecordGroupType.SQL_DATABASE.value,
                    external_record_group_id=database_name,
                    external_record_id=fqn,
                    external_revision_id=str(current_time),
                    origin=OriginTypes.CONNECTOR.value,
                    connector_name=self.connector_name,
                    connector_id=self.connector_id,
                    mime_type=MimeTypes.SQL_TABLE.value,
                    weburl=weburl,
                    source_created_at=current_time,
                    source_updated_at=current_time,
                    database_name=database_name,
                    row_count=table.row_count,
                    size_bytes=table.total_bytes,
                    column_count=len(table.columns),
                    primary_keys=table.primary_keys,
                    comment=table.comment,
                    version=1,
                    inherit_permissions=True,
                )

                if self.indexing_filters and not self.indexing_filters.is_enabled(IndexingFilterKey.TABLES.value):
                    record.indexing_status = IndexingStatus.AUTO_INDEX_OFF.value

                yield (record, [])
                await asyncio.sleep(0)

            except Exception as e:
                self.logger.error(f"Error processing table {table.name}: {e}", exc_info=True)
                continue

    async def _sync_tables(self, database_name: str, tables: List[ClickHouseTable]) -> None:
        """Batch-process tables and call on_new_records."""
        if not tables:
            return

        batch: List[Tuple[Record, List[Permission]]] = []
        total_synced = 0

        async for record, perms in self._process_tables_generator(database_name, tables):
            batch.append((record, perms))
            total_synced += 1

            if len(batch) >= self.batch_size:
                self.logger.debug(f"Processing batch of {len(batch)} tables")
                await self.data_entities_processor.on_new_records(batch)
                batch = []

        if batch:
            await self.data_entities_processor.on_new_records(batch)

        self.logger.info(f"Synced {total_synced} tables in {database_name}")

    async def _sync_updated_tables(self, database_name: str, tables: List[ClickHouseTable]) -> None:
        """Sync tables whose content or schema has changed."""
        if not tables:
            return

        self.logger.info(f"Processing {len(tables)} updated tables in {database_name}")

        for table in tables:
            try:
                fqn = table.fqn

                existing_record = await self.data_entities_processor.get_record_by_external_id(
                    connector_id=self.connector_id,
                    external_record_id=fqn
                )
                if not existing_record:
                    self.logger.warning(f"No existing record found for updated table {fqn}, skipping")
                    continue

                current_time = get_epoch_timestamp_in_ms()

                updated_record = SQLTableRecord(
                    id=existing_record.id,
                    record_name=table.name,
                    record_type=RecordType.SQL_TABLE,
                    record_group_type=RecordGroupType.SQL_DATABASE.value,
                    external_record_group_id=database_name,
                    external_record_id=fqn,
                    external_revision_id=str(current_time),
                    origin=OriginTypes.CONNECTOR.value,
                    connector_name=self.connector_name,
                    connector_id=self.connector_id,
                    mime_type=MimeTypes.SQL_TABLE.value,
                    weburl=self._build_dashboard_url(database_name, table.name),
                    source_created_at=existing_record.source_created_at if hasattr(existing_record, 'source_created_at') else current_time,
                    source_updated_at=current_time,
                    database_name=database_name,
                    row_count=table.row_count,
                    size_bytes=table.total_bytes,
                    column_count=len(table.columns),
                    primary_keys=table.primary_keys,
                    comment=table.comment,
                    version=(existing_record.version or 1) + 1,
                    inherit_permissions=True,
                )

                if self.indexing_filters and not self.indexing_filters.is_enabled(IndexingFilterKey.TABLES.value):
                    updated_record.indexing_status = IndexingStatus.AUTO_INDEX_OFF.value

                await self.data_entities_processor.on_record_content_update(updated_record)
                self.logger.debug(f"Published content update for table: {fqn}")

            except Exception as e:
                self.logger.error(f"Error syncing updated table {table.name}: {e}", exc_info=True)
                continue

        self.logger.info(f"Completed syncing {len(tables)} updated tables in {database_name}")

    # ── View sync methods ──────────────────────────────────────────────

    def _fetch_view_definition(self, database: str, view: str) -> str:
        """Fetch view definition via SHOW CREATE TABLE (works for views in ClickHouse)."""
        response = self.data_source.get_table_ddl(database=database, table=view)
        if response.success:
            return str(response.data.get("ddl", ""))
        return ""

    async def _process_views_generator(
        self,
        database_name: str,
        views: List[ClickHouseTable],
    ) -> AsyncGenerator[Tuple[Record, List[Permission]], None]:
        """Yield (SQLViewRecord, permissions) for each view."""
        for view in views:
            try:
                fqn = view.fqn
                record_id = str(uuid.uuid4())
                self._record_id_cache[fqn] = record_id

                weburl = self._build_dashboard_url(database_name, view.name)

                definition = await asyncio.to_thread(
                    self._fetch_view_definition, database_name, view.name
                )

                current_time = get_epoch_timestamp_in_ms()
                record = SQLViewRecord(
                    id=record_id,
                    record_name=view.name,
                    record_type=RecordType.SQL_VIEW,
                    record_group_type=RecordGroupType.SQL_DATABASE.value,
                    external_record_group_id=database_name,
                    external_record_id=fqn,
                    external_revision_id=str(current_time),
                    origin=OriginTypes.CONNECTOR.value,
                    connector_name=self.connector_name,
                    connector_id=self.connector_id,
                    mime_type=MimeTypes.SQL_VIEW.value,
                    weburl=weburl,
                    source_created_at=current_time,
                    source_updated_at=current_time,
                    database_name=database_name,
                    fqn=fqn,
                    definition=definition,
                    is_secure=False,
                    comment=view.comment,
                    version=1,
                    inherit_permissions=True,
                )

                yield (record, [])
                await asyncio.sleep(0)

            except Exception as e:
                self.logger.error(f"Error processing view {view.name}: {e}", exc_info=True)
                continue

    async def _sync_views(self, database_name: str, views: List[ClickHouseTable]) -> None:
        """Batch-process views and call on_new_records."""
        if not views:
            return

        batch: List[Tuple[Record, List[Permission]]] = []
        total_synced = 0

        async for record, perms in self._process_views_generator(database_name, views):
            batch.append((record, perms))
            total_synced += 1

            if len(batch) >= self.batch_size:
                self.logger.debug(f"Processing batch of {len(batch)} views")
                await self.data_entities_processor.on_new_records(batch)
                batch = []

        if batch:
            await self.data_entities_processor.on_new_records(batch)

        self.logger.info(f"Synced {total_synced} views in {database_name}")

    async def _sync_updated_views(self, database_name: str, views: List[ClickHouseTable]) -> None:
        """Sync views whose content or schema has changed."""
        if not views:
            return

        self.logger.info(f"Processing {len(views)} updated views in {database_name}")

        for view in views:
            try:
                fqn = view.fqn

                existing_record = await self.data_entities_processor.get_record_by_external_id(
                    connector_id=self.connector_id,
                    external_record_id=fqn
                )
                if not existing_record:
                    self.logger.warning(f"No existing record found for updated view {fqn}, skipping")
                    continue

                definition = await asyncio.to_thread(
                    self._fetch_view_definition, database_name, view.name
                )

                current_time = get_epoch_timestamp_in_ms()

                updated_record = SQLViewRecord(
                    id=existing_record.id,
                    record_name=view.name,
                    record_type=RecordType.SQL_VIEW,
                    record_group_type=RecordGroupType.SQL_DATABASE.value,
                    external_record_group_id=database_name,
                    external_record_id=fqn,
                    external_revision_id=str(current_time),
                    origin=OriginTypes.CONNECTOR.value,
                    connector_name=self.connector_name,
                    connector_id=self.connector_id,
                    mime_type=MimeTypes.SQL_VIEW.value,
                    weburl=self._build_dashboard_url(database_name, view.name),
                    source_created_at=existing_record.source_created_at if hasattr(existing_record, 'source_created_at') else current_time,
                    source_updated_at=current_time,
                    database_name=database_name,
                    fqn=fqn,
                    definition=definition,
                    is_secure=False,
                    comment=view.comment,
                    version=(existing_record.version or 1) + 1,
                    inherit_permissions=True,
                )

                await self.data_entities_processor.on_record_content_update(updated_record)
                self.logger.debug(f"Published content update for view: {fqn}")

            except Exception as e:
                self.logger.error(f"Error syncing updated view {view.name}: {e}", exc_info=True)
                continue

        self.logger.info(f"Completed syncing {len(views)} updated views in {database_name}")

    # ── Full sync ───────────────────────────────────────────────────────

    async def run_sync(self) -> None:
        try:
            self.logger.info("[Sync] Starting ClickHouse sync...")

            if not self.data_source:
                raise ConnectionError("ClickHouse connector not initialized")

            self.sync_filters, self.indexing_filters = await load_connector_filters(
                self.config_service, "clickhouse", self.connector_id, self.logger
            )

            self.sync_stats = SyncStats()

            sync_point_key = "clickhouse_tables_state"
            stored_state = await self.tables_sync_point.read_sync_point(sync_point_key)

            if stored_state and stored_state.get("table_states"):
                self.logger.info("[Sync] Found existing sync state, running incremental sync...")
                await self.run_incremental_sync()
            else:
                self.logger.info("[Sync] No existing sync state, running full sync...")
                await self._run_full_sync_internal()

            self.sync_stats.log_summary(self.logger)

        except Exception as e:
            self.logger.error(f"[Sync] Error: {e}", exc_info=True)
            raise

    async def _run_full_sync_internal(self) -> None:
        try:
            self.logger.info("[Full Sync] Starting full sync...")
            self._record_id_cache.clear()

            await self._create_app_users()

            selected_databases, selected_tables, selected_views = self._get_filter_values()

            databases = await asyncio.to_thread(self._fetch_databases)

            if selected_databases:
                databases = [d for d in databases if d.name in selected_databases]

            await self._sync_databases(databases)
            self.sync_stats.databases_synced = len(databases)

            for db in databases:
                tables = await asyncio.to_thread(self._fetch_tables, db.name)
                views = await asyncio.to_thread(self._fetch_views, db.name)

                if selected_tables:
                    tables = [t for t in tables if t.fqn in selected_tables]
                if selected_views:
                    views = [v for v in views if v.fqn in selected_views]

                await self._sync_tables(db.name, tables)
                self.sync_stats.tables_new += len(tables)

                await self._sync_views(db.name, views)
                self.sync_stats.views_new += len(views)

            await self._save_tables_sync_state("clickhouse_tables_state")

            self.logger.info("[Full Sync] ClickHouse full sync completed")
        except Exception as e:
            self.sync_stats.errors += 1
            self.logger.error(f"[Full Sync] Error: {e}", exc_info=True)
            raise

    # ── Incremental sync ────────────────────────────────────────────────

    async def run_incremental_sync(self) -> None:
        """Run incremental sync comparing stored vs current table states."""
        self.logger.info("[Incremental Sync] Starting ClickHouse incremental sync...")

        if not self.data_source:
            raise ConnectionError("ClickHouse connector not initialized")

        self.sync_filters, self.indexing_filters = await load_connector_filters(
            self.config_service, "clickhouse", self.connector_id, self.logger
        )

        try:
            sync_point_key = "clickhouse_tables_state"
            stored_state = await self.tables_sync_point.read_sync_point(sync_point_key)

            if not stored_state or not stored_state.get("table_states"):
                self.logger.info("No previous sync state found, running full sync")
                await self._run_full_sync_internal()
                return

            stored_table_states: Dict[str, Dict[str, Any]] = json.loads(
                stored_state.get("table_states", "{}")
            )

            selected_databases, selected_tables, selected_views = self._get_filter_values()
            current_stats = await asyncio.to_thread(self._get_current_table_states, selected_databases, selected_tables, selected_views)

            current_fqns = set(current_stats.keys())
            stored_fqns = set(stored_table_states.keys())

            new_tables = list(current_fqns - stored_fqns)
            deleted_tables = list(stored_fqns - current_fqns)

            changed_tables: List[str] = []
            for fqn in current_fqns & stored_fqns:
                current = current_stats[fqn]
                stored = stored_table_states[fqn]
                if self._has_table_changed(current, stored):
                    changed_tables.append(fqn)

            self.logger.info(
                f"Change detection: new={len(new_tables)}, "
                f"changed={len(changed_tables)}, deleted={len(deleted_tables)}"
            )

            if new_tables:
                await self._sync_new_tables(new_tables)
            if changed_tables:
                await self._sync_changed_tables(changed_tables)
            if deleted_tables:
                await self._handle_deleted_tables(deleted_tables)

            await self._save_tables_sync_state(sync_point_key)
            self.logger.info("[Incremental Sync] ClickHouse incremental sync completed")

        except Exception as e:
            self.logger.error(f"[Incremental Sync] Error: {e}", exc_info=True)
            raise

    def _get_current_table_states(
        self,
        selected_databases: Optional[List[str]],
        selected_tables: Optional[List[str]],
        selected_views: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, Any]]:
        """Fetch current table and view states from ClickHouse for change detection."""
        table_states: Dict[str, Dict[str, Any]] = {}

        db_response = self.data_source.list_databases()
        if not db_response.success:
            self.logger.warning(f"Failed to get databases: {db_response.error}")
            return table_states

        for db in (db_response.data or []):
            db_name = db["name"]
            if selected_databases and db_name not in selected_databases:
                continue

            # Fetch tables for this database
            tbl_response = self.data_source.list_tables(database=db_name)
            if tbl_response.success:
                for row_dict in (tbl_response.data or []):
                    table_name = row_dict["name"]
                    fqn = f"{db_name}.{table_name}"
                    if selected_tables and fqn not in selected_tables:
                        continue
                    column_hash = self._compute_column_hash(db_name, table_name)
                    table_states[fqn] = {
                        "engine": row_dict.get("engine", ""),
                        "column_hash": column_hash,
                        "total_rows": row_dict.get("total_rows", 0) or 0,
                        "total_bytes": row_dict.get("total_bytes", 0) or 0,
                        "metadata_modification_time": str(row_dict.get("metadata_modification_time", "")),
                    }

            # Fetch views for this database
            view_response = self.data_source.list_views(database=db_name)
            if view_response.success:
                for row_dict in (view_response.data or []):
                    view_name = row_dict["name"]
                    fqn = f"{db_name}.{view_name}"
                    if selected_views and fqn not in selected_views:
                        continue
                    column_hash = self._compute_column_hash(db_name, view_name)
                    table_states[fqn] = {
                        "engine": row_dict.get("engine", ""),
                        "column_hash": column_hash,
                        "total_rows": 0,
                        "total_bytes": 0,
                        "metadata_modification_time": str(row_dict.get("metadata_modification_time", "")),
                    }

        return table_states

    def _compute_column_hash(self, database: str, table: str) -> str:
        """Compute MD5 hash of column definitions for schema change detection."""
        columns = self._fetch_columns(database, table)
        if not columns:
            return ""
        column_str = json.dumps(columns, sort_keys=True, default=str)
        return hashlib.md5(column_str.encode()).hexdigest()

    def _has_table_changed(
        self,
        current: Dict[str, Any],
        stored: Dict[str, Any]
    ) -> bool:
        """Check if table has changed by comparing metadata."""
        if current.get("column_hash") != stored.get("column_hash"):
            return True

        if str(current.get("metadata_modification_time", "")) != str(stored.get("metadata_modification_time", "")):
            return True

        if current.get("total_rows", 0) != stored.get("total_rows", 0):
            return True

        if current.get("total_bytes", 0) != stored.get("total_bytes", 0):
            return True

        return False

    async def _sync_new_tables(self, table_fqns: List[str]) -> None:
        """Sync newly discovered tables and views."""
        self.logger.info(f"Syncing {len(table_fqns)} new tables/views")

        # Ensure parent database record groups exist
        new_databases = set()
        for fqn in table_fqns:
            db_name = fqn.split(".", 1)[0]
            new_databases.add(db_name)

        if new_databases:
            dbs = [ClickHouseDatabase(name=d) for d in new_databases]
            await self._sync_databases(dbs)

        # Group FQNs by database, then fetch all objects once per database
        fqns_by_db: Dict[str, List[str]] = {}
        for fqn in table_fqns:
            db_name, table_name = fqn.split(".", 1)
            fqns_by_db.setdefault(db_name, []).append(table_name)

        for db_name, table_names in fqns_by_db.items():
            target_names = set(table_names)

            tables = await asyncio.to_thread(self._fetch_tables, db_name)
            views = await asyncio.to_thread(self._fetch_views, db_name)

            new_tables = [t for t in tables if t.name in target_names]
            new_views = [v for v in views if v.name in target_names]

            if new_tables:
                await self._sync_tables(db_name, new_tables)
                self.sync_stats.tables_new += len(new_tables)
            if new_views:
                await self._sync_views(db_name, new_views)
                self.sync_stats.views_new += len(new_views)

    async def _sync_changed_tables(self, table_fqns: List[str]) -> None:
        """Sync tables/views whose content or schema has changed."""
        self.logger.info(f"Syncing {len(table_fqns)} changed tables/views")

        fqns_by_db: Dict[str, List[str]] = {}
        for fqn in table_fqns:
            db_name, table_name = fqn.split(".", 1)
            fqns_by_db.setdefault(db_name, []).append(table_name)

        for db_name, table_names in fqns_by_db.items():
            target_names = set(table_names)

            tables = await asyncio.to_thread(self._fetch_tables, db_name)
            views = await asyncio.to_thread(self._fetch_views, db_name)

            changed_tables = [t for t in tables if t.name in target_names]
            changed_views = [v for v in views if v.name in target_names]

            if changed_tables:
                await self._sync_updated_tables(db_name, changed_tables)
                self.sync_stats.tables_updated += len(changed_tables)
            if changed_views:
                await self._sync_updated_views(db_name, changed_views)
                self.sync_stats.views_updated += len(changed_views)

    async def _handle_deleted_tables(self, table_fqns: List[str]) -> None:
        """Handle tables that no longer exist in the database."""
        self.logger.info(f"Handling {len(table_fqns)} deleted tables")

        for fqn in table_fqns:
            try:
                record = await self.data_entities_processor.get_record_by_external_id(
                    connector_id=self.connector_id,
                    external_record_id=fqn
                )
                if record and record.id:
                    await self.data_entities_processor.on_record_deleted(record.id)
                    self.logger.debug(f"Deleted record for table: {fqn}")
            except Exception as e:
                self.logger.warning(f"Failed to delete record for {fqn}: {e}")

    async def _save_tables_sync_state(self, sync_point_key: str) -> None:
        """Save current table states for next incremental sync comparison."""
        selected_databases, selected_tables, selected_views = self._get_filter_values()
        current_states = await asyncio.to_thread(self._get_current_table_states, selected_databases, selected_tables, selected_views)
        count = len(current_states)
        current_states_json = json.dumps(current_states, default=str)
        await self.tables_sync_point.update_sync_point(
            sync_point_key,
            {
                "last_sync_time": get_epoch_timestamp_in_ms(),
                "table_states": current_states_json,
            }
        )
        self.logger.debug(f"Saved sync state for {count} tables")

    # ── Stream record ───────────────────────────────────────────────────

    async def stream_record(
        self,
        record: Record,
        user_id: Optional[str] = None,
        convertTo: Optional[str] = None
    ) -> StreamingResponse:
        try:
            if not self.data_source:
                raise HTTPException(status_code=500, detail="ClickHouse data source not initialized")

            if record.record_type == RecordType.SQL_TABLE:
                parts = record.external_record_id.split(".")
                if len(parts) != 2:
                    raise HTTPException(status_code=500, detail="Invalid table FQN")
                database, table = parts[0], parts[1]

                columns = await asyncio.to_thread(self._fetch_columns, database, table)
                self.logger.info(f"Retrieved {len(columns)} columns for {database}.{table}")

                rows = await asyncio.to_thread(self._fetch_table_rows, database, table)

                # Fetch DDL
                ddl = ""
                ddl_response = await asyncio.to_thread(
                    self.data_source.get_table_ddl,
                    database=database,
                    table=table
                )
                if ddl_response.success:
                    ddl = str(ddl_response.data.get("ddl", ""))

                primary_keys = [c["name"] for c in columns if c.get("is_in_primary_key")]
                order_by = [c["name"] for c in columns if c.get("is_in_sorting_key")]

                data = {
                    "table_name": table,
                    "database_name": database,
                    "columns": columns,
                    "rows": rows,
                    "primary_keys": primary_keys,
                    "order_by": order_by,
                    "ddl": ddl,
                    "connector_name": self.connector_name.value if hasattr(self.connector_name, "value") else str(self.connector_name),
                }

                json_bytes = json.dumps(data, default=str).encode("utf-8")

                async def json_iterator():
                    yield json_bytes

                return create_stream_record_response(
                    json_iterator(), filename=f"{table}.json", mime_type=MimeTypes.SQL_TABLE.value
                )

            elif record.record_type == RecordType.SQL_VIEW:
                parts = record.external_record_id.split(".")
                if len(parts) != 2:
                    raise HTTPException(status_code=500, detail="Invalid view FQN")
                database, view_name = parts[0], parts[1]

                columns = await asyncio.to_thread(self._fetch_columns, database, view_name)
                self.logger.info(f"Retrieved {len(columns)} columns for view {database}.{view_name}")

                # Fetch DDL (also serves as view definition)
                ddl = ""
                definition = ""
                ddl_response = await asyncio.to_thread(
                    self.data_source.get_table_ddl,
                    database=database,
                    table=view_name
                )
                if ddl_response.success:
                    ddl = str(ddl_response.data.get("ddl", ""))
                    definition = ddl

                # Determine engine to decide whether to fetch sample rows
                # MaterializedView stores data, so we can fetch rows; View is a proxy
                rows = []
                engine = ""
                constraints_response = await asyncio.to_thread(
                    self.data_source.get_table_constraints,
                    database=database,
                    table=view_name
                )
                if constraints_response.success:
                    table_info = constraints_response.data.get("table_info", {})
                    engine = table_info.get("engine", "")

                if engine == "MaterializedView":
                    rows = await asyncio.to_thread(self._fetch_table_rows, database, view_name)

                data = {
                    "view_name": view_name,
                    "database_name": database,
                    "engine": engine,
                    "definition": definition,
                    "columns": columns,
                    "rows": rows,
                    "ddl": ddl,
                    "connector_name": self.connector_name.value if hasattr(self.connector_name, "value") else str(self.connector_name),
                }

                json_bytes = json.dumps(data, default=str).encode("utf-8")

                async def view_json_iterator():
                    yield json_bytes

                return create_stream_record_response(
                    view_json_iterator(), filename=f"{view_name}.json", mime_type=MimeTypes.SQL_VIEW.value
                )

            raise HTTPException(status_code=400, detail="Unsupported record type")

        except Exception as e:
            self.logger.error(f"Error streaming record: {e}", exc_info=True)
            raise

    # ── Connection / lifecycle ──────────────────────────────────────────

    async def test_connection_and_access(self) -> bool:
        if not self.data_source:
            return False
        try:
            response = await asyncio.to_thread(self.data_source.ping)
            if response.success:
                self.logger.info("ClickHouse connection test successful")
                return True
            self.logger.error(f"Connection test failed: {response.error}")
            return False
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}", exc_info=True)
            return False

    async def cleanup(self) -> None:
        try:
            self.logger.info("Starting ClickHouse connector cleanup...")

            if self.data_source:
                self.data_source.close()
                self.data_source = None

            self._record_id_cache.clear()
            self.database_name = None

            self.logger.info("ClickHouse connector cleanup completed")

        except Exception as e:
            self.logger.error(f"Error during ClickHouse connector cleanup: {e}", exc_info=True)

    def get_signed_url(self, record: Record) -> Optional[str]:
        return None

    def handle_webhook_notification(self, notification: Dict) -> None:
        raise NotImplementedError(
            "ClickHouse does not support webhook notifications. "
            "Use scheduled sync for change tracking."
        )

    async def reindex_records(self, records: List[Record]) -> None:
        try:
            if not records:
                self.logger.info("No records to reindex")
                return

            self.logger.info(f"Starting reindex for {len(records)} ClickHouse records")

            if not self.data_source:
                raise Exception("ClickHouse data source not initialized")

            await self.data_entities_processor.reindex_existing_records(records)
            self.logger.info(f"Published reindex events for {len(records)} records")

        except Exception as e:
            self.logger.error(f"Error during ClickHouse reindex: {e}", exc_info=True)
            raise

    @classmethod
    async def create_connector(
        cls,
        logger: Logger,
        data_store_provider: DataStoreProvider,
        config_service: ConfigurationService,
        connector_id: str,
        **kwargs,
    ) -> "ClickHouseConnector":
        """Factory method to create a ClickHouse connector instance."""
        data_entities_processor = DataSourceEntitiesProcessor(
            logger, data_store_provider, config_service
        )
        await data_entities_processor.initialize()
        return cls(
            logger,
            data_entities_processor,
            data_store_provider,
            config_service,
            connector_id,
        )

    # ── Filter options ──────────────────────────────────────────────────

    async def get_filter_options(
        self,
        filter_key: str,
        page: int = 1,
        limit: int = 20,
        search: Optional[str] = None,
        cursor: Optional[str] = None
    ) -> FilterOptionsResponse:
        if cursor and cursor.isdigit():
            page = int(cursor)

        try:
            if filter_key == "databases":
                return await self._get_database_options(page, limit, search)
            elif filter_key == "tables":
                return await self._get_table_options(page, limit, search)
            elif filter_key == "views":
                return await self._get_view_options(page, limit, search)
            else:
                return FilterOptionsResponse(
                    success=False,
                    options=[],
                    page=page,
                    limit=limit,
                    has_more=False,
                    message=f"Unknown filter key: {filter_key}"
                )
        except Exception as e:
            self.logger.error(f"Error getting filter options for {filter_key}: {e}", exc_info=True)
            return FilterOptionsResponse(
                success=False,
                options=[],
                page=page,
                limit=limit,
                has_more=False,
                message=str(e)
            )

    async def _get_database_options(
        self,
        page: int,
        limit: int,
        search: Optional[str] = None
    ) -> FilterOptionsResponse:
        if not self.data_source:
            return FilterOptionsResponse(
                success=False,
                options=[],
                page=page,
                limit=limit,
                has_more=False,
                message="ClickHouse data source not initialized"
            )

        try:
            response = self.data_source.list_databases()

            if not response.success:
                return FilterOptionsResponse(
                    success=False,
                    options=[],
                    page=page,
                    limit=limit,
                    has_more=False,
                    message=response.error or "Failed to fetch databases"
                )

            databases = [db["name"] for db in (response.data or [])]

            if search:
                databases = [d for d in databases if search.lower() in d.lower()]

            start = (page - 1) * limit
            end = start + limit
            paginated = databases[start:end]

            options = [FilterOption(id=name, label=name) for name in paginated]

            return FilterOptionsResponse(
                success=True,
                options=options,
                page=page,
                limit=limit,
                has_more=end < len(databases)
            )

        except Exception as e:
            self.logger.error(f"Error getting database options: {e}", exc_info=True)
            return FilterOptionsResponse(
                success=False,
                options=[],
                page=page,
                limit=limit,
                has_more=False,
                message=str(e)
            )

    async def _get_table_options(
        self,
        page: int,
        limit: int,
        search: Optional[str] = None
    ) -> FilterOptionsResponse:
        if not self.data_source:
            return FilterOptionsResponse(
                success=False,
                options=[],
                page=page,
                limit=limit,
                has_more=False,
                message="ClickHouse data source not initialized"
            )

        try:
            db_response = self.data_source.list_databases()
            if not db_response.success:
                return FilterOptionsResponse(
                    success=False,
                    options=[],
                    page=page,
                    limit=limit,
                    has_more=False,
                    message=db_response.error or "Failed to fetch databases"
                )

            all_tables = []
            for db in (db_response.data or []):
                db_name = db["name"]
                tbl_response = self.data_source.list_tables(database=db_name)
                if tbl_response.success:
                    for t in (tbl_response.data or []):
                        all_tables.append(f"{db_name}.{t['name']}")

            if search:
                all_tables = [t for t in all_tables if search.lower() in t.lower()]

            start = (page - 1) * limit
            end = start + limit
            paginated = all_tables[start:end]

            options = [FilterOption(id=fqn, label=fqn) for fqn in paginated]

            return FilterOptionsResponse(
                success=True,
                options=options,
                page=page,
                limit=limit,
                has_more=end < len(all_tables)
            )

        except Exception as e:
            self.logger.error(f"Error getting table options: {e}", exc_info=True)
            return FilterOptionsResponse(
                success=False,
                options=[],
                page=page,
                limit=limit,
                has_more=False,
                message=str(e)
            )

    async def _get_view_options(
        self,
        page: int,
        limit: int,
        search: Optional[str] = None
    ) -> FilterOptionsResponse:
        if not self.data_source:
            return FilterOptionsResponse(
                success=False,
                options=[],
                page=page,
                limit=limit,
                has_more=False,
                message="ClickHouse data source not initialized"
            )

        try:
            db_response = self.data_source.list_databases()
            if not db_response.success:
                return FilterOptionsResponse(
                    success=False,
                    options=[],
                    page=page,
                    limit=limit,
                    has_more=False,
                    message=db_response.error or "Failed to fetch databases"
                )

            all_views = []
            for db in (db_response.data or []):
                db_name = db["name"]
                view_response = self.data_source.list_views(database=db_name)
                if view_response.success:
                    for v in (view_response.data or []):
                        all_views.append(f"{db_name}.{v['name']}")

            if search:
                all_views = [v for v in all_views if search.lower() in v.lower()]

            start = (page - 1) * limit
            end = start + limit
            paginated = all_views[start:end]

            options = [FilterOption(id=fqn, label=fqn) for fqn in paginated]

            return FilterOptionsResponse(
                success=True,
                options=options,
                page=page,
                limit=limit,
                has_more=end < len(all_views)
            )

        except Exception as e:
            self.logger.error(f"Error getting view options: {e}", exc_info=True)
            return FilterOptionsResponse(
                success=False,
                options=[],
                page=page,
                limit=limit,
                has_more=False,
                message=str(e)
            )
