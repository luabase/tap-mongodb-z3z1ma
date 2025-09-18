"""MongoDB tap class."""
from __future__ import annotations

import os
import sys
import atexit

import orjson
import genson
import singer_sdk.singerlib.messages    
import singer_sdk.helpers._typing
from pathlib import Path
from typing import Any, Optional
import yaml
from pymongo.mongo_client import MongoClient
from singer_sdk import Stream, Tap
from singer_sdk import typing as th
from singer_sdk.singerlib.catalog import Catalog, CatalogEntry
from sshtunnel import SSHTunnelForwarder
from tap_mongodb.utils import guess_key_type
from tap_mongodb.collection import CollectionStream, MockCollection


_BLANK = ""
"""A sentinel value to represent a blank value in the config."""

# Monkey patch the singer lib to use orjson
singer_sdk.singerlib.messages.format_message = lambda message: orjson.dumps(
    message.to_dict(), default=lambda o: str(o), option=orjson.OPT_OMIT_MICROSECONDS
).decode("utf-8")


def noop(*args, **kwargs) -> None:
    """No-op function to silence the warning about unmapped properties."""
    pass


# Monkey patch the singer lib to silence the warning about unmapped properties
singer_sdk.helpers._typing._warn_unmapped_properties = noop


def recursively_drop_required(schema: dict) -> None:
    """Recursively drop the required property from a schema.

    This is used to clean up genson generated schemas which are strict by default."""
    schema.pop("required", None)
    if "properties" in schema:
        for prop in schema["properties"]:
            if schema["properties"][prop].get("type") == "object":
                recursively_drop_required(schema["properties"][prop])


class TapMongoDB(Tap):
    """MongoDB tap class."""
    name = "tap-mongodb"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._ssh_tunnel: Optional[SSHTunnelForwarder] = None
        self._mongo_client: Optional[MongoClient] = None
        self.ssh_config = self.config.get("ssh_tunnel", {})
        atexit.register(self._cleanup_resources)

        if self._get_ssh_enabled():
            assert (
                self.ssh_config.get("host") is not None
            )
            assert (
                self.ssh_config.get("username") is not None
            )
            assert (
                self.ssh_config.get("port") is not None
            )
            assert (
                self.ssh_config.get("private_key") is not None
            )

    config_jsonschema = th.PropertiesList(
        th.Property(
            "mongo",
            th.ObjectType(),
            description=(
                "These props are passed directly to pymongo MongoClient allowing the "
                "tap user full flexibility not provided in other Mongo taps since every kwarg "
                "can be tuned."
            ),
            required=True,
        ),
        th.Property(
            "mongo_file_location",
            th.StringType,
            description=(
                "Optional file path, useful if reading mongo configuration from a file."
            ),
            default=_BLANK,
        ),
        th.Property(
            "stream_prefix",
            th.StringType,
            description=(
                "Optionally add a prefix for all streams, useful if ingesting from multiple"
                " shards/clusters via independent tap-mongodb configs. This is applied during"
                " catalog generation. Regenerate the catalog to apply a new stream prefix."
            ),
            default=_BLANK,
        ),
        th.Property(
            "optional_replication_key",
            th.BooleanType,
            description=(
                "This setting allows the tap to continue processing if a document is"
                " missing the replication key. Useful if a very small percentage of documents"
                " are missing the property."
            ),
            default=False,
        ),
        th.Property(
            "database_includes",
            th.ArrayType(th.StringType),
            description=(
                "A list of databases to include. If this list is empty, all databases"
                " will be included."
            ),
        ),
        th.Property(
            "database_excludes",
            th.ArrayType(th.StringType),
            description=(
                "A list of databases to exclude. If this list is empty, no databases"
                " will be excluded."
            ),
        ),
        th.Property(
            "strategy",
            th.StringType,
            description=(
                "The strategy to use for schema resolution. Defaults to 'raw'. The 'raw' strategy"
                " uses a relaxed schema using additionalProperties: true to accept the document"
                " as-is leaving the target to respect it. Useful for blob or jsonl. The 'envelope'"
                " strategy will envelope the document under a key named `document`. The target"
                " should use a variant type for this key. The 'infer' strategy will infer the"
                " schema from the data based on a configurable number of documents."
            ),
            default="raw",
            allowed_values=["raw", "envelope", "infer"],
        ),
        th.Property(
            "infer_schema_max_docs",
            th.IntegerType,
            description=(
                "The maximum number of documents to sample when inferring the schema."
                " This is only used when infer_schema is true."
            ),
            default=2_000,
        ),
        th.Property(
            "ssh_tunnel",
            th.ObjectType(
                th.Property(
                    "enable",
                    th.OneOf(th.BooleanType, th.StringType),
                    description="Enable SSH tunnel for MongoDB connection.",
                    default=False,
                ),
                th.Property(
                    "private_key",
                    th.StringType,
                    description="Private key for SSH tunnel authentication.",
                    secret=True,
                ),
                th.Property(
                    "host",
                    th.StringType,
                    description="SSH tunnel host.",
                ),
                th.Property(
                    "username",
                    th.StringType,
                    description="SSH tunnel username.",
                ),
                th.Property(
                    "port",
                    th.OneOf(th.StringType, th.IntegerType),
                    description="SSH tunnel port.",
                ),
            ),
            description="SSH tunnel configuration for MongoDB connection.",
        ),
        th.Property("stream_maps", th.ObjectType()),
        th.Property("stream_map_config", th.ObjectType()),
        th.Property("batch_config", th.ObjectType()),
    ).to_dict()

    def __del__(self) -> None:
        """Clean up resources when the tap is destroyed."""
        self._cleanup_resources()

    def _cleanup_resources(self) -> None:
        """Best-effort cleanup for client and SSH tunnel."""
        try:
            self.close_mongo_client()
            self.close_ssh_tunnel()
        except Exception:
            # Suppress exceptions during interpreter shutdown
            pass

    def _get_ssh_enabled(self) -> bool:
        """Get SSH tunnel enabled status, handling both boolean and string values."""
        enable_value = self.ssh_config.get("enable", False)
        if isinstance(enable_value, str):
            return enable_value.lower() in ("true", "1", "yes", "on")
        return bool(enable_value)

    def setup_ssh_tunnel(self) -> None:
        """Set up SSH tunnel if enabled and update mongo config accordingly."""
        if not self._get_ssh_enabled():
            self.logger.info("SSH tunnel disabled, skipping setup")
            return
        
        if not hasattr(self, '_ssh_tunnel') or self._ssh_tunnel is None:
            # Determine remote Mongo target before we mutate any config
            remote_host = self.config["mongo"].get("host")
            remote_port = int(self.config["mongo"].get("port", 27017))

            if not remote_host:
                raise RuntimeError("Missing mongo.host for SSH tunnel")

            # Build SSH key and tunnel
            try:
                ssh_key = guess_key_type(self.ssh_config["private_key"])
            except Exception as e:
                self.logger.error("Invalid SSH private key: %s", e)
                raise

            bastion_host = self.ssh_config["host"]
            bastion_port = int(self.ssh_config.get("port", 22))
            bastion_user = self.ssh_config["username"]

            tunnel = SSHTunnelForwarder(
                ssh_address_or_host=(bastion_host, bastion_port),
                ssh_username=bastion_user,
                ssh_pkey=ssh_key,
                remote_bind_address=(remote_host, remote_port),
                # Use ephemeral local port then inject into mongo config
                local_bind_address=("localhost", 0),
            )

            try:
                tunnel.start()
                self._ssh_tunnel = tunnel
                local_port = tunnel.local_bind_port
                self.logger.info(
                    "SSH tunnel established localhost:%s -> %s:%s via %s",
                    local_port,
                    remote_host,
                    remote_port,
                    bastion_host,
                )

                # Update Mongo connection to point to the local forwarded port
                self.config["mongo"]["host"] = "localhost"
                self.config["mongo"]["port"] = local_port
            except Exception as e:
                # Ensure cleanup on failure
                try:
                    tunnel.stop()
                except Exception:
                    pass
                self._ssh_tunnel = None
                self.logger.error("Failed to start SSH tunnel: %s", e)
                raise

    def get_mongo_client(self) -> MongoClient:
        """Get or create a cached MongoDB client using current config."""
        if not hasattr(self, '_mongo_client') or self._mongo_client is None:
            # Ensure tunnel is established and config updated
            self.setup_ssh_tunnel()
            self._mongo_client = MongoClient(**self.get_mongo_config())
        return self._mongo_client

    def close_mongo_client(self) -> None:
        """Close the cached MongoDB client if it exists."""
        if hasattr(self, '_mongo_client') and self._mongo_client is not None:
            try:
                self._mongo_client.close()
                self.logger.info("MongoDB client closed")
            except Exception as e:
                self.logger.warning(f"Error closing MongoDB client: {e}")
            finally:
                self._mongo_client = None

    def close_ssh_tunnel(self) -> None:
        """Close the SSH tunnel if it exists."""
        if hasattr(self, '_ssh_tunnel') and self._ssh_tunnel is not None:
            try:
                self._ssh_tunnel.stop()
                self.logger.warning("SSH tunnel closed")
            except Exception as e:
                self.logger.warning(f"Error closing SSH tunnel: {e}")
            finally:
                self._ssh_tunnel = None

    def get_mongo_config(self) -> dict[str, Any]:
        mongo_file_location = self.config.get("mongo_file_location", _BLANK)

        if mongo_file_location != _BLANK:
            if Path(mongo_file_location).is_file():
                try:
                    with open(mongo_file_location) as f:
                        return yaml.safe_load(f)
                except ValueError:
                    self.logger.critical(
                        f"The YAML mongo_file_location '{mongo_file_location}' has errors"
                    )
                    sys.exit(1)

        self.setup_ssh_tunnel()
        
        mongo_config = self.config["mongo"].copy()
        
        # Add SSH-specific connection options when SSH tunnel is enabled
        if self._get_ssh_enabled():
            self.logger.info("Adding SSH-specific MongoDB connection options")
            mongo_config.update({
                "tls": True,
                "directConnection": True,
                "tlsAllowInvalidHostnames": True,
                "serverSelectionTimeoutMS": 5000
            })

        return mongo_config

    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.

        Returns:
            The tap's catalog as a dict
        """
        # Use cached catalog if available
        if hasattr(self, "_catalog_dict") and self._catalog_dict:
            return self._catalog_dict
        # Defer to passed in catalog if available
        if self.input_catalog:
            return self.input_catalog.to_dict()
        # Handle discovery in test mode
        if "TAP_MONGO_TEST_NO_DB" in os.environ:
            return {"streams": [{"tap_stream_id": "test", "stream": "test"}]}
        # If no catalog is provided, discover streams
        catalog = Catalog()
        client: MongoClient = self.get_mongo_client()
        try:
            client.server_info()
        except Exception as exc:
            self.close_mongo_client()
            self.close_ssh_tunnel()
            raise RuntimeError("Could not connect to MongoDB to generate catalog") from exc
        
        try:
            db_includes = self.config.get("database_includes", [])
            db_excludes = self.config.get("database_excludes", [])
            for db_name in client.list_database_names():
                if db_includes and db_name not in db_includes:
                    continue
                if db_excludes and db_name in db_excludes:
                    continue
                try:
                    collections = client[db_name].list_collection_names()
                except Exception:
                    # Skip databases that are not accessible by the authenticated user
                    # This is a common case when using a shared cluster
                    # https://docs.mongodb.com/manual/core/security-users/#database-user-privileges
                    # TODO: vet the list of exceptions that can be raised here to be more explicit
                    self.logger.debug(
                        "Skipping database %s, authenticated user does not have permission to access",
                        db_name,
                    )
                    continue
                for collection in collections:
                    try:
                        client[db_name][collection].find_one()
                    except Exception:
                        # Skip collections that are not accessible by the authenticated user
                        # This is a common case when using a shared cluster
                        # https://docs.mongodb.com/manual/core/security-users/#database-user-privileges
                        # TODO: vet the list of exceptions that can be raised here to be more explicit
                        self.logger.debug(
                            (
                                "Skipping collections %s, authenticated user does not have permission"
                                " to access"
                            ),
                            db_name,
                        )
                        continue
                    self.logger.info("Discovered collection %s.%s", db_name, collection)
                    stream_prefix = self.config.get("stream_prefix", _BLANK)
                    stream_prefix += db_name.replace("-", "_").replace(".", "_")
                    # Sanitize collection name to replace problematic characters for SQL targets
                    sanitized_collection = collection.replace(".", "_").replace("-", "_")
                    stream_name = f"{stream_prefix}_{sanitized_collection}"
                    entry = CatalogEntry.from_dict({"tap_stream_id": stream_name})
                    entry.stream = stream_name
                    strategy: str | None = self.config.get("strategy")
                    if strategy == "infer":
                        builder = genson.SchemaBuilder(schema_uri=None)
                        for record in client[db_name][collection].aggregate(
                            [{"$sample": {"size": self.config.get("infer_schema_max_docs", 2_000)}}]
                        ):
                            builder.add_object(
                                orjson.loads(
                                    orjson.dumps(
                                        record,
                                        default=lambda o: str(o),
                                        option=orjson.OPT_OMIT_MICROSECONDS,
                                    ).decode("utf-8")
                                )
                            )
                        schema = builder.to_schema()
                        recursively_drop_required(schema)
                        if not schema:
                            # If the schema is empty, skip the stream
                            # this errs on the side of strictness
                            continue
                        self.logger.info("Inferred schema: %s", schema)
                    elif strategy == "envelope":
                        schema = {
                            "type": "object",
                            "properties": {
                                "_id": {
                                    "type": ["string", "null"],
                                    "description": "The document's _id",
                                },
                                "document": {
                                    "type": "object",
                                    "additionalProperties": True,
                                    "description": "The document from the collection",
                                },
                            },
                        }
                    elif strategy == "raw":
                        schema = {
                            "type": "object",
                            "additionalProperties": True,
                            "description": "The document from the collection",
                            "properties": {
                                "_id": {
                                    "type": ["string", "null"],
                                    "description": "The document's _id",
                                },
                            },
                        }
                    else:
                        raise RuntimeError(f"Unknown strategy {strategy}")
                    entry.schema = entry.schema.from_dict(schema)
                    entry.key_properties = ["_id"]
                    entry.metadata = entry.metadata.get_standard_metadata(
                        schema=schema, key_properties=["_id"]
                    )
                    entry.database = db_name
                    entry.table = collection
                    catalog.add_stream(entry)
        finally:
            # Keep the client open for subsequent steps (discovery/streaming)
            pass
        self._catalog_dict = catalog.to_dict()
        
        # Always clean up resources after catalog discovery to prevent hanging
        # Resources will be recreated if needed for streaming
        self.logger.info("Catalog discovery complete, cleaning up resources")
        self.close_mongo_client()
        self.close_ssh_tunnel()
        
        return self._catalog_dict

    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams."""
        if "TAP_MONGO_TEST_NO_DB" in os.environ:
            # This is a hack to allow the tap to be tested without a MongoDB instance
            return [
                CollectionStream(
                    tap=self,
                    name="test",
                    schema={
                        "type": "object",
                        "properties": {
                            "_id": {
                                "type": ["string", "null"],
                                "description": "The document's _id",
                            },
                        },
                        "additionalProperties": True,
                    },
                    collection=MockCollection(
                        name="test",
                        schema={},
                    ),
                )
            ]
        client: MongoClient = self.get_mongo_client()
        try:
            client.server_info()
        except Exception as e:
            self.close_mongo_client()
            self.close_ssh_tunnel()
            raise RuntimeError("Could not connect to MongoDB") from e
        
        try:
            db_includes = self.config.get("database_includes", [])
            db_excludes = self.config.get("database_excludes", [])
            for entry in self.catalog.streams:
                if entry.database in db_excludes:
                    continue
                if db_includes and entry.database not in db_includes:
                    continue
                stream = CollectionStream(
                    tap=self,
                    name=entry.tap_stream_id,
                    schema=entry.schema,
                    collection=client[entry.database][entry.table],
                )
                stream.apply_catalog(self.catalog)
                yield stream
        finally:
            # Keep the client open for streaming
            pass
