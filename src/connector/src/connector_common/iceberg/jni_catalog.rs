// Copyright 2024 RisingWave Labs
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

//! This module provide jni catalog.

#![expect(
    clippy::disallowed_types,
    reason = "construct iceberg::Error to implement the trait"
)]

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{Schema, SortOrder, TableMetadata, UnboundPartitionSpec};
use iceberg::table::Table;
use iceberg::{
    Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent, TableRequirement,
    TableUpdate,
};
use itertools::Itertools;
use jni::objects::{GlobalRef, JObject};
use risingwave_common::bail;
use risingwave_common::global_jvm::Jvm;
use risingwave_jni_core::call_method;
use risingwave_jni_core::jvm_runtime::{execute_with_jni_env, jobj_to_str};
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;

use crate::error::ConnectorResult;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct LoadTableResponse {
    pub metadata_location: Option<String>,
    pub metadata: TableMetadata,
    pub _config: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct CreateTableRequest {
    /// The name of the table.
    pub name: String,
    /// The location of the table.
    pub location: Option<String>,
    /// The schema of the table.
    pub schema: Schema,
    /// The partition spec of the table, could be None.
    pub partition_spec: Option<UnboundPartitionSpec>,
    /// The sort order of the table.
    pub write_order: Option<SortOrder>,
    /// The properties of the table.
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CommitTableRequest {
    identifier: TableIdent,
    requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct CommitTableResponse {
    metadata_location: String,
    metadata: TableMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ListNamespacesResponse {
    namespaces: Vec<NamespaceIdent>,
    next_page_token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ListTablesResponse {
    identifiers: Vec<TableIdent>,
    next_page_token: Option<String>,
}

impl From<&TableCreation> for CreateTableRequest {
    fn from(value: &TableCreation) -> Self {
        Self {
            name: value.name.clone(),
            location: value.location.clone(),
            schema: value.schema.clone(),
            partition_spec: value.partition_spec.clone(),
            write_order: value.sort_order.clone(),
            properties: value.properties.clone(),
        }
    }
}

#[derive(Debug)]
pub struct JniCatalog {
    java_catalog: GlobalRef,
    jvm: Jvm,
    file_io_props: HashMap<String, String>,
}

#[async_trait]
impl Catalog for JniCatalog {
    /// List namespaces from the catalog.
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        execute_with_jni_env(self.jvm, |env| {
            let result_json =
                call_method!(env, self.java_catalog.as_obj(), {String listNamespaces()})
                    .with_context(|| "Failed to list iceberg namespaces".to_owned())?;

            let rust_json_str = jobj_to_str(env, result_json)?;

            let resp: ListNamespacesResponse = serde_json::from_str(&rust_json_str)?;

            Ok(resp.namespaces)
        })
        .map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to list iceberg namespaces.",
            )
            .with_source(e)
        })
    }

    /// Create a new namespace inside the catalog.
    async fn create_namespace(
        &self,
        namespace: &iceberg::NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<iceberg::Namespace> {
        execute_with_jni_env(self.jvm, |env| {
            let namespace_jstr = if namespace.is_empty() {
                env.new_string("").unwrap()
            } else {
                if namespace.len() > 1 {
                    bail!("Namespace with more than one level is not supported!")
                }
                env.new_string(&namespace[0]).unwrap()
            };

            // Serialize properties to JSON to pass to Java
            let properties_json = if properties.is_empty() {
                env.new_string("").unwrap()
            } else {
                let json_str = serde_json::to_string(&properties).map_err(|e| {
                    anyhow::anyhow!("Failed to serialize namespace properties: {e}")
                })?;
                env.new_string(&json_str)?
            };

            call_method!(env, self.java_catalog.as_obj(), {void createNamespace(String, String)},
                &namespace_jstr, &properties_json)
            .with_context(|| format!("Failed to create namespace: {namespace}"))?;

            Ok(Namespace::new(namespace.clone()))
        })
        .map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to create namespace.",
            )
            .with_source(e)
        })
    }

    /// Get a namespace information from the catalog.
    async fn get_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        todo!()
    }

    /// Check if namespace exists in catalog.
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        execute_with_jni_env(self.jvm, |env| {
            let namespace_jstr = if namespace.is_empty() {
                env.new_string("").unwrap()
            } else {
                if namespace.len() > 1 {
                    bail!("Namespace with more than one level is not supported!")
                }
                env.new_string(&namespace[0]).unwrap()
            };

            let exists =
                call_method!(env, self.java_catalog.as_obj(), {boolean namespaceExists(String)},
                &namespace_jstr)
                .with_context(|| format!("Failed to check namespace exists: {namespace}"))?;

            Ok(exists)
        })
        .map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to check namespace exists.",
            )
            .with_source(e)
        })
    }

    /// Drop a namespace from the catalog.
    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<()> {
        todo!()
    }

    /// List tables from namespace.
    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        execute_with_jni_env(self.jvm, |env| {
            let namespace_jstr = if namespace.is_empty() {
                env.new_string("").unwrap()
            } else {
                if namespace.len() > 1 {
                    bail!("Namespace with more than one level is not supported!")
                }
                env.new_string(&namespace[0]).unwrap()
            };

            let result_json =
                call_method!(env, self.java_catalog.as_obj(), {String listTables(String)},
                &namespace_jstr)
                .with_context(|| {
                    format!("Failed to list iceberg tables in namespace: {}", namespace)
                })?;

            let rust_json_str = jobj_to_str(env, result_json)?;

            let resp: ListTablesResponse = serde_json::from_str(&rust_json_str)?;

            Ok(resp.identifiers)
        })
        .map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to list iceberg  tables.",
            )
            .with_source(e)
        })
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        todo!()
    }

    /// Create a new table inside the namespace.
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        execute_with_jni_env(self.jvm, |env| {
            let namespace_jstr = if namespace.is_empty() {
                env.new_string("").unwrap()
            } else {
                if namespace.len() > 1 {
                    bail!("Namespace with more than one level is not supported!")
                }
                env.new_string(&namespace[0]).unwrap()
            };

            let creation_str = serde_json::to_string(&CreateTableRequest::from(&creation))?;

            let creation_jstr = env.new_string(&creation_str).unwrap();

            let result_json =
                call_method!(env, self.java_catalog.as_obj(), {String createTable(String, String)},
                &namespace_jstr, &creation_jstr)
                .with_context(|| format!("Failed to create iceberg table: {}", creation.name))?;

            let rust_json_str = jobj_to_str(env, result_json)?;

            let resp: LoadTableResponse = serde_json::from_str(&rust_json_str)?;

            let metadata_location = resp.metadata_location.ok_or_else(|| {
                iceberg::Error::new(
                    iceberg::ErrorKind::FeatureUnsupported,
                    "Loading uncommitted table is not supported!",
                )
            })?;

            let table_metadata = resp.metadata;

            let file_io = FileIO::from_path(&metadata_location)?
                .with_props(self.file_io_props.iter())
                .build()?;

            Ok(Table::builder()
                .file_io(file_io)
                .identifier(TableIdent::new(namespace.clone(), creation.name))
                .metadata(table_metadata)
                .build())
        })
        .map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to create iceberg table.",
            )
            .with_source(e)
        })?
    }

    /// Load table from the catalog.
    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        execute_with_jni_env(self.jvm, |env| {
            let table_name_str = format!(
                "{}.{}",
                table.namespace().clone().inner().into_iter().join("."),
                table.name()
            );

            let table_name_jstr = env.new_string(&table_name_str).unwrap();

            let result_json =
                call_method!(env, self.java_catalog.as_obj(), {String loadTable(String)},
                &table_name_jstr)
                .with_context(|| format!("Failed to load iceberg table: {table_name_str}"))?;

            let rust_json_str = jobj_to_str(env, result_json)?;

            let resp: LoadTableResponse = serde_json::from_str(&rust_json_str)?;

            let metadata_location = resp.metadata_location.ok_or_else(|| {
                iceberg::Error::new(
                    iceberg::ErrorKind::FeatureUnsupported,
                    "Loading uncommitted table is not supported!",
                )
            })?;

            tracing::info!("Table metadata location of {table_name_str} is {metadata_location}");

            let table_metadata = resp.metadata;

            let file_io = FileIO::from_path(&metadata_location)?
                .with_props(self.file_io_props.iter())
                .build()?;

            Ok(Table::builder()
                .file_io(file_io)
                .identifier(table.clone())
                .metadata(table_metadata)
                .build())
        })
        .map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to load iceberg table.",
            )
            .with_source(e)
        })?
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        let jvm = self.jvm;
        let table = table.to_owned();
        let java_catalog = self.java_catalog.clone();
        // spawn blocking the drop table task, since dropping a table by default would purge the data which may take a long time.
        tokio::task::spawn_blocking(move || -> iceberg::Result<()> {
            execute_with_jni_env(jvm, |env| {
                let table_name_str = format!(
                    "{}.{}",
                    table.namespace().clone().inner().into_iter().join("."),
                    table.name()
                );

                let table_name_jstr = env.new_string(&table_name_str).unwrap();

                call_method!(env, java_catalog.as_obj(), {boolean dropTable(String)},
                &table_name_jstr)
                .with_context(|| format!("Failed to drop iceberg table: {table_name_str}"))?;

                Ok(())
            })
            .map_err(|e| {
                iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    "Failed to drop iceberg table.",
                )
                .with_source(e)
            })
        })
        .await
        .map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to drop iceberg table.",
            )
            .with_source(e)
        })?
    }

    async fn register_table(
        &self,
        _table_ident: &TableIdent,
        _metadata_location: String,
    ) -> iceberg::Result<Table> {
        Err(iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "register_table is not supported by JniCatalog",
        ))
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        execute_with_jni_env(self.jvm, |env| {
            let table_name_str = format!(
                "{}.{}",
                table.namespace().clone().inner().into_iter().join("."),
                table.name()
            );

            let table_name_jstr = env.new_string(&table_name_str).unwrap();

            let exists =
                call_method!(env, self.java_catalog.as_obj(), {boolean tableExists(String)},
                &table_name_jstr)
                .with_context(|| {
                    format!("Failed to check iceberg table exists: {table_name_str}")
                })?;

            Ok(exists)
        })
        .map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to check iceberg table exists.",
            )
            .with_source(e)
        })
    }

    /// Rename a table in the catalog.
    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> iceberg::Result<()> {
        todo!()
    }

    /// Update a table to the catalog.
    async fn update_table(&self, mut commit: TableCommit) -> iceberg::Result<Table> {
        execute_with_jni_env(self.jvm, |env| {
            let requirements = commit.take_requirements();
            let updates = commit.take_updates();
            let request = CommitTableRequest {
                identifier: commit.identifier().clone(),
                requirements,
                updates,
            };
            let request_str = serde_json::to_string(&request)?;

            let request_jni_str = env.new_string(&request_str).with_context(|| {
                format!("Failed to create jni string from request json: {request_str}.")
            })?;

            let result_json =
                call_method!(env, self.java_catalog.as_obj(), {String updateTable(String)},
                &request_jni_str)
                .with_context(|| {
                    format!("Failed to update iceberg table: {}", commit.identifier())
                })?;

            let rust_json_str = jobj_to_str(env, result_json)?;

            let response: CommitTableResponse = serde_json::from_str(&rust_json_str)?;

            tracing::info!(
                "Table metadata location of {} is {}",
                commit.identifier(),
                response.metadata_location
            );

            let table_metadata = response.metadata;

            let file_io = FileIO::from_path(&response.metadata_location)?
                .with_props(self.file_io_props.iter())
                .build()?;

            Ok(Table::builder()
                .file_io(file_io)
                .identifier(commit.identifier().clone())
                .metadata(table_metadata)
                .build()?)
        })
        .map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "Failed to update iceberg table.",
            )
            .with_source(e)
        })
    }
}

impl Drop for JniCatalog {
    fn drop(&mut self) {
        let _ = execute_with_jni_env(self.jvm, |env| {
            call_method!(env, self.java_catalog.as_obj(), {void close()})
                .with_context(|| "Failed to close iceberg catalog".to_owned())?;
            Ok(())
        })
        .inspect_err(
            |e| tracing::error!(error = ?e.as_report(), "Failed to close iceberg catalog"),
        );
    }
}

impl JniCatalog {
    fn build(
        file_io_props: HashMap<String, String>,
        name: impl ToString,
        catalog_impl: impl ToString,
        java_catalog_props: HashMap<String, String>,
    ) -> ConnectorResult<Self> {
        let jvm = Jvm::get_or_init()?;

        execute_with_jni_env(jvm, |env| {
            // Convert props to string array
            let props = env.new_object_array(
                (java_catalog_props.len() * 2) as i32,
                "java/lang/String",
                JObject::null(),
            )?;
            for (i, (key, value)) in java_catalog_props.iter().enumerate() {
                let key_j_str = env.new_string(key)?;
                let value_j_str = env.new_string(value)?;
                env.set_object_array_element(&props, i as i32 * 2, key_j_str)?;
                env.set_object_array_element(&props, i as i32 * 2 + 1, value_j_str)?;
            }

            let jni_catalog_wrapper = env
                .call_static_method(
                    "com/risingwave/connector/catalog/JniCatalogWrapper",
                    "create",
                    "(Ljava/lang/String;Ljava/lang/String;[Ljava/lang/String;)Lcom/risingwave/connector/catalog/JniCatalogWrapper;",
                    &[
                        (&env.new_string(name.to_string()).unwrap()).into(),
                        (&env.new_string(catalog_impl.to_string()).unwrap()).into(),
                        (&props).into(),
                    ],
                )?;

            let jni_catalog = env.new_global_ref(jni_catalog_wrapper.l().unwrap())?;

            Ok(Self {
                java_catalog: jni_catalog,
                jvm,
                file_io_props,
            })
        })
            .map_err(Into::into)
    }

    pub fn build_catalog(
        file_io_props: HashMap<String, String>,
        name: impl ToString,
        catalog_impl: impl ToString,
        java_catalog_props: HashMap<String, String>,
    ) -> ConnectorResult<Arc<dyn Catalog>> {
        let catalog = Self::build(file_io_props, name, catalog_impl, java_catalog_props)?;
        Ok(Arc::new(catalog) as Arc<dyn Catalog>)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that namespace properties can be serialized to JSON correctly.
    /// This verifies the data format that will be passed to the Java side.
    #[test]
    fn test_namespace_properties_serialization() {
        let mut properties = HashMap::new();
        properties.insert("location".to_string(), "gs://my-bucket/namespace".to_string());
        properties.insert("gcp-region".to_string(), "us".to_string());

        let json = serde_json::to_string(&properties).expect("Failed to serialize properties");

        // Verify the JSON structure
        assert!(json.contains("\"location\":\"gs://my-bucket/namespace\""));
        assert!(json.contains("\"gcp-region\":\"us\""));

        // Verify it can be deserialized back
        let deserialized: HashMap<String, String> =
            serde_json::from_str(&json).expect("Failed to deserialize properties");
        assert_eq!(deserialized.len(), 2);
        assert_eq!(deserialized.get("location"), Some(&"gs://my-bucket/namespace".to_string()));
        assert_eq!(deserialized.get("gcp-region"), Some(&"us".to_string()));
    }

    /// Test that empty properties map produces empty JSON string.
    #[test]
    fn test_empty_namespace_properties() {
        let properties: HashMap<String, String> = HashMap::new();
        let json = serde_json::to_string(&properties).expect("Failed to serialize properties");
        assert_eq!(json, "{}");

        // Verify it can be deserialized back
        let deserialized: HashMap<String, String> =
            serde_json::from_str(&json).expect("Failed to deserialize properties");
        assert!(deserialized.is_empty());
    }

    /// Test that properties with special characters are handled correctly.
    #[test]
    fn test_namespace_properties_with_special_chars() {
        let mut properties = HashMap::new();
        properties.insert("key-with-dash".to_string(), "value with spaces".to_string());
        properties.insert(
            "location".to_string(),
            "gs://my-bucket/path/with/slashes".to_string(),
        );

        let json = serde_json::to_string(&properties).expect("Failed to serialize properties");

        // Verify it can be deserialized back correctly
        let deserialized: HashMap<String, String> =
            serde_json::from_str(&json).expect("Failed to deserialize properties");
        assert_eq!(
            deserialized.get("key-with-dash"),
            Some(&"value with spaces".to_string())
        );
        assert_eq!(
            deserialized.get("location"),
            Some(&"gs://my-bucket/path/with/slashes".to_string())
        );
    }

    /// Test that BigQuery namespace properties (from the user's use case) are correctly formatted.
    #[test]
    fn test_bigquery_namespace_properties() {
        let mut properties = HashMap::new();
        properties.insert("gcp-region".to_string(), "us".to_string());
        properties.insert(
            "location".to_string(),
            "gs://rainbow-data-production-iceberg/test_namespace_bq_connection".to_string(),
        );

        let json = serde_json::to_string(&properties).expect("Failed to serialize properties");

        // Verify the JSON can be parsed by Java
        let deserialized: HashMap<String, String> =
            serde_json::from_str(&json).expect("Failed to deserialize properties");
        assert_eq!(deserialized.len(), 2);
        assert_eq!(deserialized.get("gcp-region"), Some(&"us".to_string()));
        assert_eq!(
            deserialized.get("location"),
            Some(&"gs://rainbow-data-production-iceberg/test_namespace_bq_connection".to_string())
        );
    }

    /// Test that CreateTableRequest serializes properties correctly.
    #[test]
    fn test_create_table_request_serialization() {
        use iceberg::spec::{NestedField, PrimitiveType, Type};
        use std::sync::Arc;

        // Create a simple schema
        let field = Arc::new(NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long))
            .with_doc("Test ID field"));
        let schema = Schema::builder().with_schema_id(1).with_fields(vec![field]).build().unwrap();

        // Create table properties
        let mut properties = HashMap::new();
        properties.insert("bq_connection".to_string(), "projects/my-project/locations/us/connections/iceberg_conn".to_string());
        properties.insert("format-version".to_string(), "2".to_string());

        // Create TableCreation with properties
        let table_creation = TableCreation::builder()
            .name("test_table".to_string())
            .schema(schema)
            .properties(properties.clone())
            .build();

        // Convert to CreateTableRequest and serialize
        let request = CreateTableRequest::from(&table_creation);
        let json = serde_json::to_string(&request).expect("Failed to serialize CreateTableRequest");

        // Verify properties are in the JSON
        assert!(json.contains("\"bq_connection\""));
        assert!(json.contains("projects/my-project/locations/us/connections/iceberg_conn"));
        assert!(json.contains("\"format-version\""));
        assert!(json.contains("\"2\""));

        // Verify properties are accessible from the request
        assert_eq!(request.properties.len(), 2);
        assert_eq!(
            request.properties.get("bq_connection"),
            Some(&"projects/my-project/locations/us/connections/iceberg_conn".to_string())
        );
        assert_eq!(request.properties.get("format-version"), Some(&"2".to_string()));
    }

    /// Test that empty table properties are handled correctly.
    #[test]
    fn test_create_table_request_empty_properties() {
        use iceberg::spec::{NestedField, Type, PrimitiveType};
        use std::sync::Arc;

        // Create a simple schema
        let field = Arc::new(NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long))
            .with_doc("Test ID field"));
        let schema = Schema::builder().with_schema_id(1).with_fields(vec![field]).build().unwrap();

        // Create TableCreation with empty properties
        let properties: HashMap<String, String> = HashMap::new();
        let table_creation = TableCreation::builder()
            .name("test_table".to_string())
            .schema(schema)
            .properties(properties)
            .build();

        // Convert to CreateTableRequest and serialize
        let request = CreateTableRequest::from(&table_creation);
        let json = serde_json::to_string(&request).expect("Failed to serialize CreateTableRequest");

        // Verify properties field exists but is empty object
        assert!(json.contains("\"properties\":{}"));

        // Verify properties are empty
        assert!(request.properties.is_empty());
    }
}
