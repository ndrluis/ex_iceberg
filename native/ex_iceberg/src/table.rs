use rustler::{Atom, ResourceArc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};

use crate::atoms;

// Smart Table Resource that can recreate Table instances as needed
// while maintaining RefUnwindSafe compatibility
pub struct SmartTableResource {
    // RefUnwindSafe components for recreating catalog/table
    uri: String,
    warehouse: Option<String>,
    props: HashMap<String, String>,
    namespace: String,
    table_name: String,
    runtime: Arc<Runtime>,

    // Cached metadata - only updated when explicitly invalidated
    metadata_cache: Arc<Mutex<Option<HashMap<String, String>>>>,
}

unsafe impl Send for SmartTableResource {}
unsafe impl Sync for SmartTableResource {}

#[rustler::resource_impl]
impl rustler::Resource for SmartTableResource {}

impl SmartTableResource {
    pub fn new(
        uri: String,
        warehouse: Option<String>,
        props: HashMap<String, String>,
        namespace: String,
        table_name: String,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            uri,
            warehouse,
            props,
            namespace,
            table_name,
            runtime,
            metadata_cache: Arc::new(Mutex::new(None)),
        }
    }

    fn build_table_ident(&self) -> TableIdent {
        let namespace_parts: Vec<&str> = self.namespace.split('.').collect();
        let namespace_ident =
            NamespaceIdent::from_vec(namespace_parts.iter().map(|s| s.to_string()).collect())
                .unwrap();
        TableIdent::new(namespace_ident, self.table_name.clone())
    }

    async fn get_catalog(&self) -> iceberg::Result<RestCatalog> {
        let mut props = self.props.clone();
        props.insert("uri".to_string(), self.uri.clone());
        if let Some(warehouse) = &self.warehouse {
            props.insert("warehouse".to_string(), warehouse.clone());
        }

        RestCatalogBuilder::default()
            .load("ex_iceberg", props)
            .await
    }

    fn get_table(&self) -> Result<Table, String> {
        let table_ident = self.build_table_ident();

        self.runtime
            .block_on(async {
                let catalog = self.get_catalog().await?;
                catalog.load_table(&table_ident).await
            })
            .map_err(|e| format!("Failed to load table: {}", e))
    }

    pub fn get_metadata_cached(&self) -> Result<HashMap<String, String>, String> {
        // Check cache first
        {
            let cache = self.metadata_cache.lock().unwrap();
            if let Some(metadata) = cache.as_ref() {
                return Ok(metadata.clone());
            }
        }

        // Cache miss - fetch fresh data
        let table = self.get_table()?;
        let metadata = table.metadata();

        let mut response = HashMap::new();
        response.insert("table_uuid".to_string(), metadata.uuid().to_string());
        response.insert(
            "format_version".to_string(),
            format!("{}", metadata.format_version() as u8),
        );
        response.insert("location".to_string(), metadata.location().to_string());

        // Schema information
        let schema = metadata.current_schema();
        response.insert("schema_id".to_string(), format!("{}", schema.schema_id()));

        // Convert schema fields to JSON string
        let fields_json = serde_json::to_string(
            &schema
                .as_struct()
                .fields()
                .iter()
                .map(|field| {
                    serde_json::json!({
                        "id": field.id,
                        "name": field.name,
                        "required": field.required,
                        "type": format!("{:?}", field.field_type)
                    })
                })
                .collect::<Vec<_>>(),
        )
        .unwrap_or_else(|_| "[]".to_string());

        response.insert("fields".to_string(), fields_json);

        // Properties as JSON string
        let properties = metadata.properties();
        let props_json = serde_json::to_string(properties).unwrap_or_else(|_| "{}".to_string());
        response.insert("properties".to_string(), props_json);

        // Update cache
        {
            let mut cache = self.metadata_cache.lock().unwrap();
            *cache = Some(response.clone());
        }

        Ok(response)
    }

    pub fn get_inspect_data(&self) -> Result<HashMap<String, String>, String> {
        let table = self.get_table()?;
        let metadata = table.metadata();
        let mut response = HashMap::new();

        response.insert(
            "identifier".to_string(),
            format!("{:?}", table.identifier()),
        );
        response.insert("location".to_string(), metadata.location().to_string());
        response.insert("table_uuid".to_string(), metadata.uuid().to_string());

        // Add current snapshot info if available
        if let Some(snapshot) = metadata.current_snapshot() {
            response.insert(
                "current_snapshot_id".to_string(),
                format!("{}", snapshot.snapshot_id()),
            );
            response.insert(
                "sequence_number".to_string(),
                format!("{}", snapshot.sequence_number()),
            );
        }

        Ok(response)
    }

    pub fn invalidate_cache(&self) {
        let mut cache = self.metadata_cache.lock().unwrap();
        *cache = None;
    }
}

// Table NIF functions
#[rustler::nif]
pub fn table_metadata(
    table_resource: ResourceArc<SmartTableResource>,
) -> (Atom, HashMap<String, String>) {
    match table_resource.get_metadata_cached() {
        Ok(metadata) => (atoms::ok(), metadata),
        Err(e) => {
            let mut error_response = HashMap::new();
            error_response.insert("error".to_string(), e);
            (atoms::error(), error_response)
        }
    }
}

#[rustler::nif]
pub fn table_metadata_ref(
    table_resource: ResourceArc<SmartTableResource>,
) -> (Atom, HashMap<String, String>) {
    // For now, return the same as metadata since we're focusing on basic info
    // In the future, this could return a more efficient reference
    match table_resource.get_metadata_cached() {
        Ok(metadata) => (atoms::ok(), metadata),
        Err(e) => {
            let mut error_response = HashMap::new();
            error_response.insert("error".to_string(), e);
            (atoms::error(), error_response)
        }
    }
}

#[rustler::nif]
pub fn table_inspect(
    table_resource: ResourceArc<SmartTableResource>,
) -> (Atom, HashMap<String, String>) {
    match table_resource.get_inspect_data() {
        Ok(inspect_data) => (atoms::ok(), inspect_data),
        Err(e) => {
            let mut error_response = HashMap::new();
            error_response.insert("error".to_string(), e);
            (atoms::error(), error_response)
        }
    }
}

#[rustler::nif]
pub fn table_invalidate_cache(table_resource: ResourceArc<SmartTableResource>) -> Atom {
    table_resource.invalidate_cache();
    atoms::ok()
}
