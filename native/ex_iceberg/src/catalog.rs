use rustler::{Atom, NifStruct, NifTaggedEnum, ResourceArc};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;

use iceberg::spec::{ListType, MapType, NestedField, PrimitiveType, Schema, StructType, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};

use crate::atoms;
use crate::table::SmartTableResource;
use crate::types::{IcebergField, IcebergFieldType};

// REST Catalog Resource for wrapping iceberg-rust RestCatalog
pub struct RestCatalogResource {
    uri: String,
    warehouse: Option<String>,
    props: HashMap<String, String>,
    runtime: Arc<Runtime>,
}

unsafe impl Send for RestCatalogResource {}
unsafe impl Sync for RestCatalogResource {}

#[rustler::resource_impl]
impl rustler::Resource for RestCatalogResource {}

impl RestCatalogResource {
    pub fn new(uri: String, warehouse: Option<String>, props: HashMap<String, String>) -> Self {
        Self {
            uri,
            warehouse,
            props,
            runtime: Arc::new(Runtime::new().unwrap()),
        }
    }

    pub fn get_catalog(&self) -> RestCatalog {
        let config = self.build_config();
        RestCatalog::new(config)
    }

    fn build_config(&self) -> RestCatalogConfig {
        match (&self.warehouse, self.props.is_empty()) {
            (Some(warehouse), false) => RestCatalogConfig::builder()
                .uri(self.uri.clone())
                .warehouse(warehouse.clone())
                .props(self.props.clone())
                .build(),
            (Some(warehouse), true) => RestCatalogConfig::builder()
                .uri(self.uri.clone())
                .warehouse(warehouse.clone())
                .build(),
            (None, false) => RestCatalogConfig::builder()
                .uri(self.uri.clone())
                .props(self.props.clone())
                .build(),
            (None, true) => RestCatalogConfig::builder().uri(self.uri.clone()).build(),
        }
    }
}

#[derive(NifStruct)]
#[module = "ExIceberg.Rest.CatalogConfig"]
struct CatalogConfig {
    uri: String,
    warehouse: Option<String>,
    token: Option<String>,
    credential: Option<String>,
    oauth2_server_uri: Option<String>,
    scope: Option<String>,
    audience: Option<String>,
    resource: Option<String>,
}

#[derive(NifTaggedEnum)]
enum TableResult {
    Ok(ResourceArc<SmartTableResource>),
    Error(String),
}

#[rustler::nif]
pub fn rest_catalog_new(config: CatalogConfig) -> (Atom, ResourceArc<RestCatalogResource>) {
    let mut props = HashMap::new();

    if let Some(credential) = config.credential {
        props.insert("credential".to_string(), credential);
    }

    if let Some(oauth2_server_uri) = config.oauth2_server_uri {
        props.insert("oauth2-server-uri".to_string(), oauth2_server_uri);
    }

    if let Some(scope) = config.scope {
        props.insert("scope".to_string(), scope);
    }

    if let Some(token) = config.token {
        props.insert("token".to_string(), token);
    }

    if let Some(audience) = config.audience {
        props.insert("audience".to_string(), audience);
    }

    if let Some(resource) = config.resource {
        props.insert("resource".to_string(), resource);
    }

    let catalog_resource = RestCatalogResource::new(config.uri, config.warehouse, props);

    (atoms::ok(), ResourceArc::new(catalog_resource))
}

#[rustler::nif]
pub fn rest_catalog_list_namespaces(
    catalog_resource: ResourceArc<RestCatalogResource>,
) -> (Atom, Vec<String>) {
    let runtime = catalog_resource.runtime.clone();
    let catalog = catalog_resource.get_catalog();

    let result = runtime.block_on(async { catalog.list_namespaces(None).await });

    match result {
        Ok(namespaces) => {
            let namespace_names: Vec<String> = namespaces
                .into_iter()
                .map(|ns| ns.as_ref().join("."))
                .collect();
            (atoms::ok(), namespace_names)
        }
        Err(e) => {
            let error_msg = format!("Failed to list namespaces: {}", e);
            (atoms::error(), vec![error_msg])
        }
    }
}

#[rustler::nif]
pub fn rest_catalog_create_namespace(
    catalog_resource: ResourceArc<RestCatalogResource>,
    namespace: String,
    properties: HashMap<String, String>,
) -> (Atom, HashMap<String, Vec<String>>) {
    let runtime = catalog_resource.runtime.clone();
    let catalog = catalog_resource.get_catalog();

    let namespace_parts: Vec<&str> = namespace.split('.').collect();
    let namespace_ident =
        NamespaceIdent::from_vec(namespace_parts.iter().map(|s| s.to_string()).collect()).unwrap();

    let result =
        runtime.block_on(async { catalog.create_namespace(&namespace_ident, properties).await });

    match result {
        Ok(namespace_obj) => {
            let mut response = HashMap::new();
            response.insert(
                "namespace".to_string(),
                namespace_obj.name().as_ref().to_vec(),
            );
            (atoms::ok(), response)
        }
        Err(e) => {
            let mut error_response = HashMap::new();
            error_response.insert(
                "error".to_string(),
                vec![format!("Failed to create namespace: {}", e)],
            );
            (atoms::error(), error_response)
        }
    }
}

#[rustler::nif]
pub fn rest_catalog_table_exists(
    catalog_resource: ResourceArc<RestCatalogResource>,
    namespace: String,
    table_name: String,
) -> (Atom, bool) {
    let runtime = catalog_resource.runtime.clone();
    let catalog = catalog_resource.get_catalog();

    let namespace_parts: Vec<&str> = namespace.split('.').collect();
    let namespace_ident =
        NamespaceIdent::from_vec(namespace_parts.iter().map(|s| s.to_string()).collect()).unwrap();
    let table_ident = TableIdent::new(namespace_ident, table_name);

    let result = runtime.block_on(async { catalog.table_exists(&table_ident).await });

    match result {
        Ok(exists) => (atoms::ok(), exists),
        Err(_) => (atoms::error(), false),
    }
}

#[rustler::nif]
pub fn rest_catalog_drop_table(
    catalog_resource: ResourceArc<RestCatalogResource>,
    namespace: String,
    table_name: String,
) -> (Atom, HashMap<String, String>) {
    let runtime = catalog_resource.runtime.clone();
    let catalog = catalog_resource.get_catalog();

    let namespace_parts: Vec<&str> = namespace.split('.').collect();
    let namespace_ident =
        NamespaceIdent::from_vec(namespace_parts.iter().map(|s| s.to_string()).collect()).unwrap();
    let table_name_clone = table_name.clone();
    let table_ident = TableIdent::new(namespace_ident, table_name);

    let result = runtime.block_on(async { catalog.drop_table(&table_ident).await });

    match result {
        Ok(()) => {
            let mut response = HashMap::new();
            response.insert(
                "table".to_string(),
                format!("{}.{}", namespace, table_name_clone),
            );
            (atoms::ok(), response)
        }
        Err(e) => {
            let mut error_response = HashMap::new();
            error_response.insert("error".to_string(), format!("Failed to drop table: {}", e));
            (atoms::error(), error_response)
        }
    }
}

#[rustler::nif]
pub fn rest_catalog_create_table(
    catalog_resource: ResourceArc<RestCatalogResource>,
    namespace: String,
    table_name: String,
    fields: Vec<IcebergField>,
    properties: HashMap<String, String>,
) -> TableResult {
    let runtime = catalog_resource.runtime.clone();
    let catalog = catalog_resource.get_catalog();

    // Create namespace identifier
    let namespace_parts: Vec<&str> = namespace.split('.').collect();
    let namespace_ident =
        NamespaceIdent::from_vec(namespace_parts.iter().map(|s| s.to_string()).collect()).unwrap();

    // Convert IcebergField to NestedField
    let nested_fields: Vec<Arc<NestedField>> = fields
        .into_iter()
        .enumerate()
        .map(|(index, field)| {
            let field_type = match field.field_type {
                IcebergFieldType::Boolean => Type::Primitive(PrimitiveType::Boolean),
                IcebergFieldType::Int => Type::Primitive(PrimitiveType::Int),
                IcebergFieldType::Long => Type::Primitive(PrimitiveType::Long),
                IcebergFieldType::Float => Type::Primitive(PrimitiveType::Float),
                IcebergFieldType::Double => Type::Primitive(PrimitiveType::Double),
                IcebergFieldType::String => Type::Primitive(PrimitiveType::String),
                IcebergFieldType::Uuid => Type::Primitive(PrimitiveType::Uuid),
                IcebergFieldType::Date => Type::Primitive(PrimitiveType::Date),
                IcebergFieldType::Timestamp => Type::Primitive(PrimitiveType::Timestamp),
                IcebergFieldType::Binary => Type::Primitive(PrimitiveType::Binary),
                IcebergFieldType::Decimal { precision, scale } => {
                    Type::Primitive(PrimitiveType::Decimal { precision, scale })
                }
                IcebergFieldType::Fixed { length } => {
                    Type::Primitive(PrimitiveType::Fixed(length.into()))
                }
                IcebergFieldType::List {
                    element_type,
                    element_required,
                } => {
                    let element_field_type = match *element_type {
                        IcebergFieldType::String => Type::Primitive(PrimitiveType::String),
                        IcebergFieldType::Long => Type::Primitive(PrimitiveType::Long),
                        IcebergFieldType::Int => Type::Primitive(PrimitiveType::Int),
                        _ => Type::Primitive(PrimitiveType::String), // Default fallback
                    };
                    Type::List(ListType {
                        element_field: NestedField::new(
                            index as i32 * 1000 + 1,
                            "element",
                            element_field_type,
                            element_required,
                        )
                        .into(),
                    })
                }
                IcebergFieldType::Map {
                    key_type,
                    value_type,
                    value_required,
                } => {
                    let key_field_type = match *key_type {
                        IcebergFieldType::String => Type::Primitive(PrimitiveType::String),
                        IcebergFieldType::Long => Type::Primitive(PrimitiveType::Long),
                        IcebergFieldType::Int => Type::Primitive(PrimitiveType::Int),
                        _ => Type::Primitive(PrimitiveType::String), // Default fallback
                    };
                    let value_field_type = match *value_type {
                        IcebergFieldType::String => Type::Primitive(PrimitiveType::String),
                        IcebergFieldType::Long => Type::Primitive(PrimitiveType::Long),
                        IcebergFieldType::Int => Type::Primitive(PrimitiveType::Int),
                        _ => Type::Primitive(PrimitiveType::String), // Default fallback
                    };

                    Type::Map(MapType {
                        key_field: NestedField::new(
                            index as i32 * 1000 + 1,
                            "key",
                            key_field_type,
                            true,
                        )
                        .into(),
                        value_field: NestedField::new(
                            index as i32 * 1000 + 2,
                            "value",
                            value_field_type,
                            value_required,
                        )
                        .into(),
                    })
                }
                IcebergFieldType::Struct { fields } => {
                    let struct_fields: Vec<Arc<NestedField>> = fields
                        .into_iter()
                        .enumerate()
                        .map(|(struct_index, struct_field)| {
                            let struct_field_type = match struct_field.field_type {
                                IcebergFieldType::String => Type::Primitive(PrimitiveType::String),
                                IcebergFieldType::Long => Type::Primitive(PrimitiveType::Long),
                                IcebergFieldType::Int => Type::Primitive(PrimitiveType::Int),
                                _ => Type::Primitive(PrimitiveType::String), // Default fallback
                            };
                            Arc::new(NestedField::new(
                                index as i32 * 1000 + struct_index as i32 + 10,
                                struct_field.name,
                                struct_field_type,
                                struct_field.required,
                            ))
                        })
                        .collect();

                    Type::Struct(StructType::new(struct_fields))
                }
            };

            Arc::new(NestedField::new(
                (index + 1) as i32,
                field.name,
                field_type,
                field.required,
            ))
        })
        .collect();

    // Create the schema
    let table_schema = Schema::builder()
        .with_fields(nested_fields)
        .build()
        .unwrap();

    // Create table creation spec
    let table_creation = TableCreation::builder()
        .name(table_name.clone())
        .schema(table_schema)
        .properties(properties)
        .build();

    let table_name_clone = table_name.clone();
    let result =
        runtime.block_on(async { catalog.create_table(&namespace_ident, table_creation).await });

    match result {
        Ok(_table) => {
            // Table created successfully, return SmartTableResource like load_table
            let table_resource = SmartTableResource::new(
                catalog_resource.uri.clone(),
                catalog_resource.warehouse.clone(),
                catalog_resource.props.clone(),
                namespace,
                table_name_clone,
                catalog_resource.runtime.clone(),
            );
            TableResult::Ok(ResourceArc::new(table_resource))
        }
        Err(e) => TableResult::Error(format!("Failed to create table: {}", e)),
    }
}

#[rustler::nif]
pub fn rest_catalog_load_table(
    catalog_resource: ResourceArc<RestCatalogResource>,
    namespace: String,
    table_name: String,
) -> TableResult {
    let runtime = catalog_resource.runtime.clone();
    let catalog = catalog_resource.get_catalog();
    let namespace_ident = NamespaceIdent::new(namespace.clone());
    let table_ident = TableIdent::new(namespace_ident, table_name.clone());

    // First check if table exists by trying to load it
    let load_result = runtime.block_on(async { catalog.load_table(&table_ident).await });

    match load_result {
        Ok(_table) => {
            // Table exists, create SmartTableResource
            let table_resource = SmartTableResource::new(
                catalog_resource.uri.clone(),
                catalog_resource.warehouse.clone(),
                catalog_resource.props.clone(),
                namespace,
                table_name,
                catalog_resource.runtime.clone(),
            );
            TableResult::Ok(ResourceArc::new(table_resource))
        }
        Err(e) => TableResult::Error(format!("Failed to load table: {}", e)),
    }
}

#[rustler::nif]
pub fn rest_catalog_rename_table(
    catalog_resource: ResourceArc<RestCatalogResource>,
    src_namespace: String,
    src_table_name: String,
    dest_namespace: String,
    dest_table_name: String,
) -> (Atom, HashMap<String, String>) {
    let runtime = catalog_resource.runtime.clone();
    let catalog = catalog_resource.get_catalog();

    // Create source table identifier
    let src_namespace_parts: Vec<&str> = src_namespace.split('.').collect();
    let src_namespace_ident =
        NamespaceIdent::from_vec(src_namespace_parts.iter().map(|s| s.to_string()).collect())
            .unwrap();
    let src_table_ident = TableIdent::new(src_namespace_ident, src_table_name.clone());

    // Create destination table identifier
    let dest_namespace_parts: Vec<&str> = dest_namespace.split('.').collect();
    let dest_namespace_ident =
        NamespaceIdent::from_vec(dest_namespace_parts.iter().map(|s| s.to_string()).collect())
            .unwrap();
    let dest_table_ident = TableIdent::new(dest_namespace_ident, dest_table_name.clone());

    let result = runtime.block_on(async {
        catalog
            .rename_table(&src_table_ident, &dest_table_ident)
            .await
    });

    match result {
        Ok(()) => {
            let mut response = HashMap::new();
            response.insert(
                "renamed".to_string(),
                format!(
                    "{}.{} -> {}.{}",
                    src_namespace, src_table_name, dest_namespace, dest_table_name
                ),
            );
            (atoms::ok(), response)
        }
        Err(e) => {
            let mut error_response = HashMap::new();
            error_response.insert(
                "error".to_string(),
                format!("Failed to rename table: {}", e),
            );
            (atoms::error(), error_response)
        }
    }
}
