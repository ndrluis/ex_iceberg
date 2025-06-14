use rustler::{Atom, Resource, ResourceArc};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;

use iceberg::spec::{ListType, MapType, NestedField, PrimitiveType, Schema, StructType, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
// TODO: Re-enable when Table resources are implemented
// use iceberg::table::Table;
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};

mod atoms {
    rustler::atoms! {
        ok,
        error,
        nil,
    }
}

pub struct RestCatalogResource {
    uri: String,
    warehouse: Option<String>,
    props: HashMap<String, String>,
    runtime: Arc<Runtime>,
}

unsafe impl Send for RestCatalogResource {}
unsafe impl Sync for RestCatalogResource {}

#[rustler::resource_impl]
impl Resource for RestCatalogResource {}

// TODO: Table Resource for wrapping iceberg-rust Table
// Currently disabled due to RefUnwindSafe issues with iceberg-rust dependencies
// pub struct TableResource {
//     table: Table,
//     runtime: Arc<Runtime>,
// }

impl RestCatalogResource {
    fn new(uri: String, warehouse: Option<String>, props: HashMap<String, String>) -> Self {
        Self {
            uri,
            warehouse,
            props,
            runtime: Arc::new(Runtime::new().unwrap()),
        }
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

    fn get_catalog(&self) -> RestCatalog {
        RestCatalog::new(self.build_config())
    }
}

#[derive(rustler::NifStruct)]
#[module = "ExIceberg.Rest.CatalogConfig"]
struct CatalogConfigParams {
    uri: String,
    warehouse: Option<String>,
    token: Option<String>,
    credential: Option<String>,
    oauth2_server_uri: Option<String>,
    scope: Option<String>,
    audience: Option<String>,
    resource: Option<String>,
}

#[rustler::nif]
fn rest_catalog_new(config: CatalogConfigParams) -> (Atom, ResourceArc<RestCatalogResource>) {
    // Build properties map for authentication
    let mut props = std::collections::HashMap::new();

    // Add token if provided
    if let Some(token_val) = config.token {
        props.insert("token".to_string(), token_val);
    }

    // Add OAuth2 configuration if provided
    if let Some(cred) = config.credential {
        props.insert("credential".to_string(), cred);
    }

    if let Some(oauth_uri) = config.oauth2_server_uri {
        props.insert("oauth2-server-uri".to_string(), oauth_uri);
    }

    if let Some(scope_val) = config.scope {
        props.insert("scope".to_string(), scope_val);
    }

    if let Some(audience_val) = config.audience {
        props.insert("audience".to_string(), audience_val);
    }

    if let Some(resource_val) = config.resource {
        props.insert("resource".to_string(), resource_val);
    }

    // Create resource with config (catalog will be created as needed)
    let resource = RestCatalogResource::new(config.uri, config.warehouse, props);

    (atoms::ok(), ResourceArc::new(resource))
}

#[rustler::nif]
fn rest_catalog_list_namespaces(
    catalog_resource: ResourceArc<RestCatalogResource>,
) -> (Atom, Vec<String>) {
    let runtime = catalog_resource.runtime.clone();
    let catalog = catalog_resource.get_catalog();

    let result = runtime.block_on(async { catalog.list_namespaces(None).await });

    match result {
        Ok(namespaces) => {
            let namespace_names: Vec<String> = namespaces.iter().map(|ns| ns.join(".")).collect();
            (atoms::ok(), namespace_names)
        }
        Err(e) => (
            atoms::error(),
            vec![format!("Failed to list namespaces: {}", e)],
        ),
    }
}

#[rustler::nif]
fn rest_catalog_create_namespace(
    catalog_resource: ResourceArc<RestCatalogResource>,
    namespace: String,
    properties: HashMap<String, String>,
) -> (Atom, HashMap<String, Vec<String>>) {
    let runtime = catalog_resource.runtime.clone();
    let catalog = catalog_resource.get_catalog();

    // Convert namespace string to NamespaceIdent
    let namespace_parts: Vec<&str> = namespace.split('.').collect();
    let namespace_ident =
        iceberg::NamespaceIdent::from_vec(namespace_parts.iter().map(|s| s.to_string()).collect())
            .unwrap();

    let result =
        runtime.block_on(async { catalog.create_namespace(&namespace_ident, properties).await });

    match result {
        Ok(_) => {
            let mut response = HashMap::new();
            response.insert("namespace".to_string(), vec![namespace]);
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
fn rest_catalog_table_exists(
    catalog_resource: ResourceArc<RestCatalogResource>,
    namespace: String,
    table_name: String,
) -> (Atom, bool) {
    let runtime = catalog_resource.runtime.clone();
    let catalog = catalog_resource.get_catalog();

    // Create table identifier
    let namespace_parts: Vec<&str> = namespace.split('.').collect();
    let namespace_ident =
        NamespaceIdent::from_vec(namespace_parts.iter().map(|s| s.to_string()).collect()).unwrap();
    let table_ident = TableIdent::new(namespace_ident, table_name);

    let result = runtime.block_on(async { catalog.table_exists(&table_ident).await });

    match result {
        Ok(exists) => (atoms::ok(), exists),
        Err(_e) => (atoms::error(), false),
    }
}

#[rustler::nif]
fn rest_catalog_drop_table(
    catalog_resource: ResourceArc<RestCatalogResource>,
    namespace: String,
    table_name: String,
) -> (Atom, HashMap<String, String>) {
    let runtime = catalog_resource.runtime.clone();
    let catalog = catalog_resource.get_catalog();

    // Create table identifier
    let namespace_parts: Vec<&str> = namespace.split('.').collect();
    let namespace_ident =
        NamespaceIdent::from_vec(namespace_parts.iter().map(|s| s.to_string()).collect()).unwrap();
    let table_ident = TableIdent::new(namespace_ident, table_name);

    let result = runtime.block_on(async { catalog.drop_table(&table_ident).await });

    match result {
        Ok(_) => {
            let mut response = HashMap::new();
            response.insert(
                "table".to_string(),
                format!("{}.{}", namespace, table_ident.name),
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

// Structured type definitions matching what we'll send from Elixir
#[derive(Debug, rustler::NifStruct)]
#[module = "ExIceberg.Types.Field"]
pub struct IcebergField {
    pub name: String,
    pub field_type: IcebergType,
    pub required: bool,
    pub field_id: Option<i32>,
}

#[derive(Debug, rustler::NifTaggedEnum)]
pub enum IcebergType {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    String,
    Uuid,
    Date,
    Time,
    Timestamp,
    Timestamptz,
    TimestampNs,
    TimestamptzNs,
    Binary,
    Decimal {
        precision: u32,
        scale: u32,
    },
    Fixed {
        length: u64,
    },
    List {
        element_type: Box<IcebergType>,
        element_required: bool,
    },
    Map {
        key_type: Box<IcebergType>,
        value_type: Box<IcebergType>,
        value_required: bool,
    },
    Struct {
        fields: Vec<IcebergField>,
    },
}

fn iceberg_type_to_spec_type(iceberg_type: &IcebergType, next_field_id: &mut i32) -> Type {
    match iceberg_type {
        IcebergType::Boolean => Type::Primitive(PrimitiveType::Boolean),
        IcebergType::Int => Type::Primitive(PrimitiveType::Int),
        IcebergType::Long => Type::Primitive(PrimitiveType::Long),
        IcebergType::Float => Type::Primitive(PrimitiveType::Float),
        IcebergType::Double => Type::Primitive(PrimitiveType::Double),
        IcebergType::String => Type::Primitive(PrimitiveType::String),
        IcebergType::Uuid => Type::Primitive(PrimitiveType::Uuid),
        IcebergType::Date => Type::Primitive(PrimitiveType::Date),
        IcebergType::Time => Type::Primitive(PrimitiveType::Time),
        IcebergType::Timestamp => Type::Primitive(PrimitiveType::Timestamp),
        IcebergType::Timestamptz => Type::Primitive(PrimitiveType::Timestamptz),
        IcebergType::TimestampNs => Type::Primitive(PrimitiveType::TimestampNs),
        IcebergType::TimestamptzNs => Type::Primitive(PrimitiveType::TimestamptzNs),
        IcebergType::Binary => Type::Primitive(PrimitiveType::Binary),

        IcebergType::Decimal { precision, scale } => Type::Primitive(PrimitiveType::Decimal {
            precision: *precision,
            scale: *scale,
        }),

        IcebergType::Fixed { length } => Type::Primitive(PrimitiveType::Fixed(*length)),

        IcebergType::List {
            element_type,
            element_required,
        } => {
            let element_spec_type = iceberg_type_to_spec_type(element_type, next_field_id);
            let element_id = *next_field_id;
            *next_field_id += 1;

            let element_field = if *element_required {
                NestedField::required(element_id, "element", element_spec_type)
            } else {
                NestedField::optional(element_id, "element", element_spec_type)
            };

            Type::List(ListType {
                element_field: element_field.into(),
            })
        }

        IcebergType::Map {
            key_type,
            value_type,
            value_required,
        } => {
            let key_spec_type = iceberg_type_to_spec_type(key_type, next_field_id);
            let key_id = *next_field_id;
            *next_field_id += 1;

            let value_spec_type = iceberg_type_to_spec_type(value_type, next_field_id);
            let value_id = *next_field_id;
            *next_field_id += 1;

            let key_field = NestedField::required(key_id, "key", key_spec_type);

            let value_field = if *value_required {
                NestedField::required(value_id, "value", value_spec_type)
            } else {
                NestedField::optional(value_id, "value", value_spec_type)
            };

            Type::Map(MapType {
                key_field: key_field.into(),
                value_field: value_field.into(),
            })
        }

        IcebergType::Struct { fields } => {
            let mut struct_fields = Vec::new();

            for field in fields {
                let field_spec_type = iceberg_type_to_spec_type(&field.field_type, next_field_id);
                let field_id = field.field_id.unwrap_or_else(|| {
                    let id = *next_field_id;
                    *next_field_id += 1;
                    id
                });

                let nested_field = if field.required {
                    NestedField::required(field_id, &field.name, field_spec_type)
                } else {
                    NestedField::optional(field_id, &field.name, field_spec_type)
                };

                struct_fields.push(nested_field.into());
            }

            Type::Struct(StructType::new(struct_fields))
        }
    }
}

#[rustler::nif]
fn rest_catalog_create_table(
    catalog_resource: ResourceArc<RestCatalogResource>,
    namespace: String,
    table_name: String,
    fields: Vec<IcebergField>,
    properties: HashMap<String, String>,
) -> (Atom, HashMap<String, String>) {
    let runtime = catalog_resource.runtime.clone();
    let catalog = catalog_resource.get_catalog();

    // Create namespace identifier
    let namespace_parts: Vec<&str> = namespace.split('.').collect();
    let namespace_ident =
        NamespaceIdent::from_vec(namespace_parts.iter().map(|s| s.to_string()).collect()).unwrap();

    // Build schema using iceberg-rust API
    let mut schema_fields = Vec::new();
    let mut next_field_id = 1000i32; // Start nested field IDs from 1000

    for (index, field) in fields.iter().enumerate() {
        let field_id = field.field_id.unwrap_or((index + 1) as i32);
        let iceberg_spec_type = iceberg_type_to_spec_type(&field.field_type, &mut next_field_id);

        let nested_field = if field.required {
            NestedField::required(field_id, &field.name, iceberg_spec_type)
        } else {
            NestedField::optional(field_id, &field.name, iceberg_spec_type)
        };

        schema_fields.push(nested_field.into());
    }

    let table_schema = Schema::builder()
        .with_fields(schema_fields)
        .with_schema_id(1)
        .build()
        .unwrap();

    // Build table creation
    let table_creation = TableCreation::builder()
        .name(table_name.clone())
        .schema(table_schema)
        .properties(properties)
        .build();

    let result =
        runtime.block_on(async { catalog.create_table(&namespace_ident, table_creation).await });

    match result {
        Ok(_table) => {
            let mut response = HashMap::new();
            response.insert("table".to_string(), format!("{}.{}", namespace, table_name));
            (atoms::ok(), response)
        }
        Err(e) => {
            let mut error_response = HashMap::new();
            error_response.insert(
                "error".to_string(),
                format!("Failed to create table: {}", e),
            );
            (atoms::error(), error_response)
        }
    }
}

#[rustler::nif]
fn rest_catalog_load_table(
    catalog_resource: ResourceArc<RestCatalogResource>,
    namespace: String,
    table_name: String,
) -> (Atom, HashMap<String, String>) {
    let runtime = catalog_resource.runtime.clone();
    let catalog = catalog_resource.get_catalog();

    // Create table identifier
    let namespace_parts: Vec<&str> = namespace.split('.').collect();
    let namespace_ident =
        NamespaceIdent::from_vec(namespace_parts.iter().map(|s| s.to_string()).collect()).unwrap();
    let table_ident = TableIdent::new(namespace_ident, table_name);

    let result = runtime.block_on(async { catalog.load_table(&table_ident).await });

    match result {
        Ok(table) => {
            let metadata = table.metadata();
            let mut response = HashMap::new();

            // Extract basic table information
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

            (atoms::ok(), response)
        }
        Err(e) => {
            let mut error_response = HashMap::new();
            error_response.insert("error".to_string(), format!("Failed to load table: {}", e));
            (atoms::error(), error_response)
        }
    }
}

// TODO: Table metadata operations
// Currently disabled due to RefUnwindSafe issues with iceberg-rust dependencies
// These will be re-enabled in a future version with proper resource handling

rustler::init!("Elixir.ExIceberg.Nif");
