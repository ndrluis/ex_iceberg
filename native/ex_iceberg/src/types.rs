use rustler::{NifStruct, NifTaggedEnum};

#[derive(Debug, NifStruct)]
#[module = "ExIceberg.Types.Field"]
pub struct IcebergField {
    pub name: String,
    pub field_type: IcebergFieldType,
    pub required: bool,
}

#[derive(Debug, NifTaggedEnum)]
pub enum IcebergFieldType {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    String,
    Uuid,
    Date,
    Timestamp,
    Binary,
    Decimal {
        precision: u32,
        scale: u32,
    },
    Fixed {
        length: u32,
    },
    List {
        element_type: Box<IcebergFieldType>,
        element_required: bool,
    },
    Map {
        key_type: Box<IcebergFieldType>,
        value_type: Box<IcebergFieldType>,
        value_required: bool,
    },
    Struct {
        fields: Vec<IcebergField>,
    },
}
