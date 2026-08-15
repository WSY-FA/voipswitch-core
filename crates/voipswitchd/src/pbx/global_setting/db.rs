use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "global_settings")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub key: String,
    pub value: String,
    pub value_type: String,
    pub version: i64,
    pub updated_at: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
