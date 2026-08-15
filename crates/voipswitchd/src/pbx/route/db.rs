#![allow(dead_code)]

pub mod inbound_route_record {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "inbound_route")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub domain_id: String,
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: i64,
        pub name: String,
        pub enabled: bool,
        pub trunk_match: String,
        pub dst_pattern: String,
        pub src_pattern: Option<String>,
        pub dst_strip: i64,
        pub dst_prefix: String,
        pub dst_suffix: String,
        pub src_strip: i64,
        pub src_prefix: String,
        pub src_suffix: String,
        pub target: String,
        pub priority: i64,
        pub note: String,
        pub created_at: i64,
        pub updated_at: i64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod outbound_route_record {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "outbound_route")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub domain_id: String,
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: i64,
        pub name: String,
        pub enabled: bool,
        pub dst_pattern: String,
        pub src_pattern: Option<String>,
        pub dst_strip: i64,
        pub dst_prefix: String,
        pub dst_suffix: String,
        pub src_strip: i64,
        pub src_prefix: String,
        pub src_suffix: String,
        pub priority: i64,
        pub note: String,
        pub created_at: i64,
        pub updated_at: i64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod outbound_route_trunk_record {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "outbound_route_trunks")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub domain_id: String,
        #[sea_orm(primary_key, auto_increment = false)]
        pub route_id: i64,
        #[sea_orm(primary_key, auto_increment = false)]
        pub trunk_ref: String,
        pub position: i64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}
