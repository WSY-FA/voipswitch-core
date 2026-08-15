#![allow(dead_code)]

pub mod peer_trunk_record {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "peer_trunk")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub domain_id: String,
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: i64,
        pub name: String,
        pub server_host: String,
        pub server_port: i64,
        pub outbound_proxy_host: Option<String>,
        pub outbound_proxy_port: Option<i64>,
        pub transport: String,
        pub keep_alive_seconds: i64,
        pub enabled: bool,
        pub note: String,
        pub created_at: i64,
        pub updated_at: i64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod reg_trunk_record {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "reg_trunk")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub domain_id: String,
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: i64,
        pub name: String,
        pub server_host: String,
        pub server_port: i64,
        pub outbound_proxy_host: Option<String>,
        pub outbound_proxy_port: Option<i64>,
        pub transport: String,
        pub keep_alive_seconds: i64,
        pub requested_expires_seconds: i64,
        pub enabled: bool,
        pub note: String,
        pub created_at: i64,
        pub updated_at: i64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod reg_account_record {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "reg_account")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub domain_id: String,
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: i64,
        pub reg_trunk_id: i64,
        pub auth_name: String,
        pub auth_pwd: String,
        pub enabled: bool,
        pub note: String,
        pub created_at: i64,
        pub updated_at: i64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}
