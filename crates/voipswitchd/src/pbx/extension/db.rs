pub mod extension_record {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "extensions")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub domain_id: String,
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: i64,
        pub number: String,
        pub auth_user: String,
        pub password: String,
        pub enabled: bool,
        pub note: String,
        pub created_at: i64,
        pub updated_at: i64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}
