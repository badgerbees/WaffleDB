/// Auto-generated protobuf code
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InsertRequest {
    #[prost(string, tag = "1")]
    pub collection: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub id: ::prost::alloc::string::String,
    #[prost(float, repeated, tag = "3")]
    pub vector: ::prost::alloc::vec::Vec<f32>,
    #[prost(map = "string, string", tag = "4")]
    pub metadata: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InsertResponse {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub status: ::prost::alloc::string::String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchRequest {
    #[prost(string, tag = "1")]
    pub collection: ::prost::alloc::string::String,
    #[prost(float, repeated, tag = "2")]
    pub vector: ::prost::alloc::vec::Vec<f32>,
    #[prost(int32, tag = "3")]
    pub top_k: i32,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchResponse {
    #[prost(message, repeated, tag = "1")]
    pub results: ::prost::alloc::vec::Vec<SearchResult>,
    #[prost(double, tag = "2")]
    pub latency_ms: f64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchResult {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(float, tag = "2")]
    pub distance: f32,
    #[prost(map = "string, string", tag = "3")]
    pub metadata: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateCollectionRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(int32, tag = "2")]
    pub dimension: i32,
    #[prost(string, tag = "3")]
    pub metric: ::prost::alloc::string::String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateCollectionResponse {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(int32, tag = "2")]
    pub dimension: i32,
    #[prost(string, tag = "3")]
    pub status: ::prost::alloc::string::String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteCollectionRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteCollectionResponse {
    #[prost(string, tag = "1")]
    pub status: ::prost::alloc::string::String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListCollectionsRequest {}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListCollectionsResponse {
    #[prost(message, repeated, tag = "1")]
    pub collections: ::prost::alloc::vec::Vec<Collection>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Collection {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(int32, tag = "2")]
    pub dimension: i32,
    #[prost(int32, tag = "3")]
    pub vector_count: i32,
}

#[tonic::async_trait]
pub trait VectorService: Send + Sync + 'static {
    async fn insert(
        &self,
        request: tonic::Request<InsertRequest>,
    ) -> Result<tonic::Response<InsertResponse>, tonic::Status>;
    
    async fn search(
        &self,
        request: tonic::Request<SearchRequest>,
    ) -> Result<tonic::Response<SearchResponse>, tonic::Status>;
}

#[tonic::async_trait]
pub trait CollectionService: Send + Sync + 'static {
    async fn create_collection(
        &self,
        request: tonic::Request<CreateCollectionRequest>,
    ) -> Result<tonic::Response<CreateCollectionResponse>, tonic::Status>;
}

pub mod vector_service_server {
    use super::*;
    use tonic::codegen::*;
    
    pub struct VectorServiceServer<T: VectorService> {
        inner: _Inner<T>,
    }

    struct _Inner<T>(Arc<T>);

    impl<T: VectorService> VectorServiceServer<T> {
        pub fn new(inner: T) -> Self {
            Self {
                inner: _Inner(Arc::new(inner)),
            }
        }
    }
}

pub mod collection_service_server {
    use super::*;
    use tonic::codegen::*;
    
    pub struct CollectionServiceServer<T: CollectionService> {
        inner: _Inner<T>,
    }

    struct _Inner<T>(Arc<T>);

    impl<T: CollectionService> CollectionServiceServer<T> {
        pub fn new(inner: T) -> Self {
            Self {
                inner: _Inner(Arc::new(inner)),
            }
        }
    }
}
