pub mod auth;
pub mod timeout;
pub mod rate_limit;

pub use auth::RequestContext;

use actix_web::{
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
    Error,
};
use futures::future::LocalBoxFuture;
use std::time::Instant;
use tracing::{info, warn, span, Level};

/// Request logging middleware
pub struct RequestLogger;

impl<S, B> Transform<S, ServiceRequest> for RequestLogger
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = RequestLoggerMiddleware<S>;
    type Future = std::future::Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        std::future::ready(Ok(RequestLoggerMiddleware { service }))
    }
}

pub struct RequestLoggerMiddleware<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for RequestLoggerMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let method = req.method().to_string();
        let path = req.path().to_string();
        let request_id = uuid::Uuid::new_v4().to_string();
        let start = Instant::now();

        let span = span!(Level::DEBUG, "http_request", 
            request_id = %request_id,
            method = %method,
            path = %path
        );

        let fut = self.service.call(req);

        Box::pin(async move {
            let res = fut.await?;
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
            let status = res.status();

            let _enter = span.enter();

            if status.is_success() {
                info!(
                    status = status.as_u16(),
                    latency_ms = elapsed_ms,
                    "Request completed successfully"
                );
            } else {
                warn!(
                    status = status.as_u16(),
                    latency_ms = elapsed_ms,
                    "Request failed"
                );
            }

            Ok(res)
        })
    }
}
