//! Authentication - OSS Foundation
//!
//! This module provides the option for authentication.
//! By default, all requests are allowed (OSS mode).
//!
//! To add authentication, create a custom middleware or wrap handlers.
//! Examples:
//! - Firebase authentication wrapper
//! - API key validation
//! - JWT token verification
//! - Custom OAuth2 provider

use actix_web::{HttpRequest, FromRequest, dev::Payload};
use futures::future::{ok, Ready};

/// Request context with tenant and user information
#[derive(Clone, Debug)]
pub struct RequestContext {
    /// Tenant identifier - scopes all operations to this tenant
    pub tenant_id: String,
    /// Optional user identifier
    pub user_id: Option<String>,
}

impl FromRequest for RequestContext {
    type Error = actix_web::error::Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        // Extract tenant_id from header or URL path
        // Priority: X-Tenant-ID header → path parameter → default "default"
        let tenant_id = req
            .headers()
            .get("X-Tenant-ID")
            .and_then(|h| h.to_str().ok())
            .or_else(|| req.match_info().get("tenant_id"))
            .unwrap_or("default")
            .to_string();

        ok(RequestContext {
            tenant_id,
            user_id: None,
        })
    }
}

/// Authorization check (OSS: always true by default)
pub fn is_authorized(_context: &RequestContext, _resource: &str) -> bool {
    true
}

