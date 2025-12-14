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

/// Simple context for OSS (extensible)
#[derive(Clone, Debug)]
pub struct RequestContext {
    /// Optional user identifier
    pub user_id: Option<String>,
}

impl FromRequest for RequestContext {
    type Error = actix_web::error::Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(_req: &HttpRequest, _: &mut Payload) -> Self::Future {
        // OSS: Allow all requests by default
        ok(RequestContext { user_id: None })
    }
}

/// Authorization check (OSS: always true by default)
pub fn is_authorized(_context: &RequestContext, _resource: &str) -> bool {
    true
}

