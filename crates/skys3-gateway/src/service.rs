//! The request pipeline: limits, authentication, the anonymous-access
//! gate, rejected features, body bounds, then `s3s` routing, authorization,
//! and the S3 operations.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use http::{Request, Response, StatusCode};
use s3s::service::{S3Service, S3ServiceBuilder};
use s3s::{Body, BodySizeLimitExceeded, S3Error, s3_error};
use skys3_control::{ControlError, ControlStore};

use crate::api::Api;
use crate::authz::{self, Access, NoSignatures, Permissions};
use crate::buckets::{Buckets, GatewayConfig, IdSource};
use crate::features;
use crate::limits::{BodyKind, RequestLimits};
use crate::shard::Shards;
use crate::sigv4::{Authenticated, BodyError};

/// Authenticates requests before they are routed.
///
/// The gateway calls it with every request whose head is within the
/// limits, before anything else looks at the request. It returns the
/// request to route, with whatever it learned about the caller in its
/// extensions, or the S3 error to answer with.
/// [`SigV4Authenticator`](crate::SigV4Authenticator) is the production
/// implementation.
pub trait Authenticator: Send + Sync + 'static {
    /// Authenticates `request`.
    ///
    /// # Errors
    ///
    /// The S3 error to answer with, such as `AccessDenied` or
    /// `SignatureDoesNotMatch`.
    fn authenticate(
        &self,
        request: Request<Body>,
    ) -> impl Future<Output = Result<Request<Body>, S3Error>> + Send;
}

/// An [`Authenticator`] for tests that trusts every request: it takes each
/// one as signed by [`TrustAll::PRINCIPAL`], whom every policy check
/// allows.
#[cfg(any(test, feature = "test-util"))]
#[derive(Debug, Clone, Copy, Default)]
pub struct TrustAll;

#[cfg(any(test, feature = "test-util"))]
impl TrustAll {
    /// The name of the principal every request comes from.
    pub const PRINCIPAL: &str = "trusted";
}

#[cfg(any(test, feature = "test-util"))]
impl Authenticator for TrustAll {
    async fn authenticate(&self, mut request: Request<Body>) -> Result<Request<Body>, S3Error> {
        request.extensions_mut().insert(Authenticated {
            principal: crate::Principal::new(Self::PRINCIPAL, Permissions::allow_all()),
            access_key_id: String::new(),
            method: crate::sigv4::AuthMethod::Header,
        });
        Ok(request)
    }
}

/// The bucket records, without their store and shard types.
trait Catalog: Send + Sync {
    fn reload(&self) -> Pin<Box<dyn Future<Output = Result<(), ControlError>> + Send + '_>>;
}

impl<C: ControlStore, H: Shards> Catalog for Buckets<C, H> {
    fn reload(&self) -> Pin<Box<dyn Future<Output = Result<(), ControlError>> + Send + '_>> {
        Box::pin(Buckets::reload(self))
    }
}

/// The S3 gateway: an HTTP service that answers S3 requests.
///
/// [`Gateway::handle`] runs each request through the pipeline:
///
/// 1. The head is checked against the [`RequestLimits`].
/// 2. The [`Authenticator`] authenticates it.
/// 3. An unsigned request is refused unless anonymous access is on.
/// 4. Requests for features SkyS3 rejects are answered (design §11).
/// 5. An XML body is read, within its size limit, and checked for depth.
/// 6. `s3s` routes the request to its operation, which the caller's
///    permissions must allow ([`crate::authz`]), parses it, and calls the
///    operation.
///
/// Clones share the gateway.
pub struct Gateway<A> {
    inner: Arc<Inner<A>>,
}

struct Inner<A> {
    s3: S3Service,
    limits: RequestLimits,
    auth: A,
    anonymous: Option<Permissions>,
    catalog: Arc<dyn Catalog>,
}

impl<A> Clone for Gateway<A> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<A> fmt::Debug for Gateway<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Gateway")
            .field("limits", &self.inner.limits)
            .finish_non_exhaustive()
    }
}

impl<A: Authenticator> Gateway<A> {
    /// Builds a gateway over a control store and the shards, loading the
    /// bucket registers.
    ///
    /// # Errors
    ///
    /// The control store's errors, including an invalid bucket register.
    pub async fn new<C: ControlStore, H: Shards>(
        config: GatewayConfig,
        store: C,
        shards: H,
        ids: IdSource,
        auth: A,
    ) -> Result<Self, ControlError> {
        let limits = config.limits;
        let anonymous = config.anonymous.clone();
        let buckets = Arc::new(Buckets::load(store, shards, config, ids).await?);
        let mut builder = S3ServiceBuilder::new(Api::new(Arc::clone(&buckets)));
        builder.set_config(limits.s3s_config());
        builder.set_auth(NoSignatures);
        builder.set_access(Access::new(anonymous.clone()));
        Ok(Self {
            inner: Arc::new(Inner {
                s3: builder.build(),
                limits,
                auth,
                anonymous,
                catalog: buckets,
            }),
        })
    }

    /// The request limits.
    #[must_use]
    pub fn limits(&self) -> &RequestLimits {
        &self.inner.limits
    }

    /// Reloads the bucket registers from the control store, for a node
    /// that learns of a change another node made (design §6.2).
    ///
    /// # Errors
    ///
    /// The control store's errors; the local copy is then unchanged.
    pub async fn reload_buckets(&self) -> Result<(), ControlError> {
        self.inner.catalog.reload().await
    }

    /// Answers one request.
    pub async fn handle(&self, request: Request<Body>) -> Response<Body> {
        match self.process(request).await {
            Ok(response) => response,
            Err(error) => error_response(error),
        }
    }

    async fn process(&self, request: Request<Body>) -> Result<Response<Body>, S3Error> {
        let inner = &*self.inner;
        let (parts, body) = request.into_parts();
        let shape = inner.limits.check_head(&parts)?;
        let request = inner
            .auth
            .authenticate(Request::from_parts(parts, body))
            .await?;
        let (parts, mut body) = request.into_parts();
        if inner.anonymous.is_none() && parts.extensions.get::<Authenticated>().is_none() {
            return Err(authz::access_denied());
        }
        features::reject_unsupported(&parts, &shape)?;
        if shape.body == BodyKind::Xml {
            let max = inner.limits.max_xml_body_bytes;
            let bytes = body
                .store_all_limited(max)
                .await
                .map_err(|error| body_error(&inner.limits, &*error))?;
            inner.limits.check_xml(&bytes)?;
        }
        inner
            .s3
            .call(Request::from_parts(parts, body))
            .await
            .map_err(|error| {
                tracing::error!(%error, "s3s failed to answer a request");
                s3_error!(InternalError)
            })
    }
}

/// The S3 error for a request body that could not be read.
fn body_error(limits: &RequestLimits, error: &(dyn std::error::Error + 'static)) -> S3Error {
    if let Some(error) = BodyError::find(error) {
        return error.to_s3_error();
    }
    let too_long = error.is::<BodySizeLimitExceeded>()
        || error.is::<http_body_util::LengthLimitError>()
        || error
            .source()
            .is_some_and(|source| source.is::<http_body_util::LengthLimitError>());
    if too_long {
        limits.xml_too_large()
    } else {
        s3_error!(
            IncompleteBody,
            "The request body could not be read: {error}"
        )
    }
}

/// The XML error response for `error`.
///
/// A message that cannot be written as XML, such as one with a control
/// character, is left out rather than turning the answer into a `500`.
pub(crate) fn error_response(error: S3Error) -> Response<Body> {
    let code = error.code().clone();
    error
        .to_http_response()
        .or_else(|_| S3Error::new(code).to_http_response())
        .unwrap_or_else(|_| {
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            response
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unreadable_bodies_are_classified() {
        let limits = RequestLimits::default();
        let too_long = BodySizeLimitExceeded { size: 2, limit: 1 };
        assert_eq!(
            *body_error(&limits, &too_long).code(),
            s3s::S3ErrorCode::MaxMessageLengthExceeded
        );
        let broken = std::io::Error::other("reset");
        assert_eq!(
            *body_error(&limits, &broken).code(),
            s3s::S3ErrorCode::IncompleteBody
        );
    }

    #[test]
    fn errors_render_as_s3_xml() {
        let response = error_response(s3_error!(NoSuchBucket));
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let unwritable = error_response(s3_error!(MalformedXML, "quoted \0 byte"));
        assert_eq!(unwritable.status(), StatusCode::BAD_REQUEST);
    }
}
