use bucket_store::BucketError;
use http::StatusCode;

/// An S3 error response: status, S3 error code, message.
#[derive(Debug, Clone)]
pub struct S3Error {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
}

impl S3Error {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        S3Error { status, code, message: message.into() }
    }

    pub fn auth(code: &'static str, message: impl Into<String>) -> Self {
        let status = match code {
            "AuthorizationHeaderMalformed" | "AuthorizationQueryParametersError" => StatusCode::BAD_REQUEST,
            _ => StatusCode::FORBIDDEN,
        };
        S3Error::new(status, code, message)
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        S3Error::new(StatusCode::BAD_REQUEST, "InvalidArgument", message)
    }

    pub fn malformed_xml() -> Self {
        S3Error::new(StatusCode::BAD_REQUEST, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema")
    }

    pub fn not_implemented(what: &str) -> Self {
        S3Error::new(StatusCode::NOT_IMPLEMENTED, "NotImplemented", format!("{what} is not implemented"))
    }

    pub fn no_such_bucket(name: &str) -> Self {
        S3Error::new(StatusCode::NOT_FOUND, "NoSuchBucket", format!("The specified bucket does not exist: {name}"))
    }

    pub fn no_such_upload() -> Self {
        S3Error::new(StatusCode::NOT_FOUND, "NoSuchUpload", "The specified multipart upload does not exist")
    }

    pub fn internal(message: impl Into<String>) -> Self {
        S3Error::new(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", message)
    }
}

impl From<BucketError> for S3Error {
    fn from(e: BucketError) -> Self {
        let (status, code) = match &e {
            BucketError::BucketNotFound(_) => (StatusCode::NOT_FOUND, "NoSuchBucket"),
            BucketError::NoSuchKey(_) | BucketError::ContentEvicted(_) | BucketError::OriginUnavailable(_) => (StatusCode::NOT_FOUND, "NoSuchKey"),
            BucketError::PermissionDenied(_) | BucketError::NotHeld(_) | BucketError::TokenInvalid => (StatusCode::FORBIDDEN, "AccessDenied"),
            BucketError::PreconditionFailed(_) => (StatusCode::PRECONDITION_FAILED, "PreconditionFailed"),
            BucketError::QuotaExceeded(_) => (StatusCode::INSUFFICIENT_STORAGE, "QuotaExceeded"),
            BucketError::ObjectTooLarge { .. } => (StatusCode::BAD_REQUEST, "EntityTooLarge"),
            BucketError::InvalidKey(_) => (StatusCode::BAD_REQUEST, "InvalidArgument"),
            BucketError::InvalidArgument(_) | BucketError::OriginRequired(_) => (StatusCode::BAD_REQUEST, "InvalidArgument"),
            BucketError::KindMismatch(_) | BucketError::PolicyViolation(_) => (StatusCode::BAD_REQUEST, "InvalidRequest"),
            BucketError::ContentNotAvailable(_) | BucketError::Conflict(_) => (StatusCode::CONFLICT, "OperationAborted"),
            BucketError::Store(_) | BucketError::Db(_) | BucketError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "InternalError"),
        };
        S3Error::new(status, code, e.to_string())
    }
}

pub type S3Result<T> = Result<T, S3Error>;
