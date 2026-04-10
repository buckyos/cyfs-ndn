use async_trait::async_trait;
use bytes::Bytes;
use cyfs_gateway_lib::{
    HttpServer, ServerError, ServerResult, StreamInfo,
};
use http::{Method, Response, StatusCode, Version};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use log::info;

#[derive(Default, Clone)]
pub struct NamedStoreMgrHttpGateway;

impl NamedStoreMgrHttpGateway {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl HttpServer for NamedStoreMgrHttpGateway {
    async fn serve_request(
        &self,
        req: http::Request<BoxBody<Bytes, ServerError>>,
        info: StreamInfo,
    ) -> ServerResult<http::Response<BoxBody<Bytes, ServerError>>> {
        let status = if *req.method() == Method::POST {
            StatusCode::NOT_IMPLEMENTED
        } else {
            StatusCode::OK
        };

        let body = if status == StatusCode::OK {
            format!(
                "NamedStoreMgrHttpGateway is running\nmethod: {}\npath: {}\nsrc: {}\n",
                req.method(),
                req.uri()
                    .path_and_query()
                    .map(|pq| pq.as_str())
                    .unwrap_or("/"),
                info.src_addr.clone().unwrap_or_else(|| "unknown".into()),
            )
        } else {
            "NamedStoreMgrHttpGateway POST handler is not implemented yet\n".to_string()
        };

        let resp = Response::builder()
            .status(status)
            .header("content-type", "text/plain; charset=utf-8")
            .body(
                Full::new(Bytes::from(body))
                    .map_err(|never| match never {})
                    .boxed(),
            )
            .map_err(|e| {
                cyfs_gateway_lib::server_err!(
                    cyfs_gateway_lib::ServerErrorCode::EncodeError,
                    "build response failed: {e}"
                )
            })?;

        info!("served request {}", resp.status());
        Ok(resp)
    }

    fn id(&self) -> String {
        "named-store-mgr".to_string()
    }

    fn http_version(&self) -> Version {
        Version::HTTP_11
    }

    fn http3_port(&self) -> Option<u16> {
        None
    }
}

/*
How to start NamedStoreMgrHttpGateway
fn default_port() -> u16 {
    const DEFAULT_PORT: u16 = 3180;
    std::env::var("TEST_SERVER_PORT")
        .ok()
        .and_then(|val| val.parse::<u16>().ok())
        .unwrap_or(DEFAULT_PORT)
}

#[tokio::main]
async fn main() -> ServerResult<()> {
    buckyos_kit::init_logging("test_server", true);

    let port = default_port();
    let runner = Runner::new(port);
    runner.add_http_server("/server/".to_string(), Arc::new(store_mgr_http_gateway::new()))?;

    info!("test_server listening on http://127.0.0.1:{port}");
    runner.run().await
}


*/
