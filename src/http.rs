use crate::{
    api::{self, Api, ApiError, Command, MAX_BUNDLE, Query, TRANSFER_TIMEOUT_SECONDS},
    authority,
    model::{ClaimRequest, CompleteRequest, HelloRequest, RenewRequest},
    store::Store,
};
use anyhow::{Result, ensure};
use axum::{
    Extension, Json, Router,
    body::{Body, Bytes},
    extract::{
        DefaultBodyLimit, Path, Query as UrlQuery, Request, State,
        rejection::{JsonRejection, QueryRejection},
    },
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{
        IntoResponse, Response, Sse,
        sse::{Event, KeepAlive},
    },
    routing::{get, post, put},
};
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    net::SocketAddr,
    path::{Path as FsPath, PathBuf},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use tokio::{
    io::AsyncReadExt,
    sync::{OwnedSemaphorePermit, Semaphore},
};

// Capacity records live only while requests or detached work own them. The
// registry itself is bounded, including when callers rotate invalid credentials.
struct Budgets<T>(Mutex<BTreeMap<String, Weak<T>>>);
impl<T> Budgets<T> {
    fn new() -> Self {
        Self(Mutex::new(BTreeMap::new()))
    }
    fn get(&self, key: String, create: impl FnOnce() -> T) -> Result<Arc<T>, ApiError> {
        let mut entries = self
            .0
            .lock()
            .map_err(|_| ApiError::internal("admission lock"))?;
        if let Some(value) = entries.get(&key).and_then(Weak::upgrade) {
            return Ok(value);
        }
        entries.retain(|_, value| value.strong_count() > 0);
        if entries.len() >= 256 {
            return Err(busy());
        }
        let value = Arc::new(create());
        entries.insert(key, Arc::downgrade(&value));
        Ok(value)
    }
}

struct PrincipalBudget {
    requests: Arc<Semaphore>,
    work: Arc<Semaphore>,
    control: Arc<Semaphore>,
}
impl PrincipalBudget {
    fn new() -> Self {
        Self {
            requests: Arc::new(Semaphore::new(8)),
            work: Arc::new(Semaphore::new(2)),
            control: Arc::new(Semaphore::new(4)),
        }
    }
}
struct Admission {
    principal: Arc<PrincipalBudget>,
    _request: Option<OwnedSemaphorePermit>,
    _call: Option<OwnedSemaphorePermit>,
}

#[derive(Clone)]
pub struct Service {
    pub api: Arc<Api>,
    calls: Arc<Semaphore>,
    work: Arc<Semaphore>,
    control: Arc<Semaphore>,
    authentication: Arc<Semaphore>,
    credentials: Arc<Budgets<Arc<Semaphore>>>,
    principals: Arc<Budgets<PrincipalBudget>>,
    ingress: Arc<Semaphore>,
    streams: Arc<Semaphore>,
    transfers: Arc<Semaphore>,
    pub origins: BTreeSet<String>,
}
impl Service {
    pub fn new(api: Arc<Api>, origins: BTreeSet<String>) -> Self {
        Self {
            api,
            calls: Arc::new(Semaphore::new(64)),
            streams: Arc::new(Semaphore::new(32)),
            transfers: Arc::new(Semaphore::new(4)),
            // Together these allow at most 48 blocking application operations,
            // leaving room in the runtime's 64-thread pool for file streaming.
            work: Arc::new(Semaphore::new(24)),
            control: Arc::new(Semaphore::new(8)),
            authentication: Arc::new(Semaphore::new(16)),
            credentials: Arc::new(Budgets::new()),
            principals: Arc::new(Budgets::new()),
            ingress: Arc::new(Semaphore::new(4)),
            origins,
        }
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            StatusCode::from_u16(self.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Json(self.body()),
        )
            .into_response()
    }
}
fn header<'a>(headers: &'a HeaderMap, name: &str) -> std::result::Result<&'a str, ApiError> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            ApiError::new(
                400,
                "missing_header",
                &format!("Missing or invalid {name} header."),
            )
        })
}
pub fn token(headers: &HeaderMap) -> std::result::Result<String, ApiError> {
    let value = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or_else(|| {
            ApiError::new(
                401,
                "unauthorized",
                "A scoped Bearer credential is required.",
            )
        })?;
    Ok(value.into())
}
fn busy() -> ApiError {
    ApiError::new(
        503,
        "busy",
        "Service concurrency budget is full; retry with backoff.",
    )
}

async fn boundary(State(service): State<Service>, mut request: Request, next: Next) -> Response {
    if let Some(origin) = request.headers().get("origin")
        && origin
            .to_str()
            .ok()
            .is_none_or(|o| !service.origins.contains(o))
    {
        return ApiError::new(403, "origin_forbidden", "Origin is not allowed.").into_response();
    }
    let token = match token(request.headers()) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    let admitted = async {
        // Bound authentication before opening a database connection. Store only
        // the digest for this accounting; authentication is still checked live.
        let credential = service.credentials.get(crate::hash(token.as_bytes()), || {
            Arc::new(Semaphore::new(2))
        })?;
        let credential_permit = credential
            .as_ref()
            .clone()
            .try_acquire_owned()
            .map_err(|_| busy())?;
        let api = service.api.clone();
        let principal = blocking(service.authentication.clone(), move || {
            let _credential = credential;
            let _permit = credential_permit;
            api.authenticate(&token)
        })
        .await?;
        // Different credentials for one principal share these budgets.
        let principal = service.principals.get(principal.id, PrincipalBudget::new)?;
        let request = principal
            .requests
            .clone()
            .try_acquire_owned()
            .map_err(|_| busy())?;
        let call = service
            .calls
            .clone()
            .try_acquire_owned()
            .map_err(|_| busy())?;
        Ok::<_, ApiError>(Arc::new(Admission {
            principal,
            _request: Some(request),
            _call: Some(call),
        }))
    }
    .await;
    let admission = match admitted {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    request.extensions_mut().insert(admission);
    let transfer = if request.uri().path().starts_with("/v1/blobs/") {
        match service.ingress.clone().try_acquire_owned() {
            Ok(p) => Some(p),
            Err(_) => return busy().into_response(),
        }
    } else {
        None
    };
    // A completion hashes and moves every upload it names.
    let transfer_request = request.uri().path().starts_with("/v1/blobs/")
        || (request.uri().path().starts_with("/v1/revisions/")
            && request.uri().path().ends_with("/bundle"))
        || request.uri().path() == "/v1/executor/complete";
    let deadline = if transfer_request {
        TRANSFER_TIMEOUT_SECONDS
    } else {
        35
    };
    let response =
        match tokio::time::timeout(Duration::from_secs(deadline), next.run(request)).await {
            Ok(response) => response,
            Err(_) => ApiError::new(
                504,
                "request_timeout",
                "Retry a mutation with its original idempotency key.",
            )
            .into_response(),
        };
    drop(transfer);
    response
}

pub fn router(service: Service) -> Router {
    Router::new()
        .route("/v1/commands", post(commands))
        .route("/v1/query", post(queries))
        .route("/v1/executor/profiles", post(executor_profiles))
        .route("/v1/executor/claim", post(executor_claim))
        .route("/v1/executor/renew", post(executor_renew))
        .route(
            "/v1/executor/complete",
            post(executor_complete).layer(DefaultBodyLimit::max(api::MAX_COMPLETE_BODY)),
        )
        .route("/v1/events", get(events))
        .route("/v1/blobs/{sha256}", put(upload))
        .route("/v1/revisions/{revision}/bundle", get(bundle))
        .route("/v1/artifacts/{action}/{sha256}", get(artifact))
        .route("/mcp", post(mcp))
        .fallback(|| async { ApiError::new(404, "not_found", "No such endpoint.") })
        .layer(DefaultBodyLimit::max(2 * 1024 * 1024))
        .layer(middleware::from_fn_with_state(service.clone(), boundary))
        .with_state(service)
}
fn parse_json<T>(
    value: std::result::Result<Json<T>, JsonRejection>,
) -> std::result::Result<T, ApiError> {
    value.map(|Json(v)| v).map_err(|error| {
        ApiError::new(
            error.status().as_u16(),
            "invalid_json",
            "The JSON body does not match this endpoint's schema.",
        )
    })
}
async fn blocking<T: Send + 'static>(
    gate: Arc<Semaphore>,
    work: impl FnOnce() -> std::result::Result<T, ApiError> + Send + 'static,
) -> std::result::Result<T, ApiError> {
    start_blocking(gate, work)?
        .await
        .map_err(|_| ApiError::new(500, "internal", "Application worker failed."))?
}
fn start_blocking<T: Send + 'static>(
    gate: Arc<Semaphore>,
    work: impl FnOnce() -> std::result::Result<T, ApiError> + Send + 'static,
) -> std::result::Result<tokio::task::JoinHandle<std::result::Result<T, ApiError>>, ApiError> {
    let permit = gate.try_acquire_owned().map_err(|_| busy())?;
    // The closure owns capacity even if its HTTP caller disconnects or times out.
    Ok(tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    }))
}

fn admitted_work<T: Send + 'static>(
    service: &Service,
    admission: Arc<Admission>,
    control: bool,
    work: impl FnOnce() -> std::result::Result<T, ApiError> + Send + 'static,
) -> std::result::Result<tokio::task::JoinHandle<std::result::Result<T, ApiError>>, ApiError> {
    let (local, global) = if control {
        (&admission.principal.control, &service.control)
    } else {
        (&admission.principal.work, &service.work)
    };
    let permit = local.clone().try_acquire_owned().map_err(|_| busy())?;
    start_blocking(global.clone(), move || {
        // Preserve both request and execution admission after a disconnect.
        let _admission = admission;
        let _permit = permit;
        work()
    })
}
async fn admitted<T: Send + 'static>(
    service: &Service,
    admission: Arc<Admission>,
    control: bool,
    work: impl FnOnce() -> std::result::Result<T, ApiError> + Send + 'static,
) -> std::result::Result<T, ApiError> {
    admitted_work(service, admission, control, work)?
        .await
        .map_err(|_| ApiError::new(500, "internal", "Application worker failed."))?
}
async fn commands(
    State(service): State<Service>,
    Extension(admission): Extension<Arc<Admission>>,
    headers: HeaderMap,
    body: std::result::Result<Json<Command>, JsonRejection>,
) -> Response {
    let result = async {
        let command = parse_json(body)?;
        let token = token(&headers)?;
        let key = header(&headers, "idempotency-key")?.to_owned();
        let control = matches!(command, Command::Cancel { .. });
        let api = service.api.clone();
        admitted(&service, admission, control, move || {
            api.command(&token, &key, &command)
        })
        .await
    }
    .await;
    match result {
        Ok(value) => (
            StatusCode::from_u16(value["status"].as_u64().unwrap_or(200) as u16)
                .unwrap_or(StatusCode::OK),
            Json(value),
        )
            .into_response(),
        Err(error) => error.into_response(),
    }
}
async fn queries(
    State(service): State<Service>,
    Extension(admission): Extension<Arc<Admission>>,
    headers: HeaderMap,
    body: std::result::Result<Json<Query>, JsonRejection>,
) -> Response {
    let result = async {
        let query = parse_json(body)?;
        let token = token(&headers)?;
        let api = service.api.clone();
        admitted(&service, admission, true, move || api.query(&token, &query)).await
    }
    .await;
    match result {
        Ok(value) => Json(json!({"api_version":"1","result":value})).into_response(),
        Err(e) => e.into_response(),
    }
}

/// The executor routes: one JSON request, one result. Heartbeats and profile descriptions use
/// the control reserve, so saturated work never lets an executor's leases lapse.
async fn executor_call<T: Send + 'static, R: Serialize + Send + 'static>(
    service: Service,
    admission: Arc<Admission>,
    headers: HeaderMap,
    body: std::result::Result<Json<T>, JsonRejection>,
    control: bool,
    call: fn(&Api, &str, &T) -> std::result::Result<R, ApiError>,
) -> Response {
    let result = async {
        let request = parse_json(body)?;
        let token = token(&headers)?;
        let api = service.api.clone();
        admitted(&service, admission, control, move || {
            call(&api, &token, &request)
        })
        .await
    }
    .await;
    match result {
        Ok(value) => Json(json!({"api_version":"1","result":value})).into_response(),
        Err(error) => error.into_response(),
    }
}
async fn executor_profiles(
    State(service): State<Service>,
    Extension(admission): Extension<Arc<Admission>>,
    headers: HeaderMap,
    body: std::result::Result<Json<HelloRequest>, JsonRejection>,
) -> Response {
    executor_call(service, admission, headers, body, true, Api::executor_hello).await
}
async fn executor_claim(
    State(service): State<Service>,
    Extension(admission): Extension<Arc<Admission>>,
    headers: HeaderMap,
    body: std::result::Result<Json<ClaimRequest>, JsonRejection>,
) -> Response {
    executor_call(
        service,
        admission,
        headers,
        body,
        false,
        Api::executor_claim,
    )
    .await
}
async fn executor_renew(
    State(service): State<Service>,
    Extension(admission): Extension<Arc<Admission>>,
    headers: HeaderMap,
    body: std::result::Result<Json<RenewRequest>, JsonRejection>,
) -> Response {
    executor_call(service, admission, headers, body, true, Api::executor_renew).await
}
async fn executor_complete(
    State(service): State<Service>,
    Extension(admission): Extension<Arc<Admission>>,
    headers: HeaderMap,
    body: std::result::Result<Json<CompleteRequest>, JsonRejection>,
) -> Response {
    executor_call(
        service,
        admission,
        headers,
        body,
        false,
        Api::executor_complete,
    )
    .await
}

#[derive(Deserialize)]
struct EventParameters {
    after: Option<i64>,
    project: Option<String>,
    #[serde(default = "event_limit")]
    limit: usize,
}
fn event_limit() -> usize {
    1000
}
async fn events(
    State(service): State<Service>,
    Extension(admission): Extension<Arc<Admission>>,
    headers: HeaderMap,
    parameters: std::result::Result<UrlQuery<EventParameters>, QueryRejection>,
) -> Response {
    let result=async {
        let token=token(&headers)?;
        let UrlQuery(parameters)=parameters.map_err(|e|ApiError::new(e.status().as_u16(),"invalid_query","Invalid event query parameters."))?;
        let header_cursor=headers.get("last-event-id").map(|h|h.to_str().ok().and_then(|s|s.parse::<i64>().ok())
            .ok_or_else(||ApiError::new(400,"invalid_cursor","Last-Event-ID must be an integer."))).transpose()?;
        if header_cursor.is_some() && parameters.after.is_some() && header_cursor!=parameters.after {
            return Err(ApiError::new(400,"cursor_conflict","Header and query cursors differ."));
        }
        let after=header_cursor.or(parameters.after).unwrap_or(0);
        let query=Query::Events {after,project:parameters.project.clone(),limit:parameters.limit};
        let api=service.api.clone(); let credential=token.clone();
        let first=admitted(&service,admission.clone(),true,move||api.query(&credential,&query)).await?;
        let wants_stream=headers.get("accept").and_then(|h|h.to_str().ok()).is_some_and(|h|h.contains("text/event-stream"));
        if !wants_stream { return Ok(Json(json!({"api_version":"1","result":first})).into_response()); }
        let permit=service.streams.clone().try_acquire_owned().map_err(|_|busy())?;
        // Established streams use their own 32-slot budget. Keep the principal
        // record for bounded polling, but release HTTP request admission when
        // headers are returned, preserving the existing stream capacity.
        let admission=Arc::new(Admission {
            principal: admission.principal.clone(), _request: None, _call: None,
        });
        let stream=async_stream::stream! {
            let _permit=permit;
            let mut cursor=after;
            let mut page=first;
            loop {
                let next=page["next_after"].as_i64().unwrap_or(cursor);
                if let Some(rows)=page["events"].as_array() {
                    for row in rows {
                        yield Ok::<_,Infallible>(Event::default().id(row["seq"].to_string()).event("patchweft")
                            .data(row.to_string()));
                    }
                }
                if next!=cursor {
                    // Advance even when rows were filtered by project permissions.
                    yield Ok(Event::default().id(next.to_string()).event("cursor")
                        .data(json!({"after":next}).to_string()));
                }
                cursor=next;
                if page["more"]!=true { tokio::time::sleep(Duration::from_millis(250)).await; }
                let api=service.api.clone(); let credential=token.clone();
                let query=Query::Events {after:cursor,project:parameters.project.clone(),limit:parameters.limit};
                match admitted(&service,admission.clone(),true,move||api.query(&credential,&query)).await {
                    Ok(value)=>page=value,
                    Err(error)=>{
                        yield Ok(Event::default().event("error").data(error.body().to_string()));
                        break;
                    }
                }
            }
        };
        Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(10))).into_response())
    }.await;
    result.unwrap_or_else(IntoResponse::into_response)
}
async fn upload(
    State(service): State<Service>,
    Extension(admission): Extension<Arc<Admission>>,
    Path(sha256): Path<String>,
    request: Request,
) -> Response {
    let result = async {
        let token = token(request.headers())?;
        if request
            .headers()
            .get("content-length")
            .is_some_and(|value| {
                value
                    .to_str()
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .is_some_and(|bytes| bytes > MAX_BUNDLE as u64)
            })
        {
            return Err(upload_body_error(413));
        }
        let transfer = service
            .transfers
            .clone()
            .try_acquire_owned()
            .map_err(|_| busy())?;
        let api = service.api.clone();
        let credential = token.clone();
        let digest = sha256.clone();
        let mut temp = admitted(&service, admission.clone(), false, move || {
            api.begin_upload(&credential, &digest)
        })
        .await?;
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let api = service.api.clone();
        let writer = admitted_work(&service, admission, false, move || {
            let _transfer = transfer;
            receive_upload(receiver, temp.as_file_mut())?;
            api.finish_upload(&token, &sha256, temp)
        })?;
        let received = spool_upload(request.into_body(), sender, MAX_BUNDLE).await;
        if let Err(error) = received
            && error.code != "upload_writer_closed"
        {
            // Dropping the handle does not cancel the disk writer. It owns the
            // staged file and both capacity permits until its last write finishes.
            return Err(error);
        }
        writer
            .await
            .map_err(|_| ApiError::new(500, "internal", "Upload worker failed."))?
    }
    .await;
    match result {
        Ok(v) => Json(json!({"api_version":"1","result":v})).into_response(),
        Err(e) => e.into_response(),
    }
}
fn upload_body_error(status: u16) -> ApiError {
    ApiError::new(
        status,
        "invalid_body",
        "Upload could not be read within its size limit.",
    )
}
enum UploadPart {
    Data(Bytes),
    Complete,
}
fn receive_upload(
    mut receiver: tokio::sync::mpsc::Receiver<UploadPart>,
    file: &mut impl std::io::Write,
) -> std::result::Result<(), ApiError> {
    while let Some(part) = receiver.blocking_recv() {
        match part {
            UploadPart::Data(data) => file.write_all(&data).map_err(ApiError::internal)?,
            UploadPart::Complete => return Ok(()),
        }
    }
    Err(upload_body_error(400))
}
async fn spool_upload(
    mut body: Body,
    sender: tokio::sync::mpsc::Sender<UploadPart>,
    limit: usize,
) -> std::result::Result<(), ApiError> {
    // Direct Body consumption bypasses DefaultBodyLimit: count every data frame,
    // including uploads with no Content-Length, before writing it to disk.
    let mut bytes = 0usize;
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| upload_body_error(400))?;
        if let Ok(data) = frame.into_data() {
            if data.len() > limit - bytes {
                return Err(upload_body_error(413));
            }
            bytes += data.len();
            // Bound queued disk work as well as the total upload size.
            for chunk in data.chunks(65536) {
                sender
                    .send(UploadPart::Data(Bytes::copy_from_slice(chunk)))
                    .await
                    .map_err(|_| upload_writer_closed())?;
            }
        }
    }
    sender
        .send(UploadPart::Complete)
        .await
        .map_err(|_| upload_writer_closed())
}
fn upload_writer_closed() -> ApiError {
    ApiError::new(500, "upload_writer_closed", "Upload writer stopped.")
}
async fn stream_file(
    file: std::fs::File,
    keep: Option<tempfile::TempDir>,
    permit: tokio::sync::OwnedSemaphorePermit,
    admission: Arc<Admission>,
) -> Response {
    let mut file = tokio::fs::File::from_std(file);
    let stream = async_stream::try_stream! {
        let _keep=keep; let _permit=permit; let _admission=admission;
        loop {
            let mut bytes=vec![0u8;65536];
            let count=file.read(&mut bytes).await?;
            if count==0 {break;}
            bytes.truncate(count);
            yield Bytes::from(bytes);
        }
    };
    let stream: std::pin::Pin<
        Box<dyn futures_core::Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send>,
    > = Box::pin(stream);
    (
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        Body::from_stream(stream),
    )
        .into_response()
}
#[derive(Deserialize)]
struct BundleParameters {
    /// Comma-separated commits the caller already holds.
    have: Option<String>,
}
async fn bundle(
    State(service): State<Service>,
    Extension(admission): Extension<Arc<Admission>>,
    headers: HeaderMap,
    Path(revision): Path<String>,
    parameters: std::result::Result<UrlQuery<BundleParameters>, QueryRejection>,
) -> Response {
    let result = async {
        let UrlQuery(parameters) = parameters.map_err(|e| {
            ApiError::new(
                e.status().as_u16(),
                "invalid_query",
                "Invalid bundle query parameters.",
            )
        })?;
        let haves: Vec<String> = parameters
            .have
            .map(|haves| haves.split(',').map(str::to_owned).collect())
            .unwrap_or_default();
        let permit = service
            .transfers
            .clone()
            .try_acquire_owned()
            .map_err(|_| busy())?;
        let token = token(&headers)?;
        let api = service.api.clone();
        let (temp, file, sha256, permit) =
            admitted(&service, admission.clone(), false, move || {
                let (temp, path) = api.source_bundle(&token, &revision, &haves)?;
                let sha256 = crate::file_hash(&path)
                    .map_err(|_| ApiError::new(500, "internal", "Bundle could not be hashed."))?;
                let file = std::fs::File::open(path)
                    .map_err(|_| ApiError::new(500, "internal", "Bundle could not be opened."))?;
                Ok((temp, file, sha256, permit))
            })
            .await?;
        let mut response = stream_file(file, Some(temp), permit, admission).await;
        response
            .headers_mut()
            .insert("x-content-sha256", sha256.parse().expect("digest header"));
        Ok::<_, ApiError>(response)
    }
    .await;
    result.unwrap_or_else(IntoResponse::into_response)
}
async fn artifact(
    State(service): State<Service>,
    Extension(admission): Extension<Arc<Admission>>,
    headers: HeaderMap,
    Path((action, sha256)): Path<(String, String)>,
) -> Response {
    let result = async {
        let permit = service
            .transfers
            .clone()
            .try_acquire_owned()
            .map_err(|_| busy())?;
        let token = token(&headers)?;
        let api = service.api.clone();
        let (file, permit) = admitted(&service, admission.clone(), false, move || {
            Ok((api.artifact(&token, &action, &sha256)?, permit))
        })
        .await?;
        Ok::<_, ApiError>(stream_file(file, None, permit, admission).await)
    }
    .await;
    result.unwrap_or_else(IntoResponse::into_response)
}
async fn mcp(
    State(service): State<Service>,
    Extension(admission): Extension<Arc<Admission>>,
    headers: HeaderMap,
    body: std::result::Result<Json<Value>, JsonRejection>,
) -> Response {
    let result = async {
        let message = parse_json(body)?;
        let token = token(&headers)?;
        let control = mcp_control(&message);
        let api = service.api.clone();
        admitted(&service, admission, control, move || {
            Ok(crate::mcp::http(&api, &token, &headers, message))
        })
        .await
    }
    .await;
    match result {
        Ok((status, Some(body))) => (
            StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Json(body),
        )
            .into_response(),
        Ok((status, None)) => StatusCode::from_u16(status)
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
            .into_response(),
        Err(error) => error.into_response(),
    }
}

fn mcp_control(message: &Value) -> bool {
    message["method"] == "tools/call"
        && (message["params"]["name"] == "patchweft_query"
            || (message["params"]["name"] == "patchweft_command"
                && matches!(
                    serde_json::from_value::<Command>(
                        message["params"]["arguments"]["command"].clone()
                    ),
                    Ok(Command::Cancel { .. })
                )))
}

const PURGE_INTERVAL: Duration = Duration::from_secs(600);

/// Lease expiry (MUST-FIX 2) and upload hygiene (MUST-FIX 3) run only in the API process: a
/// worker's view of time must never expire another host's work.
fn maintain(mut store: Store, started: f64, stop: &AtomicBool) {
    let mut purged: Option<Instant> = None;
    while !stop.load(Ordering::Relaxed) {
        if let Err(error) = authority::expire(&mut store, crate::now(), started) {
            eprintln!("Patchweft lease expiry failed: {error:#}");
        }
        if purged.is_none_or(|last| last.elapsed() >= PURGE_INTERVAL) {
            match api::purge(&store.root, SystemTime::now()) {
                Ok(0) => {}
                Ok(removed) => eprintln!("{}", json!({"event":"uploads_purged","removed":removed})),
                Err(error) => eprintln!("Patchweft upload purge failed: {error:#}"),
            }
            purged = Some(Instant::now());
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

// A peer behind a NAT that drops idle mappings leaves a half-open connection: no FIN or RST
// ever arrives, so without probes the server holds that socket and its descriptor forever while
// the peer reconnects on a fresh one. Probing after a minute idle, every 15 s, closes a dead peer
// after four unanswered probes, about two minutes, while a live idle client costs one small
// packet exchange a minute.
const KEEPALIVE_IDLE: Duration = Duration::from_secs(60);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
const KEEPALIVE_PROBES: u32 = 4;
// macOS names the idle time TCP_KEEPALIVE; Linux, where the API runs, names it TCP_KEEPIDLE.
#[cfg(target_vendor = "apple")]
const KEEPALIVE_IDLE_OPTION: libc::c_int = libc::TCP_KEEPALIVE;
#[cfg(not(target_vendor = "apple"))]
const KEEPALIVE_IDLE_OPTION: libc::c_int = libc::TCP_KEEPIDLE;

/// The listener `serve` accepts on, configured before any connection arrives.
fn bind(listen: SocketAddr) -> std::io::Result<std::net::TcpListener> {
    let listener = std::net::TcpListener::bind(listen)?;
    // Tokio requires nonblocking sockets; otherwise a later accept can stall shutdown.
    listener.set_nonblocking(true)?;
    // Accepted sockets inherit these from the listener, so every connection is probed without
    // touching the accept path (the test checks this on a real accepted socket).
    let tcp = libc::IPPROTO_TCP;
    set_socket_option(&listener, libc::SOL_SOCKET, libc::SO_KEEPALIVE, 1)?;
    set_socket_option(
        &listener,
        tcp,
        KEEPALIVE_IDLE_OPTION,
        KEEPALIVE_IDLE.as_secs() as libc::c_int,
    )?;
    set_socket_option(
        &listener,
        tcp,
        libc::TCP_KEEPINTVL,
        KEEPALIVE_INTERVAL.as_secs() as libc::c_int,
    )?;
    set_socket_option(
        &listener,
        tcp,
        libc::TCP_KEEPCNT,
        KEEPALIVE_PROBES as libc::c_int,
    )?;
    Ok(listener)
}

fn set_socket_option(
    socket: &impl std::os::fd::AsRawFd,
    level: libc::c_int,
    name: libc::c_int,
    value: libc::c_int,
) -> std::io::Result<()> {
    // SAFETY: the descriptor stays open for the call, and the pointer and length describe a c_int.
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            level,
            name,
            (&raw const value).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

pub fn serve(
    root: &FsPath,
    listen: SocketAddr,
    certificate: Option<PathBuf>,
    key: Option<PathBuf>,
    ready: Option<PathBuf>,
    origins: BTreeSet<String>,
) -> Result<Value> {
    ensure!(
        certificate.is_some() == key.is_some(),
        "TLS requires both certificate and private key paths"
    );
    ensure!(
        listen.ip().is_loopback() || certificate.is_some(),
        "non-loopback listeners require TLS"
    );
    ensure!(!origins.contains("*"), "origins must be explicit");
    let api = Arc::new(Api::open(root)?);
    let maintenance = Store::open(root)?;
    let listener = bind(listen)?;
    let address = listener.local_addr()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .max_blocking_threads(64)
        .enable_all()
        .build()?;
    let stop = Arc::new(AtomicBool::new(false));
    let maintainer = {
        let (stop, started) = (stop.clone(), api.started);
        std::thread::Builder::new()
            .name("maintenance".into())
            .spawn(move || maintain(maintenance, started, &stop))?
    };
    let served = runtime.block_on(async move {
        let app = router(Service::new(api, origins));
        let handle = axum_server::Handle::new();
        let stop = handle.clone();
        tokio::spawn(async move {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("signal");
            tokio::select! { _=tokio::signal::ctrl_c()=>{}, _=term.recv()=>{} }
            stop.graceful_shutdown(Some(Duration::from_secs(5)));
        });
        let config = match (certificate, key) {
            (Some(cert), Some(key)) => {
                Some(axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key).await?)
            }
            _ => None,
        };
        if let Some(path) = ready {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)?;
            writeln!(
                file,
                "{}",
                json!({"address":address,"tls":config.is_some(),"pid":std::process::id()})
            )?;
        }
        eprintln!("Patchweft listening on {address}");
        if let Some(config) = config {
            axum_server::from_tcp_rustls(listener, config)?
                .handle(handle)
                .serve(app.into_make_service())
                .await?;
        } else {
            axum_server::from_tcp(listener)?
                .handle(handle)
                .serve(app.into_make_service())
                .await?;
        }
        Ok::<_, anyhow::Error>(json!({"stopped":true}))
    });
    stop.store(true, Ordering::Relaxed);
    let _ = maintainer.join();
    served
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    const FIXTURE_TOKEN: &str = "public-control-admission-fixture";
    const SECOND_TOKEN: &str = "public-control-admission-second-credential";

    fn service_fixture() -> (tempfile::TempDir, Service) {
        let temp = tempfile::tempdir().unwrap();
        let store = crate::store::Store::init(temp.path(), 2, 1024).unwrap();
        let api = Arc::new(Api::open(temp.path()).unwrap());
        let policy = json!({"id":"alice","projects":[],"profiles":[],
            "roles":["read","write"],"max_pending":100});
        store
            .db
            .execute(
                "INSERT INTO api_principals VALUES ('alice',?)",
                [policy.to_string()],
            )
            .unwrap();
        for token in [FIXTURE_TOKEN, SECOND_TOKEN] {
            store
                .db
                .execute(
                    "INSERT INTO api_credentials VALUES (?,'alice')",
                    [crate::hash(token.as_bytes())],
                )
                .unwrap();
        }
        (temp, Service::new(api, BTreeSet::new()))
    }

    fn admission(service: &Service, id: &str) -> Arc<Admission> {
        let principal = service
            .principals
            .get(id.into(), PrincipalBudget::new)
            .unwrap();
        Arc::new(Admission {
            _request: Some(principal.requests.clone().try_acquire_owned().unwrap()),
            _call: Some(service.calls.clone().try_acquire_owned().unwrap()),
            principal,
        })
    }

    async fn fixture_request(service: &Service, token: &str, path: &str, body: Value) -> Response {
        router(service.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .header("idempotency-key", "control-fixture")
                    .header("accept", "application/json, text/event-stream")
                    .header("mcp-protocol-version", crate::mcp::LEGACY)
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn saturated_mutations_leave_http_and_mcp_control_capacity() {
        let (_temp, service) = service_fixture();
        let _ordinary = service.work.clone().acquire_many_owned(24).await.unwrap();
        let mutation = json!({"op":"request","revision":"absent","profile":"absent","job":"check"});
        assert_eq!(
            fixture_request(&service, FIXTURE_TOKEN, "/v1/commands", mutation.clone())
                .await
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            fixture_request(
                &service,
                FIXTURE_TOKEN,
                "/v1/query",
                json!({"op":"projects"})
            )
            .await
            .status(),
            StatusCode::OK
        );
        let cancel = Command::Cancel {
            action: "absent".into(),
        };
        let expected = service
            .api
            .command(FIXTURE_TOKEN, "expected", &cancel)
            .unwrap_err();
        assert_ne!(expected.status, 503);
        assert_eq!(
            fixture_request(
                &service,
                FIXTURE_TOKEN,
                "/v1/commands",
                serde_json::to_value(&cancel).unwrap()
            )
            .await
            .status()
            .as_u16(),
            expected.status
        );
        for (name, arguments) in [
            ("patchweft_query", json!({"op":"projects"})),
            (
                "patchweft_command",
                json!({"idempotency_key":"mcp-cancel","command":cancel}),
            ),
        ] {
            let response = fixture_request(&service, FIXTURE_TOKEN, "/mcp", json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":name,"arguments":arguments}})).await;
            assert_eq!(response.status(), StatusCode::OK);
            let body: Value =
                serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            if name == "patchweft_query" {
                assert_eq!(body["result"]["isError"], false);
            } else {
                assert_eq!(body["result"]["isError"], true);
                assert_eq!(body["result"]["structuredContent"], expected.body());
            }
        }
        let response = fixture_request(&service, FIXTURE_TOKEN, "/mcp", json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"patchweft_command","arguments":{"idempotency_key":"mcp-mutation","command":mutation}}})).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn renew_and_profiles_use_the_control_reserve() {
        const EXECUTOR: &str = "public-executor-admission-fixture";
        let (temp, service) = service_fixture();
        let mut store = crate::store::Store::open(temp.path()).unwrap();
        let policy = json!({"id":"mindy","projects":[],"profiles":[],
            "roles":["read","execute"],"max_pending":1});
        store
            .db
            .execute(
                "INSERT INTO api_principals VALUES ('mindy',?)",
                [policy.to_string()],
            )
            .unwrap();
        store
            .db
            .execute(
                "INSERT INTO api_credentials VALUES (?,'mindy')",
                [crate::hash(EXECUTOR.as_bytes())],
            )
            .unwrap();
        crate::authority::configure(
            &mut store,
            "mindy",
            &crate::authority::ConfigureRequest {
                slots: Some(1),
                memory_mb: Some(1024),
                ..Default::default()
            },
        )
        .unwrap();
        let complete = json!({"attempt":"absent","lease":"l","outcome":"lost","exit_code":125,
            "evidence":{},"drained":false});
        let status = |path: &'static str, body: Value| {
            let service = service.clone();
            async move {
                fixture_request(&service, EXECUTOR, path, body)
                    .await
                    .status()
            }
        };
        let control = || async {
            assert_eq!(
                status("/v1/executor/profiles", json!({})).await,
                StatusCode::OK
            );
            assert_eq!(
                status("/v1/executor/renew", json!({"leases":[]})).await,
                StatusCode::OK
            );
        };
        // Heartbeats keep leases alive while claims and reports wait for ordinary capacity.
        let ordinary = service.work.clone().acquire_many_owned(24).await.unwrap();
        for (path, body) in [
            ("/v1/executor/claim", json!({"claim_key":"k"})),
            ("/v1/executor/complete", complete.clone()),
        ] {
            assert_eq!(
                status(path, body).await,
                StatusCode::SERVICE_UNAVAILABLE,
                "{path}"
            );
        }
        control().await;
        drop(ordinary);
        // The same holds when only this executor's own work budget is spent.
        let principal = service
            .principals
            .get("mindy".into(), PrincipalBudget::new)
            .unwrap();
        let held = principal.work.clone().acquire_many_owned(2).await.unwrap();
        assert_eq!(
            status("/v1/executor/claim", json!({"claim_key":"k"})).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        control().await;
        drop(held);
        assert_eq!(
            status("/v1/executor/complete", complete).await,
            StatusCode::CONFLICT
        );
    }

    #[tokio::test]
    async fn principal_limits_survive_disconnect_and_span_credentials() {
        let (_temp, service) = service_fixture();
        let owner = admission(&service, "alice");
        let mut releases = Vec::new();
        for _ in 0..2 {
            let (release, held) = std::sync::mpsc::channel();
            let (entered, ready) = tokio::sync::oneshot::channel();
            let worker = admitted_work(&service, owner.clone(), false, move || {
                entered.send(()).unwrap();
                held.recv().unwrap();
                Ok(())
            })
            .unwrap();
            ready.await.unwrap();
            // Abandon the join handle, as a disconnected HTTP future does.
            drop(worker);
            releases.push(release);
        }
        let principal = owner.principal.clone();
        drop(owner);
        assert_eq!(principal.work.available_permits(), 0);
        assert_eq!(principal.requests.available_permits(), 7);
        assert_eq!(service.calls.available_permits(), 63);
        for token in [FIXTURE_TOKEN, SECOND_TOKEN] {
            assert_eq!(
                fixture_request(
                    &service,
                    token,
                    "/v1/commands",
                    json!({"op":"request","revision":"absent","profile":"absent","job":"check"})
                )
                .await
                .status(),
                StatusCode::SERVICE_UNAVAILABLE
            );
            assert_eq!(
                fixture_request(&service, token, "/v1/query", json!({"op":"projects"}))
                    .await
                    .status(),
                StatusCode::OK
            );
        }
        let other = admission(&service, "bob");
        admitted(&service, other, false, || Ok(())).await.unwrap();
        for release in releases {
            release.send(()).unwrap();
        }
        let all = tokio::time::timeout(
            Duration::from_secs(5),
            principal.work.clone().acquire_many_owned(2),
        )
        .await
        .unwrap()
        .unwrap();
        drop(all);
        let requests = tokio::time::timeout(
            Duration::from_secs(5),
            principal.requests.clone().acquire_many_owned(8),
        )
        .await
        .unwrap()
        .unwrap();
        drop(requests);
        let calls = tokio::time::timeout(
            Duration::from_secs(5),
            service.calls.clone().acquire_many_owned(64),
        )
        .await
        .unwrap()
        .unwrap();
        drop(calls);
        assert_eq!(principal.requests.available_permits(), 8);
        assert_eq!(service.calls.available_permits(), 64);
    }

    #[test]
    fn admission_registry_is_bounded_and_reclaims_idle_entries() {
        let registry = Budgets::new();
        let mut live = Vec::new();
        for i in 0..256 {
            live.push(registry.get(i.to_string(), || ()).unwrap());
        }
        assert_eq!(registry.get("new".into(), || ()).unwrap_err().status, 503);
        assert!(Arc::ptr_eq(
            &live[0],
            &registry.get("0".into(), || ()).unwrap()
        ));
        live.clear();
        registry.get("new".into(), || ()).unwrap();
        assert_eq!(registry.0.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn streamed_upload_counts_frames_and_rejects_interrupted_bodies() {
        async fn spool(body: Body) -> (std::result::Result<(), ApiError>, tempfile::NamedTempFile) {
            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            let writer = tokio::task::spawn_blocking(move || {
                let mut temp = tempfile::NamedTempFile::new().unwrap();
                let _ = receive_upload(receiver, temp.as_file_mut());
                temp
            });
            let result = spool_upload(body, sender, 8).await;
            (result, writer.await.unwrap())
        }
        let stream = async_stream::stream! {
            yield Ok::<_, std::io::Error>(Bytes::from_static(b"1234"));
            yield Ok(Bytes::from_static(b"5678"));
        };
        let (result, temp) = spool(Body::from_stream(stream)).await;
        result.unwrap();
        assert_eq!(std::fs::read(temp.path()).unwrap(), b"12345678");

        let stream = async_stream::stream! {
            yield Ok::<_, std::io::Error>(Bytes::from_static(b"1234"));
            yield Ok(Bytes::from_static(b"56789"));
        };
        let (result, temp) = spool(Body::from_stream(stream)).await;
        assert_eq!(result.unwrap_err().status, 413);
        assert_eq!(std::fs::read(temp.path()).unwrap(), b"1234");

        let stream = async_stream::stream! {
            yield Ok(Bytes::from_static(b"1234"));
            yield Err(std::io::Error::other("interrupted fixture"));
        };
        let (result, _) = spool(Body::from_stream(stream)).await;
        assert_eq!(result.unwrap_err().status, 400);
    }

    #[tokio::test]
    async fn canceled_upload_holds_capacity_until_a_pending_disk_write_finishes() {
        struct HeldWrite {
            entered: Option<tokio::sync::oneshot::Sender<()>>,
            release: std::sync::mpsc::Receiver<()>,
        }
        impl std::io::Write for HeldWrite {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.entered.take().unwrap().send(()).unwrap();
                self.release.recv().unwrap();
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let gate = Arc::new(Semaphore::new(1));
        let transfers = Arc::new(Semaphore::new(1));
        let transfer = transfers.clone().acquire_owned().await.unwrap();
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (release, hold) = std::sync::mpsc::channel();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let writer = start_blocking(gate.clone(), move || {
            let _transfer = transfer;
            receive_upload(
                receiver,
                &mut HeldWrite {
                    entered: Some(entered),
                    release: hold,
                },
            )
        })
        .unwrap();
        let request = tokio::spawn(async move {
            let stream = async_stream::stream! {
                yield Ok::<_, std::io::Error>(Bytes::from_static(b"held"));
                std::future::pending::<()>().await;
            };
            spool_upload(Body::from_stream(stream), sender, 8).await?;
            writer.await.unwrap()
        });
        ready.await.unwrap();
        request.abort();
        let _ = request.await;
        assert_eq!(gate.available_permits(), 0);
        assert_eq!(transfers.available_permits(), 0);
        release.send(()).unwrap();
        let permit = tokio::time::timeout(Duration::from_secs(5), transfers.acquire())
            .await
            .unwrap()
            .unwrap();
        drop(permit);
        let permit = tokio::time::timeout(Duration::from_secs(5), gate.acquire())
            .await
            .unwrap()
            .unwrap();
        drop(permit);
        assert_eq!(transfers.available_permits(), 1);
        assert_eq!(gate.available_permits(), 1);
    }

    #[tokio::test]
    async fn abandoned_future_holds_capacity_until_its_blocking_work_finishes() {
        let gate = Arc::new(Semaphore::new(1));
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, hold) = std::sync::mpsc::channel();
        let worker_gate = gate.clone();
        let task = tokio::spawn(async move {
            blocking(worker_gate, move || {
                started.send(()).unwrap();
                hold.recv().unwrap();
                Ok(())
            })
            .await
        });
        ready.await.unwrap();
        task.abort();
        let _ = task.await;
        assert_eq!(gate.available_permits(), 0);
        assert_eq!(
            blocking(gate.clone(), || Ok(())).await.unwrap_err().status,
            503
        );
        release.send(()).unwrap();
        let permit = tokio::time::timeout(Duration::from_secs(5), gate.acquire())
            .await
            .unwrap()
            .unwrap();
        drop(permit);
        assert_eq!(gate.available_permits(), 1);
    }

    fn socket_option(socket: &impl std::os::fd::AsRawFd, level: i32, name: i32) -> i32 {
        let mut value: libc::c_int = 0;
        let mut length = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        // SAFETY: the descriptor stays open for the call, and value/length describe a c_int.
        let result = unsafe {
            libc::getsockopt(
                socket.as_raw_fd(),
                level,
                name,
                (&raw mut value).cast(),
                &raw mut length,
            )
        };
        assert_eq!(result, 0, "{}", std::io::Error::last_os_error());
        value
    }

    #[test]
    fn accepted_connections_inherit_the_listener_keepalive() {
        let listener = bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let _client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let accepted = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "no connection to accept");
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("{error}"),
            }
        };
        let tcp = libc::IPPROTO_TCP;
        assert_ne!(
            socket_option(&accepted, libc::SOL_SOCKET, libc::SO_KEEPALIVE),
            0,
            "SO_KEEPALIVE"
        );
        assert_eq!(
            socket_option(&accepted, tcp, KEEPALIVE_IDLE_OPTION),
            KEEPALIVE_IDLE.as_secs() as i32
        );
        assert_eq!(
            socket_option(&accepted, tcp, libc::TCP_KEEPINTVL),
            KEEPALIVE_INTERVAL.as_secs() as i32
        );
        assert_eq!(
            socket_option(&accepted, tcp, libc::TCP_KEEPCNT),
            KEEPALIVE_PROBES as i32
        );
    }
}
