// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use core::mem;
use core::time::Duration;
use std::collections::HashMap;
use std::convert::Infallible;

use anyhow::{Context, anyhow};
use base64::Engine;
use base64::engine::general_purpose;
use bytes::{BufMut, Bytes, BytesMut};
use futures::StreamExt;
use reqwest::header::{HeaderName, HeaderValue};
use reqwest::{Body, Client, Method, Request, RequestBuilder, Response, StatusCode, redirect};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use url::Url;

use super::{Result, SinkError};

const BUFFER_SIZE: usize = 64 * 1024;
const MIN_CHUNK_SIZE: usize = BUFFER_SIZE - 1024;
pub(crate) const DORIS_SUCCESS_STATUS: [&str; 2] = ["Success", "Publish Timeout"];
pub(crate) const STARROCKS_SUCCESS_STATUS: [&str; 1] = ["OK"];
pub(crate) const DORIS_DELETE_SIGN: &str = "__DORIS_DELETE_SIGN__";
pub(crate) const STARROCKS_DELETE_SIGN: &str = "__op";

pub(crate) const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const LOCALHOST: &str = "localhost";
const LOCALHOST_IP: &str = "127.0.0.1";
pub struct HeaderBuilder {
    header: HashMap<String, String>,
}
impl Default for HeaderBuilder {
    fn default() -> Self {
        Self::new()
    }
}
impl HeaderBuilder {
    pub fn new() -> Self {
        Self {
            header: HashMap::default(),
        }
    }

    pub fn add_common_header(mut self) -> Self {
        self.header
            .insert("expect".to_owned(), "100-continue".to_owned());
        self
    }

    /// The method is temporarily not in use, reserved for later use in 2PC.
    /// Doris will generate a default, non-repeating label.
    pub fn set_label(mut self, label: String) -> Self {
        self.header.insert("label".to_owned(), label);
        self
    }

    pub fn set_columns_name(mut self, columns_name: Vec<&str>) -> Self {
        let columns_name_str = columns_name.join(",");
        self.header.insert("columns".to_owned(), columns_name_str);
        self
    }

    /// This method is only called during upsert operations.
    pub fn add_hidden_column(mut self) -> Self {
        self.header
            .insert("hidden_columns".to_owned(), DORIS_DELETE_SIGN.to_owned());
        self
    }

    /// The method is temporarily not in use, reserved for later use in 2PC.
    /// Only use in Doris
    pub fn enable_2_pc(mut self) -> Self {
        self.header
            .insert("two_phase_commit".to_owned(), "true".to_owned());
        self
    }

    pub fn set_user_password(mut self, user: String, password: String) -> Self {
        let auth = format!(
            "Basic {}",
            general_purpose::STANDARD.encode(format!("{}:{}", user, password))
        );
        self.header.insert("Authorization".to_owned(), auth);
        self
    }

    /// The method is temporarily not in use, reserved for later use in 2PC.
    /// Only use in Doris
    pub fn set_txn_id(mut self, txn_id: i64) -> Self {
        self.header
            .insert("txn_operation".to_owned(), txn_id.to_string());
        self
    }

    /// The method is temporarily not in use, reserved for later use in 2PC.
    /// Only use in Doris
    pub fn add_commit(mut self) -> Self {
        self.header
            .insert("txn_operation".to_owned(), "commit".to_owned());
        self
    }

    /// The method is temporarily not in use, reserved for later use in 2PC.
    /// Only use in Doris
    pub fn add_abort(mut self) -> Self {
        self.header
            .insert("txn_operation".to_owned(), "abort".to_owned());
        self
    }

    pub fn add_json_format(mut self) -> Self {
        self.header.insert("format".to_owned(), "json".to_owned());
        self
    }

    /// Only use in Doris
    pub fn add_arrow_format(mut self) -> Self {
        self.header.insert("format".to_owned(), "arrow".to_owned());
        self
    }

    /// Only use in Doris
    pub fn add_read_json_by_line(mut self) -> Self {
        self.header
            .insert("read_json_by_line".to_owned(), "true".to_owned());
        self
    }

    /// Only use in Starrocks
    pub fn add_strip_outer_array(mut self) -> Self {
        self.header
            .insert("strip_outer_array".to_owned(), "true".to_owned());
        self
    }

    /// Only use in Starrocks
    pub fn set_partial_update(mut self, partial_update: Option<String>) -> Self {
        self.header.insert(
            "partial_update".to_owned(),
            partial_update.unwrap_or_else(|| "false".to_owned()),
        );
        self
    }

    /// Only use in Doris
    pub fn set_partial_columns(mut self, partial_columns: Option<String>) -> Self {
        self.header.insert(
            "partial_columns".to_owned(),
            partial_columns.unwrap_or_else(|| "false".to_owned()),
        );
        self
    }

    /// Only use in Doris.
    ///
    /// With strict mode on, a value that fails type conversion is counted as a filtered row
    /// instead of being silently stored as NULL. Combined with the default `max_filter_ratio` of
    /// 0, that turns a bad value into a failed load rather than silent corruption.
    pub fn set_strict_mode(mut self, strict_mode: bool) -> Self {
        self.header
            .insert("strict_mode".to_owned(), strict_mode.to_string());
        self
    }

    /// Only used in Starrocks Transaction API
    pub fn set_db(mut self, db: String) -> Self {
        self.header.insert("db".to_owned(), db);
        self
    }

    /// Only used in Starrocks Transaction API
    pub fn set_table(mut self, table: String) -> Self {
        self.header.insert("table".to_owned(), table);
        self
    }

    pub fn build(self) -> HashMap<String, String> {
        self.header
    }
}

/// Try getting BE url from a redirected response, returning `Ok(None)` indicates this request does
/// not redirect.
///
/// The reason we handle the redirection manually is that if we let `reqwest` handle the redirection
/// automatically, it will remove sensitive headers (such as Authorization) during the redirection,
/// and there's no way to prevent this behavior.
fn try_get_be_url(resp: &Response, fe_host: &str) -> Result<Option<Url>> {
    match resp.status() {
        StatusCode::TEMPORARY_REDIRECT => {
            let be_url = resp
                .headers()
                .get("location")
                .ok_or_else(|| {
                    SinkError::DorisStarrocksConnect(anyhow!("Can't get doris BE url in header",))
                })?
                .to_str()
                .context("Can't get doris BE url in header")
                .map_err(SinkError::DorisStarrocksConnect)?
                .to_owned();

            let mut parsed_be_url = Url::parse(&be_url)
                .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?;

            if fe_host != LOCALHOST && fe_host != LOCALHOST_IP {
                let be_host = parsed_be_url.host_str().ok_or_else(|| {
                    SinkError::DorisStarrocksConnect(anyhow!("Can't get be host from url"))
                })?;

                if be_host == LOCALHOST || be_host == LOCALHOST_IP {
                    // if be host is 127.0.0.1, we may can't connect to it directly,
                    // so replace it with fe host
                    parsed_be_url
                        .set_host(Some(fe_host))
                        .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?;
                }
            }
            Ok(Some(parsed_be_url))
        }
        StatusCode::OK => {
            // Some of the `StarRocks` transactional APIs will respond directly from FE. For example,
            // the request to `/api/transaction/commit` endpoint does not seem to redirect to BE.
            // In this case, the request should be treated as finished.
            Ok(None)
        }
        _ => Err(SinkError::DorisStarrocksConnect(anyhow!(
            "Can't get doris BE url",
        ))),
    }
}

pub struct InserterInnerBuilder {
    url: String,
    header: HashMap<String, String>,
    #[expect(dead_code)]
    sender: Option<Sender>,
    /// The host of `url`. Despite the name, this is only read by [`try_get_be_url`], to rewrite a
    /// BE address that Doris reports as `localhost`. That only happens on the redirect path, where
    /// `url` genuinely does address an FE; when `doris.stream_load_url` points straight at a BE the
    /// preflight answers `200 OK` with no redirect and this field is never read.
    fe_host: String,
    stream_load_http_timeout: Duration,
    /// Set when `url` is known to address a BE, so no FE → BE redirect can arrive and the probe in
    /// [`Self::resolve_stream_load_url`] would be a wasted empty stream load.
    skip_redirect_probe: bool,
    // The `reqwest` crate suggests us reuse the Client, and we don't need make it Arc, because it
    // already uses an Arc internally.
    client: Client,
}
impl InserterInnerBuilder {
    pub fn new(
        url: String,
        db: String,
        table: String,
        header: HashMap<String, String>,
        stream_load_http_timeout_ms: u64,
        skip_redirect_probe: bool,
    ) -> Result<Self> {
        let fe_host = Url::parse(&url)
            .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?
            .host_str()
            .ok_or_else(|| SinkError::DorisStarrocksConnect(anyhow!("Can't get fe host from url")))?
            .to_owned();
        let url = format!("{}/api/{}/{}/_stream_load", url, db, table);
        let stream_load_http_timeout = Duration::from_millis(stream_load_http_timeout_ms);

        let client = Client::builder()
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .redirect(redirect::Policy::none()) // we handle redirect by ourselves
            .build()
            .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?;

        Ok(Self {
            url,
            sender: None,
            header,
            fe_host,
            stream_load_http_timeout,
            skip_redirect_probe,
            client,
        })
    }

    fn build_request(&self, uri: String) -> Result<RequestBuilder> {
        let mut builder = self.client.put(uri);
        for (k, v) in &self.header {
            builder = builder.header(k, v);
        }
        Ok(builder)
    }

    /// Resolve the URL the streaming-body PUT should target, by sending a bodyless preflight PUT
    /// and following the FE → BE 307 if one comes back.
    ///
    /// The probe costs one extra request per stream load. Against an FE that is all it costs, since
    /// the FE answers `307` without starting a load. Against a BE it is worse than wasteful: the BE
    /// answers `200 OK` having actually run an empty stream load, so every commit would consume two
    /// Doris transactions instead of one. `skip_redirect_probe` therefore bypasses it entirely when
    /// the caller already knows `url` addresses a BE.
    async fn resolve_stream_load_url(&self) -> Result<Url> {
        if self.skip_redirect_probe {
            return Url::parse(&self.url)
                .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)));
        }
        let builder = self.build_request(self.url.clone())?;
        let resp = builder
            .send()
            .await
            .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?;

        // The probe only runs when `url` is supposed to address an FE, and a Doris FE always
        // answers `_stream_load` with a `307` to a BE. `try_get_be_url` returning `None` means we
        // got a `200 OK` instead, i.e. the address is really a BE that just executed an *empty*
        // stream load for our bodyless probe. Falling back to `self.url` there would silently
        // commit two Doris transactions (and two table versions) for every commit, so fail loudly.
        try_get_be_url(&resp, &self.fe_host)?.ok_or_else(|| {
            SinkError::DorisStarrocksConnect(anyhow!(
                "the stream load address {} answered without redirecting to a backend, so it does \
                 not address a Doris frontend. Point `doris.url` at a Doris FE HTTP address, and \
                 set `doris.stream_load_url` if loads must go straight to a BE",
                self.url
            ))
        })
    }

    /// Send a complete body as one stream load, with a known `Content-Length`. Doris's Arrow reader
    /// needs a bounded body, so the Arrow path uses this instead of the streaming inserter.
    /// Returns the parsed response, so the caller can log the commit.
    pub async fn send_body(&self, body: Bytes) -> Result<serde_json::Value> {
        let be_url = self.resolve_stream_load_url().await?;
        let builder = self.build_request(be_url.into())?.body(body);

        // Time the whole request (send + response body read), and on timeout abort the task so
        // the connection drops mid-body and the load fails instead of committing after we report
        // failure. Same semantics as `InserterInner::wait_handle`.
        let mut handle: JoinHandle<Result<(StatusCode, Vec<u8>)>> = tokio::spawn(async move {
            let response = builder
                .send()
                .await
                .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?;
            let status = response.status();
            let raw = response
                .bytes()
                .await
                .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?
                .into();
            Ok((status, raw))
        });
        let (status, raw) =
            match tokio::time::timeout(self.stream_load_http_timeout, &mut handle).await {
                Ok(join) => join
                    .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))??,
                Err(_) => {
                    handle.abort();
                    return Err(SinkError::DorisStarrocksConnect(anyhow!(
                        "stream load timed out after {:?}",
                        self.stream_load_http_timeout
                    )));
                }
            };
        if status != StatusCode::OK {
            return Err(SinkError::DorisStarrocksConnect(anyhow!(
                "stream load http error: {:?} {:?}",
                status,
                String::from_utf8(raw).unwrap_or_default()
            )));
        }
        let res: serde_json::Value = serde_json::from_slice(&raw)
            .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;
        let res_status = res
            .get("Status")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        // `Publish Timeout` still means the load committed (Doris just failed to report the
        // publish within its timeout); treating it as a failure would replay already-committed
        // rows and duplicate them on an append-only table. Mirror the JSON path.
        if !DORIS_SUCCESS_STATUS.contains(&res_status) {
            return Err(SinkError::DorisStarrocksConnect(anyhow!(
                "stream load failed: {:?}",
                res
            )));
        }
        // Doris reports rows it silently dropped in `NumberFilteredRows` /
        // `NumberUnselectedRows` while still calling the load a success. Fail loudly here too,
        // like the JSON path's `check_all_rows_loaded`, so `doris.strict_mode` (whose whole
        // effect is to turn a bad value into a filtered row) surfaces a sink error instead of
        // silently losing rows.
        let filtered = res.get("NumberFilteredRows").and_then(|v| v.as_i64()).unwrap_or(0);
        let unselected = res
            .get("NumberUnselectedRows")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        if filtered != 0 || unselected != 0 {
            return Err(SinkError::DorisStarrocksConnect(anyhow!(
                "Doris dropped rows in this stream load: {} filtered, {} unselected. response: {:?}",
                filtered,
                unselected,
                res
            )));
        }
        Ok(res)
    }

}

type Sender = UnboundedSender<Bytes>;

pub struct InserterInner {
    sender: Option<Sender>,
    join_handle: JoinHandle<Result<Vec<u8>>>,
    buffer: BytesMut,
    stream_load_http_timeout: Duration,
}
impl InserterInner {
    pub fn new(
        sender: Sender,
        join_handle: JoinHandle<Result<Vec<u8>>>,
        stream_load_http_timeout: Duration,
    ) -> Self {
        Self {
            sender: Some(sender),
            join_handle,
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
            stream_load_http_timeout,
        }
    }

    async fn send_chunk(&mut self) -> Result<()> {
        if self.sender.is_none() {
            // Unreachable today: `sender` is only cleared by `finish()` (which consumes `self`)
            // and by the error path below (which returns `Err` immediately). Erroring rather than
            // returning `Ok(())` makes sure a future refactor can't turn this into a silent
            // discard of `self.buffer`.
            return Err(SinkError::DorisStarrocksConnect(anyhow!(
                "the stream load request body has already been closed, can't send more data"
            )));
        }

        let chunk = mem::replace(&mut self.buffer, BytesMut::with_capacity(BUFFER_SIZE));

        match self.sender.as_mut().unwrap().send(chunk.freeze()) {
            Err(_e) => {
                self.sender.take();
                self.wait_handle().await?;

                Err(SinkError::DorisStarrocksConnect(anyhow!("channel closed")))
            }
            _ => Ok(()),
        }
    }

    pub async fn write(&mut self, data: Bytes) -> Result<()> {
        self.buffer.put_slice(&data);
        if self.buffer.len() >= MIN_CHUNK_SIZE {
            self.send_chunk().await?;
        }
        Ok(())
    }

    async fn wait_handle(&mut self) -> Result<Vec<u8>> {
        let res = match tokio::time::timeout(self.stream_load_http_timeout, &mut self.join_handle)
            .await
        {
            Ok(res) => res.map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))??,
            Err(err) => {
                // Dropping the handle does not cancel the task, so without this the request would
                // keep running detached and could still commit the load *after* we reported
                // failure — a commit the replay would then duplicate. Aborting drops the
                // connection mid-body, so the server sees a truncated request and fails the load,
                // which makes the replay clean. There is still a race: if the load was already
                // committed and only the response is in flight, we lose it and replay anyway,
                // which is acceptable under at-least-once.
                self.join_handle.abort();
                return Err(SinkError::DorisStarrocksConnect(anyhow!(err)));
            }
        };
        Ok(res)
    }

    pub async fn finish(mut self) -> Result<Vec<u8>> {
        if !self.buffer.is_empty() {
            self.send_chunk().await?;
        }
        self.sender = None;
        self.wait_handle().await
    }
}

impl Drop for InserterInner {
    fn drop(&mut self) {
        // A stream load commits when its request body is closed cleanly. Simply dropping the
        // sender would do exactly that, so an inserter abandoned on an error path would still
        // commit the rows it had already streamed — and those rows are replayed afterwards,
        // duplicating them on a table that does not de-duplicate. Aborting the task drops the
        // connection mid-body instead, so the server sees a truncated request and fails the load,
        // which makes the replay clean. A load that had already committed server-side still lands,
        // which is acceptable under at-least-once.
        //
        // On the success path `finish()` has already awaited `wait_handle()` before dropping
        // `self`, so the task is finished and `abort()` is a no-op.
        self.join_handle.abort();
    }
}

enum StreamLoadResponse {
    BeRequest(Request),
    HttpResponse(Response),
}

/// Send the request and handle redirection if any.
/// The reason we handle the redirection manually is that if we let `reqwest` handle the redirection
/// automatically, it will remove sensitive headers (such as Authorization) during the redirection,
/// and there's no way to prevent this behavior.
/// Please note, the FE address that user specified might be a FE follower not the leader, in this case,
/// the follower FE will redirect request to leader FE and then to BE.
async fn send_stream_load_request(
    client: Client,
    mut request: Request,
    fe_host: &str,
) -> Result<StreamLoadResponse> {
    // possible redirection paths:
    // RW <-> follower FE -> leader FE -> BE
    // RW <-> leader FE -> BE
    // RW <-> leader FE
    for _ in 0..2 {
        let original_http_port = request.url().port();
        let mut request_for_redirection = request
            .try_clone()
            .ok_or_else(|| SinkError::DorisStarrocksConnect(anyhow!("Can't clone request")))?;
        let resp = client.execute(request).await.map_err(|err| {
            SinkError::DorisStarrocksConnect(
                anyhow!(err).context("sending stream load request failed"),
            )
        })?;
        let be_url = try_get_be_url(&resp, fe_host)?;
        match be_url {
            Some(be_url) => {
                // we used an unconventional method to detect if we are currently redirecting to FE leader, i.e.,
                // by comparing the port of the redirected url with that of the original request, if they are same, we consider
                // this is a FE address. Because in practice, no one would deploy their `StarRocks` cluster with the same
                // http port for both FE and BE. However, this is a potentially problematic assumption,
                // we may investigate a better way to do this. For example, we could use the `show backends` command to check
                // if the host of the redirected url is in the list. However, `show backends` requires
                // the system-level privilege, which could break the backward compatibility.
                let redirected_port = be_url.port();
                *request_for_redirection.url_mut() = be_url;
                if redirected_port == original_http_port {
                    // redirected to FE, continue another round.
                    request = request_for_redirection;
                } else {
                    // we got BE address here
                    return Ok(StreamLoadResponse::BeRequest(request_for_redirection));
                }
            }
            None => return Ok(StreamLoadResponse::HttpResponse(resp)),
        }
    }
    Err(SinkError::DorisStarrocksConnect(anyhow!(
        "redirection occur more than twice when sending stream load request"
    )))
}

pub struct MetaRequestSender {
    client: Client,
    request: Request,
    fe_host: String,
}

impl MetaRequestSender {
    pub fn new(client: Client, request: Request, fe_host: String) -> Self {
        Self {
            client,
            request,
            fe_host,
        }
    }

    pub async fn send(self) -> Result<Bytes> {
        match send_stream_load_request(self.client.clone(), self.request, &self.fe_host).await? {
            StreamLoadResponse::BeRequest(be_request) => self
                .client
                .execute(be_request)
                .await
                .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?
                .bytes()
                .await
                .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err))),
            StreamLoadResponse::HttpResponse(resp) => resp
                .bytes()
                .await
                .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err))),
        }
    }
}

pub struct StarrocksTxnRequestBuilder {
    url_begin: String,
    url_load: String,
    url_prepare: String,
    url_commit: String,
    url_rollback: String,
    header: HashMap<String, String>,
    fe_host: String,
    stream_load_http_timeout: Duration,
    // The `reqwest` crate suggests us reuse the Client, and we don't need make it Arc, because it
    // already uses an Arc internally.
    client: Client,
}

impl StarrocksTxnRequestBuilder {
    pub fn new(
        url: String,
        header: HashMap<String, String>,
        stream_load_http_timeout_ms: u64,
    ) -> Result<Self> {
        let fe_host = Url::parse(&url)
            .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?
            .host_str()
            .ok_or_else(|| SinkError::DorisStarrocksConnect(anyhow!("Can't get fe host from url")))?
            .to_owned();

        let url_begin = format!("{}/api/transaction/begin", url);
        let url_load = format!("{}/api/transaction/load", url);
        let url_prepare = format!("{}/api/transaction/prepare", url);
        let url_commit = format!("{}/api/transaction/commit", url);
        let url_rollback = format!("{}/api/transaction/rollback", url);

        let stream_load_http_timeout = Duration::from_millis(stream_load_http_timeout_ms);

        let client = Client::builder()
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .redirect(redirect::Policy::none())
            .build()
            .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?;

        Ok(Self {
            url_begin,
            url_load,
            url_prepare,
            url_commit,
            url_rollback,
            header,
            fe_host,
            stream_load_http_timeout,
            client,
        })
    }

    fn build_request(&self, uri: String, method: Method, label: String) -> Result<Request> {
        let parsed_url =
            Url::parse(&uri).map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?;
        let mut request = Request::new(method, parsed_url);

        if uri != self.url_load {
            // Set timeout for non-load requests; load requests' timeout is controlled by `tokio::timeout`
            *request.timeout_mut() = Some(self.stream_load_http_timeout);
        }

        let header = request.headers_mut();
        for (k, v) in &self.header {
            header.insert(
                HeaderName::try_from(k)
                    .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?,
                HeaderValue::try_from(v)
                    .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?,
            );
        }
        header.insert(
            "label",
            HeaderValue::try_from(label)
                .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?,
        );

        Ok(request)
    }

    pub fn build_begin_request_sender(&self, label: String) -> Result<MetaRequestSender> {
        let request = self.build_request(self.url_begin.clone(), Method::POST, label)?;
        Ok(MetaRequestSender::new(
            self.client.clone(),
            request,
            self.fe_host.clone(),
        ))
    }

    pub fn build_prepare_request_sender(&self, label: String) -> Result<MetaRequestSender> {
        let request = self.build_request(self.url_prepare.clone(), Method::POST, label)?;
        Ok(MetaRequestSender::new(
            self.client.clone(),
            request,
            self.fe_host.clone(),
        ))
    }

    pub fn build_commit_request_sender(&self, label: String) -> Result<MetaRequestSender> {
        let request = self.build_request(self.url_commit.clone(), Method::POST, label)?;
        Ok(MetaRequestSender::new(
            self.client.clone(),
            request,
            self.fe_host.clone(),
        ))
    }

    pub fn build_rollback_request_sender(&self, label: String) -> Result<MetaRequestSender> {
        let request = self.build_request(self.url_rollback.clone(), Method::POST, label)?;
        Ok(MetaRequestSender::new(
            self.client.clone(),
            request,
            self.fe_host.clone(),
        ))
    }

    pub async fn build_txn_inserter(&self, label: String) -> Result<InserterInner> {
        let request = self.build_request(self.url_load.clone(), Method::PUT, label.clone())?;
        let mut be_request =
            match send_stream_load_request(self.client.clone(), request, &self.fe_host).await? {
                StreamLoadResponse::BeRequest(be_request) => be_request,
                StreamLoadResponse::HttpResponse(resp) => {
                    // If we get a response here, it should be from BE, so we extract the URL
                    // and create a new request based on it.
                    let url = resp.url().clone();
                    self.build_request(url.into(), Method::PUT, label)?
                }
            };
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let body = Body::wrap_stream(
            tokio_stream::wrappers::UnboundedReceiverStream::new(receiver).map(Ok::<_, Infallible>),
        );
        *be_request.body_mut() = Some(body);

        let client = self.client.clone();
        let handle: JoinHandle<Result<Vec<u8>>> = tokio::spawn(async move {
            let response = client
                .execute(be_request)
                .await
                .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?;

            let status = response.status();
            let raw = response
                .bytes()
                .await
                .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?
                .into();

            if status == StatusCode::OK {
                Ok(raw)
            } else {
                let response_body = String::from_utf8(raw).map_err(|err| {
                    SinkError::DorisStarrocksConnect(
                        anyhow!(err).context("failed to parse response body"),
                    )
                })?;
                Err(SinkError::DorisStarrocksConnect(anyhow!(
                    "Failed connection {:?},{:?}",
                    status,
                    response_body
                )))
            }
        });
        Ok(InserterInner::new(
            sender,
            handle,
            self.stream_load_http_timeout,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Sending after the body has been closed must be an error. It is unreachable today, but
    /// returning `Ok(())` would silently drop the buffered rows if a refactor ever made it
    /// reachable.
    #[tokio::test]
    async fn test_send_chunk_after_close_is_an_error_not_a_silent_discard() {
        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
        let handle: JoinHandle<Result<Vec<u8>>> =
            tokio::spawn(std::future::ready(Ok(Vec::default())));
        let mut inner = InserterInner::new(sender, handle, Duration::from_secs(1));
        inner.sender = None;
        let err = inner.send_chunk().await.unwrap_err();
        assert!(
            format!("{}", err).contains("already been closed"),
            "got: {err}"
        );
    }
}
