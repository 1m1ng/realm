//! Control-plane request handling.
//!
//! HTTP/1.1 + JSON over a unix domain socket. The shapes here are the contract
//! an agent programs against, so they carry explicit versioning
//! (`schema_version`) and an explicit retryable/terminal classification on
//! every error (R31, R32).

use std::convert::Infallible;
use std::time::Duration;

use http_body_util::{BodyExt, Full, Limited};
use hyper::body::{Bytes, Incoming};
use hyper::{Method, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};

use realm_core::lifecycle::{
    DesiredEndpoint, EndpointStatus, GenerationState, Proto, ReconcileError, ReconcileHandle, ReconcileRequest,
    SlotAction, SlotState,
};

use crate::VERSION;
use crate::conf::{EndpointConf, NetConf};
use crate::consts::FEATURES;

/// Version of the request/response contract (R32).
pub const SCHEMA_VERSION: u32 = 1;

/// What this build can do, for capability probing (R22, R32).
pub const CAPABILITIES: &[&str] = &[
    "desired-state-reconcile",
    "stable-endpoint-ids",
    "per-protocol-results",
    "connection-preserving-tcp-update",
    "udp-association-rebuild",
    "per-endpoint-drain-timeout",
    "draining-cohort-status",
    "snapshot-restore",
];

/// Largest request body accepted, so the control plane cannot be made to
/// allocate without bound (R12).
pub const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

// ---------------------------------------------------------------- requests --

#[derive(Debug, Deserialize)]
struct DesiredStateDto {
    generation: u64,
    #[serde(default)]
    endpoints: Vec<DesiredEndpointDto>,
}

#[derive(Debug, Deserialize)]
struct DesiredEndpointDto {
    /// caller-provided stable key (R7)
    id: String,
    #[serde(flatten)]
    conf: EndpointConf,
}

// --------------------------------------------------------------- responses --

#[derive(Debug, Serialize)]
struct ReconcileResponseDto {
    generation: u64,
    state: &'static str,
    results: Vec<EndpointResultDto>,
}

#[derive(Debug, Serialize)]
struct EndpointResultDto {
    id: String,
    protocol: &'static str,
    action: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    /// present only on a failure: whether resubmitting may succeed (R31)
    #[serde(skip_serializing_if = "Option::is_none")]
    retryable: Option<bool>,
}

#[derive(Debug, Serialize)]
struct StatusDto {
    #[serde(skip_serializing_if = "Option::is_none")]
    active_generation: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    generation_state: Option<&'static str>,
    ready: bool,
    endpoints: Vec<EndpointStatusDto>,
    process: ProcessDto,
}

#[derive(Debug, Serialize)]
struct EndpointStatusDto {
    id: String,
    slots: Vec<SlotStatusDto>,
}

#[derive(Debug, Serialize)]
struct SlotStatusDto {
    protocol: &'static str,
    state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    generation: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    listen: Option<String>,
    connections: usize,
    draining: Vec<DrainingDto>,
}

#[derive(Debug, Serialize)]
struct DrainingDto {
    generation: u64,
    connections: usize,
    age_secs: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    draining_for_secs: Option<f64>,
}

/// Process-wide settings an agent cannot change at runtime (R35).
#[derive(Debug, Serialize)]
struct ProcessDto {
    version: &'static str,
    features: String,
    dns: Option<DnsDto>,
}

#[derive(Debug, Serialize)]
struct DnsDto {
    nameservers: Vec<String>,
}

#[derive(Debug, Serialize)]
struct VersionDto {
    implementation: &'static str,
    version: &'static str,
    schema_version: u32,
    capabilities: &'static [&'static str],
    features: String,
}

#[derive(Debug, Serialize)]
struct ReadinessDto {
    ready: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_generation: Option<u64>,
    partial: bool,
}

#[derive(Debug, Serialize)]
struct ErrorDto {
    error: ErrorBodyDto,
}

#[derive(Debug, Serialize)]
struct ErrorBodyDto {
    kind: &'static str,
    /// whether retrying the very same request may succeed (R31)
    retryable: bool,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_generation: Option<u64>,
}

// ----------------------------------------------------------------- routing --

/// Everything a request handler needs.
#[derive(Clone)]
pub struct ApiState {
    pub reconciler: ReconcileHandle<EndpointConf>,
    /// process-wide network defaults, applied before diffing (KTD3)
    pub global: NetConf,
}

type BoxBody = Full<Bytes>;

pub async fn handle(state: ApiState, req: Request<Incoming>) -> Result<Response<BoxBody>, Infallible> {
    let path = req.uri().path().to_string();
    let method = req.method().clone();

    let response = match (&method, path.as_str()) {
        (&Method::PUT, "/v1/desired-state") | (&Method::POST, "/v1/desired-state") => reconcile(state, req).await,
        (&Method::GET, "/v1/status") => status(state).await,
        (&Method::GET, "/v1/readiness") => readiness(state).await,
        (&Method::GET, "/v1/version") | (&Method::GET, "/v1/capabilities") => version(),
        (&Method::GET, _) | (&Method::PUT, _) | (&Method::POST, _) => error(
            StatusCode::NOT_FOUND,
            "unknown-route",
            false,
            format!("no route for {} {}", method, path),
            None,
        ),
        _ => error(
            StatusCode::METHOD_NOT_ALLOWED,
            "method-not-allowed",
            false,
            format!("{} is not allowed on {}", method, path),
            None,
        ),
    };

    Ok(response)
}

async fn reconcile(state: ApiState, req: Request<Incoming>) -> Response<BoxBody> {
    let body = match Limited::new(req.into_body(), MAX_BODY_BYTES).collect().await {
        Ok(x) => x.to_bytes(),
        Err(e) => {
            return error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request-too-large",
                false,
                format!("request body exceeds {} bytes: {}", MAX_BODY_BYTES, e),
                None,
            );
        }
    };

    let dto: DesiredStateDto = match serde_json::from_slice(&body) {
        Ok(x) => x,
        Err(e) => {
            return error(
                StatusCode::BAD_REQUEST,
                "malformed-request",
                false,
                format!("cannot parse the desired state: {}", e),
                None,
            );
        }
    };

    // audit trail: what was asked for, never how the transport is configured
    // (which carries keys, passwords and certificate paths) (R12)
    log::info!(
        "[control]reconcile generation {} with {} endpoints: {}",
        dto.generation,
        dto.endpoints.len(),
        dto.endpoints
            .iter()
            .map(|e| format!("{}@{}", e.id, e.conf.listen))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let request = ReconcileRequest {
        generation: dto.generation,
        endpoints: dto
            .endpoints
            .into_iter()
            .map(|e| DesiredEndpoint {
                id: e.id,
                spec: e.conf.normalized(&state.global),
            })
            .collect(),
    };

    match state.reconciler.reconcile(request).await {
        Ok(response) => {
            let state_str = match response.state {
                GenerationState::Applied => "applied",
                GenerationState::PartiallyApplied => "partially-applied",
            };

            log::info!(
                "[control]generation {} {}: {} results",
                response.generation,
                state_str,
                response.results.len()
            );

            json(
                StatusCode::OK,
                &ReconcileResponseDto {
                    generation: response.generation,
                    state: state_str,
                    results: response
                        .results
                        .into_iter()
                        .map(|r| EndpointResultDto {
                            id: r.id,
                            protocol: proto_str(r.proto),
                            action: action_str(r.action),
                            error: r.error,
                            retryable: r.retryable,
                        })
                        .collect(),
                },
            )
        }
        Err(e) => reconcile_error(e),
    }
}

fn reconcile_error(e: ReconcileError) -> Response<BoxBody> {
    let message = e.to_string();
    match e {
        ReconcileError::Stale { active } => {
            error(StatusCode::CONFLICT, "stale-generation", false, message, Some(active))
        }
        ReconcileError::NotReady => error(StatusCode::SERVICE_UNAVAILABLE, "not-ready", true, message, None),
        ReconcileError::Internal(_) => error(StatusCode::INTERNAL_SERVER_ERROR, "internal", true, message, None),
    }
}

async fn status(state: ApiState) -> Response<BoxBody> {
    let endpoints = state.reconciler.status().await;
    let (active_generation, partial, ready) = state.reconciler.generation().await;

    json(
        StatusCode::OK,
        &StatusDto {
            active_generation,
            generation_state: active_generation.map(|_| if partial { "partially-applied" } else { "applied" }),
            ready,
            endpoints: endpoints.into_iter().map(endpoint_status).collect(),
            process: process_status(),
        },
    )
}

fn endpoint_status(status: EndpointStatus) -> EndpointStatusDto {
    EndpointStatusDto {
        id: status.id,
        slots: status
            .slots
            .into_iter()
            .map(|slot| SlotStatusDto {
                protocol: proto_str(slot.proto),
                state: state_str(&slot.state),
                error: match &slot.state {
                    SlotState::Failed(e) => Some(e.clone()),
                    _ => None,
                },
                generation: slot.generation,
                listen: slot.laddr.map(|a| a.to_string()),
                connections: slot.connections,
                draining: slot
                    .draining
                    .into_iter()
                    .map(|d| DrainingDto {
                        generation: d.generation,
                        connections: d.connections,
                        age_secs: secs(d.age),
                        draining_for_secs: d.draining_for.map(secs),
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn process_status() -> ProcessDto {
    ProcessDto {
        version: VERSION,
        features: FEATURES.to_string(),
        dns: realm_core::dns::effective_conf().map(|conf| DnsDto {
            nameservers: conf.conf.name_servers().iter().map(|ns| ns.ip.to_string()).collect(),
        }),
    }
}

async fn readiness(state: ApiState) -> Response<BoxBody> {
    let (active_generation, partial, ready) = state.reconciler.generation().await;

    let code = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    json(
        code,
        &ReadinessDto {
            ready,
            active_generation,
            partial,
        },
    )
}

fn version() -> Response<BoxBody> {
    json(
        StatusCode::OK,
        &VersionDto {
            implementation: "realm-hot-reload-fork",
            version: VERSION,
            schema_version: SCHEMA_VERSION,
            capabilities: CAPABILITIES,
            features: FEATURES.to_string(),
        },
    )
}

// ----------------------------------------------------------------- helpers --

fn secs(d: Duration) -> f64 {
    d.as_secs_f64()
}

fn proto_str(proto: Proto) -> &'static str {
    match proto {
        Proto::Tcp => "tcp",
        Proto::Udp => "udp",
    }
}

fn action_str(action: SlotAction) -> &'static str {
    match action {
        SlotAction::Unchanged => "unchanged",
        SlotAction::Created => "created",
        SlotAction::Updated => "updated",
        SlotAction::Draining => "draining",
        SlotAction::Deleted => "deleted",
        SlotAction::Failed => "failed",
    }
}

fn state_str(state: &SlotState) -> &'static str {
    match state {
        SlotState::Validating => "validating",
        SlotState::Binding => "binding",
        SlotState::Running => "running",
        SlotState::Draining => "draining",
        SlotState::Stopped => "stopped",
        SlotState::Failed(_) => "failed",
    }
}

fn json<T: Serialize>(code: StatusCode, body: &T) -> Response<BoxBody> {
    let data = match serde_json::to_vec(body) {
        Ok(x) => x,
        Err(e) => {
            log::error!("[control]failed to serialize a response: {}", e);
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from_static(
                    br#"{"error":{"kind":"internal","retryable":true,"message":"failed to serialize the response"}}"#,
                )))
                .unwrap_or_default();
        }
    };

    Response::builder()
        .status(code)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(data)))
        .unwrap_or_default()
}

fn error(
    code: StatusCode,
    kind: &'static str,
    retryable: bool,
    message: String,
    active_generation: Option<u64>,
) -> Response<BoxBody> {
    log::warn!("[control]{} ({}): {}", kind, code.as_u16(), message);

    json(
        code,
        &ErrorDto {
            error: ErrorBodyDto {
                kind,
                retryable,
                message,
                active_generation,
            },
        },
    )
}
