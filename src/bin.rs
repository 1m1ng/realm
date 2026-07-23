use std::env;
use std::path::PathBuf;

use cfg_if::cfg_if;

use realm::cmd;
use realm::cmd::ControlOpts;
use realm::conf::{Config, DnsConf, EndpointConf, FullConf, LogConf, NetConf};
use realm::core::lifecycle::{
    DesiredEndpoint, EndpointSource, GenerationState, ReconcileRequest, Reconciler, SnapshotStore, derive_id,
};
use realm::{ENV_CONFIG, ENV_CONTROL_SOCKET};

cfg_if! {
    if #[cfg(feature = "mi-malloc")] {
        use mimalloc::MiMalloc;
        #[global_allocator]
        static GLOBAL: MiMalloc = MiMalloc;
    } else if #[cfg(all(feature = "jemalloc", not(target_env = "msvc")))] {
        use jemallocator::Jemalloc;
        #[global_allocator]
        static GLOBAL: Jemalloc = Jemalloc;
    } else if #[cfg(all(feature = "page-alloc", unix))] {
        use mmap_allocator::MmapAllocator;
        #[global_allocator]
        static GLOBAL: MmapAllocator = MmapAllocator::new();
    }
}

/// Report a fatal configuration error and exit, instead of aborting the process.
fn fatal(e: impl std::fmt::Display) -> ! {
    eprintln!("realm: {}", e);
    std::process::exit(1)
}

fn main() {
    let mut control = ControlOpts::default();

    let conf = 'blk: {
        if let Ok(conf_str) = env::var(ENV_CONFIG) {
            if let Ok(conf) = FullConf::from_conf_str(&conf_str) {
                // this path never reaches the argument parser, so the control
                // socket has to come from the environment as well
                control.socket = env::var(ENV_CONTROL_SOCKET).ok().map(PathBuf::from);
                break 'blk conf;
            }
        };

        use cmd::CmdInput;
        match cmd::scan() {
            CmdInput::Endpoint(ep, opts, control_opts) => {
                control = control_opts;
                let mut conf = FullConf::default();
                conf.add_endpoint(ep).apply_global_opts().apply_cmd_opts(opts);
                conf
            }
            CmdInput::Config(conf, opts, control_opts) => {
                control = control_opts;
                let mut conf = FullConf::from_conf_file(&conf).unwrap_or_else(|e| fatal(e));
                conf.apply_global_opts().apply_cmd_opts(opts);
                conf
            }
            CmdInput::None => std::process::exit(0),
        }
    };

    start_from_conf(conf, control);
}

fn start_from_conf(full: FullConf, control: ControlOpts) {
    let FullConf {
        log: log_conf,
        dns: dns_conf,
        network: net_conf,
        endpoints: endpoints_conf,
    } = full;

    // process-wide, one-time initialization: these are frozen for the lifetime
    // of the process and are never part of what a reconcile may change (R35)
    setup_log(log_conf);
    setup_dns(dns_conf);
    setup_transport();

    // the static configuration is generation 0, under ids derived from the
    // listen address and protocols, so that an agent computing the same ids
    // sees its first equivalent submission as unchanged (R5, R26)
    let endpoints: Vec<DesiredEndpoint<EndpointConf>> = endpoints_conf
        .into_iter()
        .map(|conf| {
            let conf = conf.normalized(&net_conf);
            let spec = EndpointSource::build(&conf).unwrap_or_else(|e| fatal(e));
            let id = derive_id(&spec.endpoint.laddr, spec.tcp, spec.udp);
            println!("inited: [{}] {}", id, spec.endpoint);
            DesiredEndpoint { id, spec: conf }
        })
        .collect();

    execute(endpoints, net_conf, control);
}

fn setup_log(log: LogConf) {
    println!("log: {}", &log);

    realm::process::amend(|s| {
        s.log_level = Some(log.level.unwrap_or_default().to_string());
        s.log_output = Some(log.output.clone().unwrap_or_else(|| String::from("stdout")));
    });

    let (level, output) = log.build().unwrap_or_else(|e| fatal(e));
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}]{}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(level)
        .chain(output)
        .apply()
        .unwrap_or_else(|e| panic!("failed to setup logger: {}", &e))
}

fn setup_dns(dns: DnsConf) {
    println!("dns: {}", &dns);

    let (conf, opts) = dns.build().unwrap_or_else(|e| fatal(e));
    realm::core::dns::build_lazy(conf, opts).unwrap_or_else(|e| fatal(e));
}

fn setup_transport() {
    #[cfg(feature = "transport")]
    {
        realm::core::kaminari::install_tls_provider();
    }
}

fn execute(eps: Vec<DesiredEndpoint<EndpointConf>>, global: NetConf, control: ControlOpts) {
    #[cfg(feature = "multi-thread")]
    {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap_or_else(|e| fatal(e))
            .block_on(run(eps, global, control))
    }

    #[cfg(not(feature = "multi-thread"))]
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap_or_else(|e| fatal(e))
            .block_on(run(eps, global, control))
    }
}

async fn run(endpoints: Vec<DesiredEndpoint<EndpointConf>>, global: NetConf, control: ControlOpts) {
    let state_file = control.socket.as_ref().and_then(|_| control.state_file());

    let mut reconciler = match &state_file {
        Some(path) => Reconciler::with_snapshot(SnapshotStore::new(path)),
        None => Reconciler::new(),
    };

    // a snapshot describes what this node was serving when it last ran, which
    // is newer than the static configuration once an agent has taken over
    // (R19); without one, the static configuration is the starting point (R5)
    let restored = match reconciler.restore().await {
        Ok(x) => x,
        Err(e) => {
            // an unreadable snapshot must not keep the process from forwarding:
            // fall back to the static configuration and let the agent converge
            log::error!("failed to restore the last-known-good state: {}", e);
            eprintln!("realm: failed to restore the last-known-good state: {}", e);
            reconciler.set_ready(true);
            Default::default()
        }
    };

    match restored.generation {
        Some(generation) => {
            println!(
                "restored generation {}: {} endpoints, {} failed",
                generation,
                restored.restored,
                restored.failed.len()
            );
            for (id, error) in &restored.failed {
                eprintln!("realm: could not restore [{}]: {}", id, error);
            }
        }
        None => {
            let response = reconciler
                .reconcile(ReconcileRequest {
                    generation: 0,
                    endpoints,
                })
                .await;

            match response {
                Ok(response) => {
                    // a failing endpoint is reported but never takes the
                    // process — or the other endpoints — down (R9, R21)
                    for result in response.results.iter().filter(|r| r.error.is_some()) {
                        eprintln!(
                            "realm: [{}]/{} failed: {}",
                            result.id,
                            result.proto,
                            result.error.as_deref().unwrap_or("unknown error")
                        );
                    }
                    if response.state == GenerationState::PartiallyApplied {
                        eprintln!("realm: some endpoints could not be started, the others are running");
                    }
                }
                Err(e) => fatal(e),
            }
        }
    }

    let handle = reconciler.spawn();

    #[cfg(all(unix, feature = "control"))]
    if let Some(path) = control.socket.clone() {
        use realm::control::ControlServer;
        use realm::core::lifecycle::CancellationToken;

        let server = ControlServer::new(handle.clone(), global, &path);
        let listener = server.bind().await.unwrap_or_else(|e| fatal(e));
        println!("control: {}", path.display());
        if let Some(state) = &state_file {
            println!("state: {}", state.display());
        }
        tokio::spawn(server.serve(listener, CancellationToken::new()));
    }

    #[cfg(not(all(unix, feature = "control")))]
    if control.socket.is_some() {
        fatal("this build has no control plane: rebuild with the `control` feature");
    }

    let _ = &global;
    let _ = &handle;

    // endpoints run in their own tasks; the process stays up until it is
    // stopped from the outside, exactly as before
    std::future::pending::<()>().await
}
