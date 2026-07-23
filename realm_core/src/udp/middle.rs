use std::io::Result;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;

use super::SockMap;
use super::{socket, batched};

use crate::time::timeoutfut;
use crate::dns::resolve_addr;
use crate::endpoint::UdpRuntime;
use crate::lifecycle::{CohortHandle, ConnGuard};

use batched::{Packet, SockAddrStore};
use registry::Registry;
mod registry {
    use super::*;
    type Range = std::ops::Range<u16>;

    pub struct Registry {
        pkts: Box<[Packet]>,
        groups: Vec<Range>,
        cursor: u16,
    }

    impl Registry {
        pub fn new(npkts: usize) -> Self {
            debug_assert!(npkts <= batched::MAX_PACKETS);
            Self {
                pkts: vec![Packet::new(); npkts].into_boxed_slice(),
                groups: Vec::with_capacity(npkts),
                cursor: 0u16,
            }
        }

        pub async fn batched_recv_on(&mut self, sock: &UdpSocket) -> Result<()> {
            let n = batched::recv_some(sock, &mut self.pkts).await?;
            self.cursor = n as u16;
            Ok(())
        }

        pub fn group_by_addr(&mut self) {
            let n = self.cursor as usize;
            self.groups.clear();
            group_by_inner(&mut self.pkts[..n], &mut self.groups, |a, b| a.addr == b.addr);
        }

        pub fn group_iter(&self) -> GroupIter<'_> {
            GroupIter {
                pkts: &self.pkts,
                ranges: self.groups.iter(),
            }
        }

        pub fn iter(&self) -> std::slice::Iter<'_, Packet> {
            self.pkts[..self.cursor as usize].iter()
        }

        pub const fn count(&self) -> usize {
            self.cursor as usize
        }
    }

    use std::slice::Iter;
    use std::iter::Iterator;
    pub struct GroupIter<'a> {
        pkts: &'a [Packet],
        ranges: Iter<'a, Range>,
    }

    impl<'a> Iterator for GroupIter<'a> {
        type Item = &'a [Packet];

        fn next(&mut self) -> Option<Self::Item> {
            self.ranges
                .next()
                .map(|Range { start, end }| &self.pkts[*start as usize..*end as usize])
        }
    }

    fn group_by_inner<T, F>(data: &mut [T], groups: &mut Vec<Range>, eq: F)
    where
        F: Fn(&T, &T) -> bool,
    {
        let maxn = data.len();
        let (mut beg, mut end) = (0, 1);
        while end < maxn {
            // go ahead if addr is same
            if eq(&data[end], &data[beg]) {
                end += 1;
                continue;
            }
            // pick packets afterwards
            let mut probe = end + 1;
            while probe < maxn {
                if eq(&data[probe], &data[beg]) {
                    data.swap(probe, end);
                    end += 1;
                }
                probe += 1;
            }
            groups.push(beg as _..end as _);
            (beg, end) = (end, end + 1);
        }
        groups.push(beg as _..end as _);
    }
}

/// Receive a batch of datagrams from the entry socket and forward them,
/// creating the association tasks that route the answers back.
///
/// Everything an association needs is handed to it as an owned `Arc`, so it
/// stays valid even after this loop — and the endpoint generation it belongs
/// to — is gone.
pub async fn associate_and_relay(
    lis: &Arc<UdpSocket>,
    runtime: &Arc<UdpRuntime>,
    sockmap: &Arc<SockMap>,
    cohort: &CohortHandle,
) -> Result<()> {
    let mut registry = Registry::new(batched::MAX_PACKETS);

    registry.batched_recv_on(lis).await?;
    log::debug!("[udp]entry batched recvfrom[{}]", registry.count());

    let raddr = resolve_addr(&runtime.raddr)
        .await?
        .iter()
        .next()
        .ok_or_else(|| std::io::Error::other(format!("{} resolved to no address", runtime.raddr)))?;
    log::debug!("[udp]{} resolved as {}", runtime.raddr, raddr);

    registry.group_by_addr();
    for pkts in registry.group_iter() {
        let laddr = pkts[0].addr.clone().into();
        let rsock = sockmap.find_or_insert(&laddr, || {
            let s = Arc::new(socket::associate(&raddr, &runtime.conn_opts)?);
            tokio::spawn(send_back(
                Arc::clone(lis),
                laddr,
                Arc::clone(&s),
                Arc::clone(runtime),
                Arc::clone(sockmap),
                cohort.register(),
            ));
            log::info!("[udp]new association {} => {} as {}", laddr, runtime.raddr, raddr);
            Result::Ok(s)
        })?;
        let raddr: SockAddrStore = raddr.into();
        batched::send_all(&rsock, pkts.iter().map(|x| x.ref_with_addr(&raddr))).await?;
    }

    Ok(())
}

/// Route answers of one association back to the client.
///
/// This task can outlive the receive loop that spawned it, so it observes
/// cancellation itself instead of relying on a parent task going away.
async fn send_back(
    lsock: Arc<UdpSocket>,
    laddr: SocketAddr,
    rsock: Arc<UdpSocket>,
    runtime: Arc<UdpRuntime>,
    sockmap: Arc<SockMap>,
    guard: ConnGuard,
) {
    let mut registry = Registry::new(batched::MAX_PACKETS);
    let timeout = runtime.conn_opts.associate_timeout;
    let laddr_s: SockAddrStore = laddr.into();

    loop {
        let received = tokio::select! {
            biased;
            _ = guard.token().cancelled() => {
                log::debug!("[udp]association for {} cancelled", laddr);
                break;
            }
            res = timeoutfut(registry.batched_recv_on(&rsock), timeout) => res,
        };

        match received {
            Err(_) => {
                log::debug!("[udp]rear recvfrom timeout");
                break;
            }
            Ok(Err(e)) => {
                log::error!("[udp]rear recvfrom failed: {}", e);
                break;
            }
            Ok(Ok(())) => {
                log::debug!("[udp]rear batched recvfrom[{}]", registry.count())
            }
        };

        let pkts = registry.iter().map(|pkt| pkt.ref_with_addr(&laddr_s));

        let sent = tokio::select! {
            biased;
            _ = guard.token().cancelled() => {
                log::debug!("[udp]association for {} cancelled", laddr);
                break;
            }
            res = batched::send_all(&lsock, pkts) => res,
        };

        if let Err(e) = sent {
            log::error!("[udp]failed to sendto client{}: {}", &laddr, e);
            break;
        }
    }

    sockmap.remove(&laddr);
    log::debug!("[udp]remove association for {}", &laddr);

    // released last: the cohort reports this association as gone only once the
    // task has actually finished and its socket is about to be dropped
    drop(guard);
}
