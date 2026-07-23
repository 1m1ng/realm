use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;

use tokio::net::UdpSocket;

pub struct SockMap(RwLock<HashMap<SocketAddr, Arc<UdpSocket>>>);

impl SockMap {
    pub fn new() -> Self {
        Self(RwLock::new(HashMap::new()))
    }

    #[inline]
    pub fn find(&self, addr: &SocketAddr) -> Option<Arc<UdpSocket>> {
        // fetch the lock

        let sockmap = self.0.read().unwrap();

        sockmap.get(addr).cloned()

        // drop the lock
    }

    #[inline]
    pub fn insert(&self, addr: SocketAddr, socket: Arc<UdpSocket>) {
        // fetch the lock
        let mut sockmap = self.0.write().unwrap();

        let _ = sockmap.insert(addr, socket);

        // drop the lock
    }

    #[inline]
    pub fn find_or_insert<E, F>(&self, addr: &SocketAddr, f: F) -> Result<Arc<UdpSocket>, E>
    where
        F: FnOnce() -> Result<Arc<UdpSocket>, E>,
    {
        match self.find(addr) {
            Some(x) => Ok(x),
            None => {
                let socket = f()?;
                self.insert(*addr, Arc::clone(&socket));
                Ok(socket)
            }
        }
    }

    #[inline]
    pub fn remove(&self, addr: &SocketAddr) {
        // fetch the lock
        let mut sockmap = self.0.write().unwrap();

        let _ = sockmap.remove(addr);

        // drop the lock
    }

    /// Drop every association, so that none of them is reused.
    ///
    /// Running association tasks keep their own socket alive until they exit;
    /// this only makes sure no new datagram is routed into a generation that
    /// is going away.
    #[inline]
    pub fn clear(&self) {
        // fetch the lock
        let mut sockmap = self.0.write().unwrap();

        sockmap.clear();

        // drop the lock
    }
}
