use std::io::timer::Timer;
use std::io::net::ip::SocketAddr;
use std::time::duration::Duration;

use libc::*;

use super::*;
use common::*;
use hyperdex::*;
use hyperdex_admin::*;

pub struct Admin {
    req_tx: Sender<AdminRequest>,
    shutdown_tx: Sender<()>,
}

pub struct AdminRequest {
    id: int64_t,
    status: *mut int64_t,
    success: Option<proc(): Send>,
    failure: Option<proc(err: HyperError): Send>,
}

impl Admin {
    pub unsafe fn new(coordinator: SocketAddr) -> Result<Admin, String> {
        let ip = format!("{}", coordinator.ip).to_c_str().as_ptr();
        let port = coordinator.port;

        let ptr = hyperdex_admin_create(ip, port);
        if ptr.is_null() {
            Err(format!("Could not create hyperdex_admin (ip={}, port={})", ip, port))
        } else {
            let (req_tx, req_rx) = channel();
            let (shutdown_tx, shutdown_rx) = channel();

            spawn(proc() {
                // A list of pending requests
                let mut pending: Vec<AdminRequest> = Vec::new();
                let mut timer = Timer::new().unwrap();

                // We don't want to busy-spin, so we wake up the thread every once in a while
                // to do hyperdex_admin_loop()
                let periodic = timer.periodic(Duration::milliseconds(100));

                let loop_fn = |pending: &mut Vec<AdminRequest>| {
                    let mut status = 0;
                    let ret = hyperdex_admin_loop(ptr, -1, &mut status);
                    if ret < 0 {
                        fail!("HyperDex admin error");  // TODO: better error handling
                    }
                    let req_index = pending.iter().position(|req| {
                        if req.id == ret {
                            true
                        } else {
                            false
                        }
                    }).unwrap();  // TODO: better error handling
                    let req = pending.remove(req_index).unwrap();

                    if status == HYPERDEX_ADMIN_SUCCESS {
                        match *req.status as u32 {
                            HYPERDEX_ADMIN_SUCCESS => {
                                if req.success.is_some() {
                                    req.success.unwrap()();
                                }
                            },
                            _ => {
                                if req.failure.is_some() {
                                    req.failure.unwrap()(HyperError {
                                        status: *req.status as u32,
                                        message: to_string(hyperdex_admin_error_message(ptr)),
                                        location: to_string(hyperdex_admin_error_location(ptr)),
                                    });
                                }
                            }
                        }
                    } else if req.failure.is_some() {
                        req.failure.unwrap()(HyperError {
                            status: status,
                            message: to_string(hyperdex_admin_error_message(ptr)),
                            location: to_string(hyperdex_admin_error_location(ptr)),
                        });
                    }
                };

                loop {
                    select!(
                        // Shutdown the thread
                        () = shutdown_rx.recv() => {
                            hyperdex_admin_destroy(ptr);
                            return;
                        },
                        // Add a new request
                        req = req_rx.recv() => {
                            pending.push(req);
                            loop_fn(&mut pending);
                        },
                        // Wake up and call loop()
                        () = periodic.recv() => {
                            loop_fn(&mut pending);
                        }
                    )
                }
            });

            Ok(Admin {
                req_tx: req_tx,
                shutdown_tx: shutdown_tx,
            })
        }
    }
}
