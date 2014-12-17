use std::mem::transmute;
use std::io::timer::{sleep, Timer};
use std::io::net::ip::SocketAddr;
use std::time::duration::Duration;
use std::sync::Future;
use std::ptr::null;
use std::thunk::Thunk;

use libc::*;

use common::*;
use hyperdex::*;
use hyperdex_admin::*;

pub struct Admin {
    ptr: *mut Struct_hyperdex_admin,
    req_tx: Sender<AdminRequest>,
}

pub struct AdminRequest {
    id: int64_t,
    status: Box<u32>,
    success: Option<Thunk>,
    failure: Option<Thunk<HyperError, ()>>,
}

impl Admin {
    pub fn new(coordinator: SocketAddr) -> Result<Admin, String> {
        unsafe {

        let ip_str = format!("{}", coordinator.ip).to_c_str();

        let ptr = hyperdex_admin_create(ip_str.as_ptr(), coordinator.port);
        if ptr.is_null() {
            Err(format!("Could not create hyperdex_admin ({})", coordinator))
        } else {
            let (req_tx, req_rx) = channel();

            spawn(move|| {
                // A list of pending requests
                let mut pending: Vec<AdminRequest> = Vec::new();
                let mut timer = Timer::new().unwrap();

                // We don't want to busy-spin, so we wake up the thread every once in a while
                // to do hyperdex_admin_loop()
                let periodic = timer.periodic(Duration::milliseconds(100));

                let loop_fn = |pending: &mut Vec<AdminRequest>| {
                    if pending.len() == 0 {
                        return;
                    }

                    let mut status = 0;
                    let ret = hyperdex_admin_loop(ptr, -1, &mut status);
                    if ret < 0 {
                        if ret == -1 {
                            return;
                        } else {
                            panic!(format!("the return code was: {}", ret));
                        }
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
                        match *req.status {
                            HYPERDEX_ADMIN_SUCCESS => {
                                if req.success.is_some() {
                                    req.success.unwrap().invoke(());
                                }
                            },
                            _ => {
                                if req.failure.is_some() {
                                    req.failure.unwrap().invoke(get_admin_error(ptr, *req.status));
                                }
                            }
                        }
                    } else if req.failure.is_some() {
                        req.failure.unwrap().invoke(get_admin_error(ptr, status));
                    }
                };

                loop {
                    select!(
                        // Add a new request
                        msg = req_rx.recv_opt() => {
                            match msg {
                                Ok(req) => {
                                    pending.push(req);
                                    loop_fn(&mut pending);
                                },
                                Err(()) => {
                                    // TODO: this is causing trouble for some reason
                                    hyperdex_admin_destroy(ptr);
                                    return;
                                }
                            };
                        },
                        // Wake up and call loop()
                        () = periodic.recv() => {
                            loop_fn(&mut pending);
                        }
                    )
                }
            });

            Ok(Admin {
                ptr: ptr,
                req_tx: req_tx,
            })
        }

        }
    }

    pub fn add_space(&self, desc: &str) -> Result<(), HyperError> {
        self.async_add_space(desc).unwrap()
    }

    pub fn async_add_space(&self, desc: &str) -> Future<Result<(), HyperError>> {
        self.async_add_or_remove_space(desc, "add")
    }

    pub fn remove_space(&self, desc: &str) -> Result<(), HyperError> {
        self.async_remove_space(desc).unwrap()
    }

    pub fn async_remove_space(&self, desc: &str) -> Future<Result<(), HyperError>> {
        self.async_add_or_remove_space(desc, "remove")
    }

    fn async_add_or_remove_space(&self, desc: &str, func: &str) -> Future<Result<(), HyperError>> {
        unsafe {
            let desc_str = desc.to_c_str();
            let mut status_ptr = transmute(box 0u32);
            let (res_tx, res_rx) = channel();
            let req_id = match func {
                "add" => {
                    hyperdex_admin_add_space(self.ptr,
                                             desc_str.as_ptr() as *const i8,
                                             status_ptr)
                },
                "remove" => {
                    hyperdex_admin_rm_space(self.ptr,
                                            desc_str.as_ptr() as *const i8,
                                            status_ptr)
                },
                _ => {
                    panic!("wrong func name");
                }
            };
            if req_id == -1 {
                return Future::from_value(Err(get_admin_error(self.ptr, *status_ptr)))
            }

            let res_tx2 = res_tx.clone();
            let req = AdminRequest {
                id: req_id,
                status: transmute(status_ptr),
                success: Some(Thunk::new(move|| {
                    res_tx.send(Ok(()));
                })),
                failure: Some(Thunk::with_arg(move|err| {
                    res_tx2.send(Err(err));
                })),
            };

            self.req_tx.send(req);

            Future::from_fn(move|| {
                res_rx.recv()
            })
        }
    }

    pub fn dump_config(&self) -> Result<String, HyperError> {
        self.async_dump_config().unwrap()
    }

    pub fn async_dump_config(&self) -> Future<Result<String, HyperError>> {
        self.async_dump_config_or_list_spaces("dump_config")
    }

    pub fn list_spaces(&self) -> Result<String, HyperError> {
        self.async_list_spaces().unwrap()
    }

    pub fn async_list_spaces(&self) -> Future<Result<String, HyperError>> {
        self.async_dump_config_or_list_spaces("list_spaces")
    }

    fn async_dump_config_or_list_spaces(&self, func: &str) -> Future<Result<String, HyperError>> {
        unsafe {
            let mut status_ptr = transmute(box 0u32);
            let mut res_ptr = transmute(box null::<*const i8>());

            let (res_tx, res_rx) = channel();
            let req_id = match func {
                "dump_config" => {
                    hyperdex_admin_dump_config(self.ptr, status_ptr, res_ptr)
                },
                "list_spaces" => {
                    hyperdex_admin_list_spaces(self.ptr, status_ptr, res_ptr)
                },
                _ => {
                    panic!("wrong func name");
                }
            };
            if req_id == -1 {
                return Future::from_value(Err(get_admin_error(self.ptr, *status_ptr)));
            }

            let res_tx2 = res_tx.clone();
            let req = AdminRequest {
                id: req_id,
                status: transmute(status_ptr),
                success: Some(Thunk::new(move|| {
                    let res_box: Box<*const i8> = transmute(res_ptr);
                    let res = to_string(*res_box);
                    res_tx.send(Ok(res));
                })),
                failure: Some(Thunk::with_arg(move|err| {
                    res_tx2.send(Err(err));
                })),
            };

            self.req_tx.send(req);

            Future::from_fn(move|| {
                res_rx.recv()
            })
        }
    }
}

// impl Drop for Admin {
    // fn drop(&mut self) {
        // unsafe {
            // hyperdex_admin_destroy(self.ptr);
        // }
    // }
// }

