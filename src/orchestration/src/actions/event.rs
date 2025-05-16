// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

/////////////////////////////////////////////////////////////////////////////////////////////////////
/// ///  IMPORTANT: This is temporary solution for events handling. This will be re-written later and is done to only support basic integration
////////////////////////////////////////////////////////////////////////////////////////////////////
use iceoryx2::port::listener::Listener;
use iceoryx2::port::notifier::Notifier;
use iceoryx2::prelude::*;
use libc::{poll, pollfd, POLLIN};
use libc::{sched_param, sched_setscheduler, SCHED_FIFO};
use std::collections::HashMap;
use std::process;
use std::sync::{LazyLock, Mutex};
use std::task::Waker;
use std::thread;
use std::time::Duration;

use super::action::ActionResult;
use foundation::prelude::*;

static EVENT_OBJ: LazyLock<Mutex<Event>> = LazyLock::new(|| {
    let mut evts: HashMap<usize, (Listener<ipc::Service>, Option<Waker>, bool)> = HashMap::new();
    // The internal event name shall be unique within the system. Otherwise, when two or more processes running, the trigger will be delivered to all.
    let internal_event_name = "qorix_internal_waker_".to_string() + &process::id().to_string();

    let config = {
        let mut config = Config::default();
        config.global.prefix = "orch_node".try_into().unwrap();
        config
    };

    Node::<ipc::Service>::cleanup_dead_nodes(&config);
    Node::<ipc::Service>::list(&config, |node_state| {
        if let NodeState::<ipc::Service>::Dead(view) = node_state {
            if let Err(e) = view.remove_stale_resources() {
                error!("Failed to clean iceoryx2 resources: {:?}", e);
            }
        }
        CallbackProgression::Continue
    })
    .expect("failed clenup node stall state");

    let name = NodeName::new(&format!("orch_node_{}", process::id())).expect("Broken node name");
    let node = NodeBuilder::new().name(&name).config(&config).create::<ipc::Service>().unwrap();

    let internal_notifier = node
        .service_builder(&internal_event_name.as_str().try_into().unwrap())
        .event()
        .open_or_create()
        .unwrap()
        .notifier_builder()
        .create()
        .unwrap();

    let internal_listener = node
        .service_builder(&internal_event_name.as_str().try_into().unwrap())
        .event()
        .open_or_create()
        .unwrap()
        .listener_builder()
        .create()
        .unwrap();

    evts.insert(0, (internal_listener, None, false));

    Mutex::new(Event {
        service_node: node,
        events: evts,
        internal_notifier,
        notifiers: HashMap::new(),
    })
});

pub struct Event {
    service_node: Node<ipc::Service>,
    // Hash Map: event name (for debugging/logging), waker, bool flag to indicate whether event was received already. It is updated by polling thread.
    events: HashMap<usize, (Listener<ipc::Service>, Option<Waker>, bool)>,
    internal_notifier: Notifier<ipc::Service>,
    notifiers: HashMap<String, Notifier<ipc::Service>>,
}

/// Singleton Event
impl Event {
    /// To get the instance of Event
    pub fn get_instance() -> &'static Mutex<Event> {
        &EVENT_OBJ
    }

    /// To be called when the trigger action is created as part of program
    pub fn create_notifier(&mut self, event_name: &str) {
        let event = self
            .service_node
            .service_builder(&event_name.try_into().unwrap())
            .event()
            .open_or_create()
            .unwrap();

        let notifier = event.notifier_builder().create().unwrap();
        self.notifiers.entry(event_name.to_string()).or_insert(notifier);
    }

    /// To be called when trigger action is executed
    pub fn trigger_event(&self, event_name: &str) -> ActionResult {
        match self.notifiers.get(event_name).unwrap().notify() {
            Ok(_) => Ok(()),
            _ => Err(CommonErrors::GenericError),
        }
    }

    /// To be called when the sync action is created as part of program
    /// This stores the event name. The listeners will be created in polling thread to create waitset & wait for all listeners.
    pub fn create_listener(&mut self, event_name: &str) -> usize {
        let event = self
            .service_node
            .service_builder(&event_name.try_into().unwrap())
            .event()
            .open_or_create()
            .unwrap();

        let listener = event.listener_builder().create().unwrap();

        // The event ID/key starts from 0 to n-1
        let event_id = self.events.len();
        self.events.insert(event_id, (listener, None, false));

        self.internal_notifier.notify().unwrap(); // Will panic if there is error

        // Return key as ID of the listener.
        event_id
    }

    /// This is to check whether the event is received or not
    pub fn check_event(&mut self, event_id: usize) -> bool {
        // Check the event received 'flag' updated by polling thread.
        if let Some((_, _, event_received)) = self.events.get_mut(&event_id) {
            if *event_received {
                *event_received = false;
                debug!("Received event: {}", event_id);
                return true;
            }
        }
        false
    }

    /// To be called when sync action is executed
    pub fn wake_on_event(&mut self, event_id: usize, waker_in: Waker) -> bool {
        if !self.check_event(event_id) {
            if let Some((_, waker, _)) = self.events.get_mut(&event_id) {
                *waker = Some(waker_in);
                debug!("Adding waker for event: {}", event_id);
            }
            // Event was not received
            false
        } else {
            // Event already received.
            true
        }
    }

    pub fn create_polling_thread(&self) {
        let event_obj = Event::get_instance();

        thread::spawn(move || {
            // Set the priority of Event Handler Thread to appropriate higher value for faster response to event.
            // The executable shall have CAP_SYS_NICE capability on Linux / PROCMGR_AID_PRIORITY capability on Qnx for priorities above 63.
            // The CAP_SYS_NICE capability can be enabled by executing "sudo setcap cap_sys_nice=eip <executable_name>" on terminal.
            unsafe {
                let param = sched_param { sched_priority: 50 }; // Priority shall be configurable between 1-99
                let pid = libc::gettid(); // Get thread ID
                let result = sched_setscheduler(pid, SCHED_FIFO, &param); // Policy shall be configurable
                if result == 0 {
                    debug!("[EHT] Thread priority set successfully");
                } else {
                    debug!("[EHT] Failed to set priority");
                }
            }
            debug!("[EHT] Event handler thread running...");

            'outer: loop {
                let mut event_id_and_listener_fd = vec![]; // (event_id, listener fd)
                let mut poll_fds = vec![];
                {
                    let events_map = &event_obj.lock().unwrap().events;
                    for (event_id, (listener, _, _)) in events_map {
                        let listener_fd = unsafe { listener.file_descriptor().native_handle() };
                        event_id_and_listener_fd.push((*event_id, listener_fd));

                        // Create pollfd for each listener fd
                        let poll_fd = pollfd {
                            fd: listener_fd,
                            events: POLLIN,
                            revents: 0,
                        };
                        poll_fds.push(poll_fd);
                    }
                }

                let mut update_poll_fds = false;
                // loops until the user has pressed CTRL+c, the application has received a SIGTERM or SIGINT
                loop {
                    // Blocked poll(), no timeout, return if event or error detected.
                    let result = unsafe { poll(poll_fds.as_mut_ptr(), poll_fds.len() as libc::nfds_t, -1) };

                    if result > 0 {
                        // Some events received, process it.
                        for (index, poll_fd) in poll_fds.iter().enumerate() {
                            if poll_fd.revents & POLLIN == POLLIN {
                                // Read the data and empty it.
                                let mut buf = [0u8; 128];
                                let recv_size = unsafe { libc::recv(poll_fds[index].fd, buf.as_mut_ptr() as *mut _, buf.len(), 0) };

                                if recv_size > 0 {
                                    if let Some((event_id, _)) = event_id_and_listener_fd.get(index) {
                                        if *event_id == 0 {
                                            // Internal event. Just set the flag and process others.
                                            update_poll_fds = true;
                                        } else if let Some((_, waker, event_received)) = event_obj.lock().unwrap().events.get_mut(event_id) {
                                            trace!("[EHT] Received event: {}", event_id);

                                            // Set the flag for not to miss any event. This flag can be checked in wake_on_event function.
                                            *event_received = true;

                                            // If there is any waker configured i.e.sync action is waiting for event, wake it and remove waker.
                                            if let Some(waker_obj) = waker {
                                                trace!("[EHT] Wake awaiting thread using waker for event: {}", event_id);
                                                waker_obj.wake_by_ref();
                                                *waker = None;
                                            }
                                        }
                                    }
                                } else if recv_size == -1 {
                                    // Error, just ignore now.
                                    warn!("Read error, ignored.")
                                }
                            }
                        }
                    } else if result == -1 {
                        // Error, exit thread.
                        break 'outer;
                    } else {
                        // Timeout, this should not happen.
                    }

                    // If there are newly added listeners, then exit inner loop to update poll_fds.
                    if update_poll_fds {
                        break;
                    }
                }
            }

            debug!("[EHT] Exiting event handler thread.");
        });
        // Sleep to ensure tha event handler thread gets a chance to run and change its priority.
        thread::sleep(Duration::from_millis(1));
    }
}
