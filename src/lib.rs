use std::unimplemented;

use serde::{Deserialize, Serialize};

struct Address {
    conn_string: String,
}

trait Actor {
    type Message;

    fn new(zmq_ctx: zmq::Context, address: &Address) -> Self;
    fn run(&mut self);
    fn dispatch_message(&mut self, message: Self::Message) -> ShouldTerminate;
}

struct ShouldTerminate(bool);

#[derive(Serialize, Deserialize)]
enum TestWorkerMessage {
    MessageA,
    MessageB(u8, String),
    MessageC { c_foo: u64, c_bar: String },
}

struct TestWorker {
    socket: zmq::Socket,
}

impl Actor for TestWorker {
    type Message = TestWorkerMessage;

    fn new(zmq_ctx: zmq::Context, address: &Address) -> Self {
        let socket = zmq_ctx
            .socket(zmq::PULL)
            .expect("Cannot create pull socket");
        let address = &address.conn_string;
        socket.bind(&address).expect("Cannot bind pull socket");
        Self { socket }
    }

    fn run(&mut self) {
        loop {
            let message_bytes = self
                .socket
                .recv_bytes(0)
                .expect("Actor cannot receive message bytes");
            let message: Self::Message =
                bincode::deserialize(&message_bytes).expect("Actor cannot deserialize message");
            if self.dispatch_message(message).0 {
                break;
            }
        }
    }

    fn dispatch_message(&mut self, message: Self::Message) -> ShouldTerminate {
        match message {
            Self::Message::MessageA => self.handle_message_a(),
            Self::Message::MessageB(val1, val2) => self.handle_message_b((val1, val2)),
            Self::Message::MessageC { c_foo, c_bar } => self.handle_message_c(c_foo, c_bar),
        }
    }
}

impl TestWorker {
    fn handle_message_a(&mut self) -> ShouldTerminate {
        ShouldTerminate(true)
    }

    fn handle_message_b(&mut self, params: (u8, String)) -> ShouldTerminate {
        ShouldTerminate(false)
    }

    fn handle_message_c(&mut self, c_foo: u64, c_bar: String) -> ShouldTerminate {
        ShouldTerminate(false)
    }
}

#[cfg(test)]
mod tests {
    use crate::TestWorker;
    use crate::{Actor, Address};

    #[test]
    fn create_worker() {
        let ctx = zmq::Context::new();
        TestWorker::new(
            ctx,
            &Address {
                conn_string: String::from("inproc://worker1"),
            },
        );
    }

    #[test]
    fn run_worker() {
        let ctx = zmq::Context::new();
        let mut worker = TestWorker::new(
            ctx,
            &Address {
                conn_string: String::from("inproc://worker1"),
            },
        );

        worker.run();
    }
}
