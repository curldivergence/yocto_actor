#[derive(Clone)]
struct Address {
    conn_string: String,
}

trait Actor {
    type Message;

    fn new(zmq_ctx: zmq::Context, address: &Address) -> Self;
    fn run(&mut self);
    fn dispatch_message(&mut self, message: Self::Message) -> ShouldTerminate;
}

// ToDo: Result?
struct ShouldTerminate(bool);

#[cfg(test)]
mod tests {
    use crate::{Actor, Address, ShouldTerminate};
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    enum TestWorker1Message {
        MessageA,
        MessageB(u8, String),
        MessageC { c_foo: u64, c_bar: String },
    }

    struct TestWorker1 {
        socket: zmq::Socket,
    }

    impl Actor for TestWorker1 {
        type Message = TestWorker1Message;

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

    impl TestWorker1 {
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

    #[test]
    fn create_worker() {
        let ctx = zmq::Context::new();
        TestWorker1::new(
            ctx,
            &Address {
                conn_string: String::from("inproc://worker1"),
            },
        );
    }

    #[test]
    fn run_worker() {
        let ctx = zmq::Context::new();
        let address = Address {
            conn_string: String::from("inproc://worker1"),
        };

        let ctx_copy = ctx.clone();
        let address_copy = address.clone();
        let thread_handle = std::thread::spawn(move || {
            let mut worker = TestWorker1::new(ctx_copy, &address_copy);

            worker.run();
        });

        let control_socket = ctx.socket(zmq::PUSH).expect("Cannot create control socket");
        control_socket
            .connect(&address.conn_string)
            .expect("Cannot connect control socket");

        let message = TestWorker1Message::MessageA;
        let message_bytes = bincode::serialize(&message).expect("Cannot serialize message");
        control_socket
            .send(&message_bytes, 0)
            .expect("Cannot send message to worker");

        thread_handle.join().expect("Cannot join worker thread");
    }

    use custom_derive::Actor;

    #[derive(Serialize, Deserialize, Actor)]
    #[worker_type(TestWorker2)]
    enum TestWorker2Message {
        MessageA,
        // MessageB(u8, String),
        MessageC { c_foo: u64, c_bar: String },
    }

    struct TestWorker2 {
        // ToDo: is there any way to automate this or, at least, enforce?
        socket: zmq::Socket,
    }

    impl TestWorker2 {
        fn handle_message_a(&mut self) -> ShouldTerminate {
            ShouldTerminate(true)
        }

        fn handle_message_c(&mut self, c_foo: u64, c_bar: String) -> ShouldTerminate {
            ShouldTerminate(false)
        }
    }
}
