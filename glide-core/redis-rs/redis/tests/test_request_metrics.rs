#![cfg(feature = "cluster-async")]

mod support;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use redis::aio::{ConnectionLike, MultiplexedConnection};
use redis::cluster::ClusterClient;
use redis::{cmd, parse_redis_value, GlideConnectionOptions, IntoConnectionInfo, Value};
use telemetrylib::request_metrics::{
    BoundedOperation, RequestMetricPhase, RequestMetricResult, RequestMetricsState,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

use support::{respond_startup, MockEnv};

type Driver = Pin<Box<dyn Future<Output = ()> + Send>>;

fn completed_resp_commands(buffer: &[u8]) -> (usize, usize) {
    fn line_end(buffer: &[u8], start: usize) -> Option<usize> {
        buffer[start..]
            .windows(2)
            .position(|pair| pair == b"\r\n")
            .map(|offset| start + offset)
    }

    let mut commands = 0;
    let mut position = 0;
    while position < buffer.len() {
        if buffer[position] != b'*' {
            break;
        }
        let Some(end) = line_end(buffer, position + 1) else {
            break;
        };
        let Ok(argument_count) = std::str::from_utf8(&buffer[position + 1..end])
            .expect("RESP array length must be UTF-8")
            .parse::<usize>()
        else {
            break;
        };
        position = end + 2;

        let mut complete = true;
        for _ in 0..argument_count {
            if position >= buffer.len() || buffer[position] != b'$' {
                complete = false;
                break;
            }
            let Some(end) = line_end(buffer, position + 1) else {
                complete = false;
                break;
            };
            let length = std::str::from_utf8(&buffer[position + 1..end])
                .expect("RESP bulk length must be UTF-8")
                .parse::<usize>()
                .expect("RESP bulk length must be numeric");
            let next = end + 2 + length + 2;
            if next > buffer.len() {
                complete = false;
                break;
            }
            position = next;
        }
        if !complete {
            break;
        }
        commands += 1;
    }
    (commands, position)
}

async fn serve_ok_responses(mut stream: DuplexStream) {
    let mut pending = Vec::new();
    let mut input = [0_u8; 4096];
    loop {
        let read = match stream.read(&mut input).await {
            Ok(0) | Err(_) => return,
            Ok(read) => read,
        };
        pending.extend_from_slice(&input[..read]);
        let (commands, consumed) = completed_resp_commands(&pending);
        for _ in 0..commands {
            if stream.write_all(b"+OK\r\n").await.is_err() {
                return;
            }
        }
        pending.drain(..consumed);
    }
}

async fn test_multiplexed_connection(
) -> (MultiplexedConnection, Driver, tokio::task::JoinHandle<()>) {
    let (client, server) = tokio::io::duplex(64 * 1024);
    let server = tokio::spawn(serve_ok_responses(server));
    let connection_info = "redis://127.0.0.1:6379"
        .into_connection_info()
        .expect("valid connection info");
    let (connection, driver) =
        MultiplexedConnection::new(&connection_info, client, GlideConnectionOptions::default())
            .await
            .expect("in-memory connection setup must succeed");
    (connection, Box::pin(driver), server)
}

#[tokio::test(flavor = "current_thread")]
async fn direct_command_records_pipeline_socket_response_and_attempt() {
    let state = RequestMetricsState::new(100, 1, &[]).unwrap();
    let context = state
        .start(BoundedOperation::known("GET"))
        .expect("100% sampling must create a context");
    let (mut connection, driver, server) = test_multiplexed_connection().await;
    let driver = tokio::spawn(driver);
    let mut command = cmd("GET");
    command
        .arg("key")
        .set_request_metrics(Some(Arc::clone(&context)));

    assert_eq!(
        connection.send_packed_command(&command).await.unwrap(),
        Value::Okay
    );
    assert!(context.finish(RequestMetricResult::Success));

    let drain = state.drain(1).unwrap();
    let sample = &drain.samples()[0];
    assert!(sample.phase_duration(RequestMetricPhase::PipelineQueue) > 0);
    assert!(sample.phase_duration(RequestMetricPhase::SocketWrite) > 0);
    assert!(sample.phase_duration(RequestMetricPhase::ResponseWait) > 0);
    assert_eq!(sample.attempt_count(), 1);

    driver.abort();
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn waiting_for_a_pipeline_channel_permit_is_included_in_pipeline_queue() {
    let (connection, driver, server) = test_multiplexed_connection().await;
    let mut queued = Vec::new();
    for index in 0..50 {
        let mut connection = connection.clone();
        queued.push(tokio::spawn(async move {
            let mut command = cmd("GET");
            command.arg(format!("key-{index}"));
            connection.send_packed_command(&command).await
        }));
    }
    tokio::task::yield_now().await;

    let state = RequestMetricsState::new(100, 1, &[]).unwrap();
    let context = state
        .start(BoundedOperation::known("GET"))
        .expect("100% sampling must create a context");
    let mut sampled_connection = connection.clone();
    let sampled_context = Arc::clone(&context);
    let sampled = tokio::spawn(async move {
        let mut command = cmd("GET");
        command
            .arg("sampled-key")
            .set_request_metrics(Some(sampled_context));
        sampled_connection.send_packed_command(&command).await
    });
    tokio::task::yield_now().await;
    assert!(!sampled.is_finished());

    let driver = tokio::spawn(driver);
    for request in queued {
        assert_eq!(request.await.unwrap().unwrap(), Value::Okay);
    }
    assert_eq!(sampled.await.unwrap().unwrap(), Value::Okay);
    assert!(context.finish(RequestMetricResult::Success));

    let drain = state.drain(1).unwrap();
    assert!(
        drain.samples()[0].phase_duration(RequestMetricPhase::PipelineQueue) > 0,
        "the sampled request waited behind a full 50-message channel"
    );

    driver.abort();
    server.abort();
}

#[test]
#[serial_test::serial]
fn single_node_cluster_records_selection_and_only_the_explicit_retry_delay() {
    let name = "request_metrics_single_node_retry";
    let requests = Arc::new(AtomicUsize::new(0));
    let MockEnv {
        runtime,
        async_connection: mut connection,
        handler: _handler,
        ..
    } = MockEnv::with_client_builder(
        ClusterClient::builder(vec![&*format!("redis://{name}")])
            .retries(1)
            .min_retry_wait(1)
            .max_retry_wait(2)
            .retry_wait_formula(2, 1),
        name,
        {
            let requests = Arc::clone(&requests);
            move |packed, _| {
                respond_startup(name, packed)?;
                match requests.fetch_add(1, Ordering::SeqCst) {
                    0 => Err(parse_redis_value(b"-TRYAGAIN test retry\r\n")),
                    _ => Err(Ok(Value::BulkString(b"value".to_vec().into()))),
                }
            }
        },
    );
    let state = RequestMetricsState::new(100, 1, &[]).unwrap();
    let context = state
        .start(BoundedOperation::known("GET"))
        .expect("100% sampling must create a context");
    let mut command = cmd("GET");
    command
        .arg("key")
        .set_request_metrics(Some(Arc::clone(&context)));

    let response = runtime
        .block_on(connection.req_packed_command(&command))
        .unwrap();
    assert_eq!(response, Value::BulkString(b"value".to_vec().into()));
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    assert!(context.finish(RequestMetricResult::Success));

    let drain = state.drain(1).unwrap();
    let sample = &drain.samples()[0];
    assert!(sample.phase_duration(RequestMetricPhase::ConnectionWait) > 0);
    assert!(sample.phase_duration(RequestMetricPhase::RetryBackoff) > 0);
}
