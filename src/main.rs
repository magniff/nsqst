use std::sync::Arc;

use clap::Parser;
use log::info;
use tokio::io::AsyncWriteExt;
use tokio::signal::unix::{signal, SignalKind};
use tokio_nsq::{
    NSQChannel, NSQConfigShared, NSQConsumerConfig, NSQConsumerConfigSources, NSQEvent,
    NSQProducerConfig, NSQTopic,
};

#[derive(Parser, Debug, Clone, Copy)]
#[command(author, version, about, long_about = None)]
struct Arguments {
    #[clap(long, default_value = "info")]
    pub log_level: simplelog::LevelFilter,

    #[clap(long, default_value = "127.0.0.1:4150", help = "NSQd address")]
    pub nsqd_tcp_addresses: std::net::SocketAddr,

    #[arg(
        short,
        long,
        default_value = "16",
        help = "How many topics to write in"
    )]
    topics: usize,

    #[arg(
        short,
        long,
        default_value = "50",
        help = "How many milliseconds will the writer sleep until the next write"
    )]
    sleep: u64,

    #[arg(
        short,
        long,
        default_value = "25",
        help = "How does the writer flush its socket"
    )]
    flush_time: u64,

    #[arg(long, default_value = "20", help = "Max-in-flight messages")]
    max_in_flight: u32,

    #[arg(long, default_value = "300000", help = "Message size to generate")]
    message_size: usize,

    #[arg(long, default_value = "1000")]
    message_count: usize,

    #[arg(short, long, default_value = "4", help = "How many writers per topic")]
    writers: usize,

    #[arg(short, long, default_value = "4", help = "How many channels per topic")]
    channels: usize,

    #[arg(short, long, default_value = "1", help = "How many channels per topic")]
    readers: usize,

    #[arg(long, default_value = "2")]
    workers: usize,
}

pub struct ReaderArguments {
    pub index: usize,
    pub address: Arc<std::net::SocketAddr>,
    pub topic_name: Arc<String>,
    pub channel_name: Arc<String>,
    pub max_in_flight: u32,
    pub telemetry_sender: tokio::sync::mpsc::UnboundedSender<TelemetryMessage>,
}

pub struct WriterArguments {
    pub address: Arc<std::net::SocketAddr>,
    pub topic_name: Arc<String>,
    pub message_size: usize,
    pub sleep_time: u64,
    pub flush_time: u64,
    pub message_count: usize,
}

pub async fn reader(arguments: ReaderArguments) -> anyhow::Result<()> {
    let Some(topic)  = NSQTopic::new(arguments.topic_name.to_string()) else {
        anyhow::bail!("Topic name '{name}' is not ok", name=arguments.topic_name.to_string());
    };
    let Some(channel)  = NSQChannel::new(arguments.channel_name.to_string()) else {
        anyhow::bail!("Channel name '{name}' is not ok", name=arguments.channel_name.to_string());
    };
    let addresses = vec![arguments.address.to_string()];
    let mut reader = {
        NSQConsumerConfig::new(topic, channel)
            .set_max_in_flight(arguments.max_in_flight)
            .set_sources(NSQConsumerConfigSources::Daemons(addresses))
            .build()
    };
    while let Some(message) = reader.consume_filtered().await {
        let current_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64;

        let sender_timestamp = u64::from_be_bytes(message.body.as_slice()[0..8].try_into()?);
        let diff = current_timestamp - sender_timestamp;
        message.finish().await;
        arguments.telemetry_sender.send(TelemetryMessage {
            io_delay: diff,
            index: arguments.index,
            topic_name: arguments.topic_name.as_ref().clone(),
            channel_name: arguments.channel_name.as_ref().clone(),
        })?;
    }
    Ok(())
}

pub async fn writer(arguments: WriterArguments) -> anyhow::Result<()> {
    let mut writer_backend = NSQProducerConfig::new(arguments.address.to_string())
        .set_shared(
            NSQConfigShared::new()
                .set_flush_interval(std::time::Duration::from_millis(arguments.flush_time)),
        )
        .build();
    let Some(topic) = NSQTopic::new(arguments.topic_name.to_string()) else {
        anyhow::bail!("Topic name is not ok");
    };
    let NSQEvent::Healthy() = writer_backend
        .consume()
        .await
        .ok_or_else(|| anyhow::anyhow!("Cannot establish any connection to NSQ"))? else {
        anyhow::bail!("Cannot establish a healthy connection to NSQ")
    };
    let mut counter = 0usize;
    while counter < arguments.message_count {
        // Message is a bunch of random garbage with a timestamp stiched to the front
        let message_len = arguments.message_size + 8;
        let mut message = Vec::with_capacity(message_len);
        message
            .write_u64(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_millis() as u64,
            )
            .await?;
        unsafe { message.set_len(message_len) };

        writer_backend.publish(&topic, message).await?;
        let Some(NSQEvent::Ok()) = writer_backend.consume().await else {
            anyhow::bail!("No");
        };

        counter += 1;
        tokio::time::sleep(std::time::Duration::from_millis(arguments.sleep_time)).await;
    }
    Ok(())
}

async fn telemetry_processor(
    mut telemetry_receiver: tokio::sync::mpsc::UnboundedReceiver<TelemetryMessage>,
    messages_count: usize,
) -> anyhow::Result<()> {
    let mut messages_current = 0usize;
    while let Some(message) = telemetry_receiver.recv().await {
        // println!(
        //     "{messages_current}/{messages_count} :: {delay}",
        //     delay = message.io_delay
        // );
        messages_current += 1;
        if messages_current == messages_count {
            break;
        }
    }
    Ok(())
}

#[derive(Debug)]
pub struct TelemetryMessage {
    pub io_delay: u64,
    pub topic_name: String,
    pub channel_name: String,
    pub index: usize,
}

async fn async_main(arguments: Arguments) -> anyhow::Result<()> {
    let mut interrupt_request = signal(SignalKind::interrupt())?;
    let (telemetry_sender, telemetry_receiver) =
        tokio::sync::mpsc::unbounded_channel::<TelemetryMessage>();

    // It is crucial to run the readers first, beacause readers (not writers) create channels
    #[allow(clippy::map_flatten)]
    (0..arguments.topics)
        .map(|topic_index| {
            let telemetry_sender = telemetry_sender.clone();
            (0..arguments.channels)
                .map(move |channel_index| {
                    let telemetry_sender = telemetry_sender.clone();
                    (0..arguments.readers).map(move |reader_index| {
                        tokio::spawn(reader(ReaderArguments {
                            index: reader_index,
                            address: Arc::new(arguments.nsqd_tcp_addresses),
                            topic_name: Arc::new(format!("topic{topic_index}#ephemeral")),
                            channel_name: Arc::new(format!("channel{channel_index}#ephemeral")),
                            max_in_flight: arguments.max_in_flight,
                            telemetry_sender: telemetry_sender.clone(),
                        }))
                    })
                })
                .flatten()
        })
        .flatten()
        .for_each(drop);

    // Giving NSQ a change to properly create channels
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    #[allow(clippy::map_flatten)]
    (0..arguments.topics)
        .map(|topic_index| {
            (0..arguments.writers).map(move |_| {
                tokio::spawn(writer(WriterArguments {
                    address: Arc::new(arguments.nsqd_tcp_addresses),
                    topic_name: Arc::new(format!("topic{topic_index}#ephemeral")),
                    message_size: arguments.message_size,
                    sleep_time: arguments.sleep,
                    message_count: arguments.message_count,
                    flush_time: arguments.flush_time,
                }))
            })
        })
        .flatten()
        .for_each(drop);

    let telemetry_task = tokio::spawn(telemetry_processor(
        telemetry_receiver,
        arguments.topics * arguments.writers * arguments.channels * arguments.message_count,
    ));

    tokio::select! {
        _ = telemetry_task => {
            info!("Telemetry task is done");
        }
        _ = interrupt_request.recv() => {
            info!("Caught a CTRL+C signal, halting..");
        }
    };
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let arguments = Arguments::parse();
    simplelog::TermLogger::init(
        arguments.log_level,
        simplelog::Config::default(),
        simplelog::TerminalMode::Mixed,
        simplelog::ColorChoice::Auto,
    )?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(arguments.workers)
        .enable_all()
        .build()?;

    runtime.block_on(async_main(arguments))?;
    Ok(())
}
