#[cfg(feature = "inbound-tun")]
#[path = "../../examples/tun/host_ping.rs"]
mod host_ping_example;

#[cfg(feature = "inbound-tun")]
#[tokio::test(flavor = "current_thread")]
#[ignore = "manual real TUN host ping harness; set HAMMER_REAL_TUN_HOST_PING=1"]
async fn real_tun_host_ping_harness_runs_until_ping_is_observed() {
    if std::env::var("HAMMER_REAL_TUN_HOST_PING").as_deref() != Ok("1") {
        println!("skipping real TUN host-ping harness; set HAMMER_REAL_TUN_HOST_PING=1 to run");
        return;
    }

    let timeout = host_ping_example::env_timeout().unwrap_or(host_ping_example::TEST_TIMEOUT);
    let observed_output = host_ping_example::run_manual_harness(Some(timeout))
        .await
        .expect("run real TUN host-ping harness");
    assert!(
        observed_output,
        "host ping did not drive packets into tun-output before timeout"
    );
}
