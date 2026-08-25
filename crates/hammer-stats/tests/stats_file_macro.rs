use hammer_stats::{StatsError, stats_segment_socket};

#[test]
fn stats_socket_file_functions_have_read_and_error_without_write() {
    let functions = stats_segment_socket::file_functions::<(), StatsError>();
    assert!(functions.read.is_some());
    assert!(functions.write.is_none());
    assert!(functions.error.is_some());
}
