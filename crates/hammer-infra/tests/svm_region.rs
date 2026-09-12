//! Behavior tests for the SVM region owner (ADR-0011 section 12).
//!
//! A region is a payload layout: a fixed header, one or two offset heaps, the
//! member table, and the root subregion registry. Every shared location is a
//! payload-relative offset, so the cross-process cases attach the same memfd
//! from a second exec and check that both processes see the same objects.

#![cfg(target_os = "linux")]

use std::alloc::Layout;
use std::mem::size_of;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use hammer_infra::svm::region::{
    REGION_FLAG_DATA_HEAP, REGION_FLAG_SUBDIVIDED, RegionLockTag, SvmRegion, SvmRegionConfig,
    SvmRegionError, SvmRegionHeader, SvmRegionState,
};
use hammer_infra::svm::region_heap::SvmRegionHeap;
use hammer_infra::svm::ssvm::{SSVM_PAYLOAD_OFFSET, SsvmConfig, SsvmPrivate, SsvmSegmentBackend};

const CHILD_CASE: &str = "HAMMER_SVM_REGION_CHILD_CASE";
const CHILD_FD: &str = "HAMMER_SVM_REGION_CHILD_FD";
const CHILD_TEST: &str = "region_is_reachable_from_another_process";
const CHILD_LOCK_TEST: &str = "region_lock_owner_death_fails_the_region";
const REGION_BYTES: u64 = 1 << 20;

fn subdivided_config() -> SvmRegionConfig {
    SvmRegionConfig {
        size: REGION_BYTES,
        flags: REGION_FLAG_SUBDIVIDED,
    }
}

fn data_heap_config() -> SvmRegionConfig {
    SvmRegionConfig {
        size: REGION_BYTES,
        flags: REGION_FLAG_DATA_HEAP,
    }
}

fn layout(size: usize, align: usize) -> Layout {
    Layout::from_size_align(size, align).expect("test layout")
}

fn header_end() -> u64 {
    (size_of::<SvmRegionHeader>() as u64).div_ceil(64) * 64
}

/// Creates a segment plus region for `config`, using the layout the region asks
/// for so the mapping size and the region layout cannot drift apart.
fn create_region(
    config: &SvmRegionConfig,
) -> Result<(Arc<SsvmPrivate>, SvmRegion), SvmRegionError> {
    let mapping = SvmRegion::layout(config)?;
    let segment = Arc::new(SsvmPrivate::server_init_memfd(&SsvmConfig {
        backend: SsvmSegmentBackend::Memfd,
        name: "hammer-svm-region-test".to_string(),
        size: mapping.size(),
        requested_va: 0,
        huge_page: false,
        attach_timeout: Duration::from_secs(5),
    })?);
    let region = SvmRegion::create(Arc::clone(&segment), config)?;
    Ok((segment, region))
}

fn header(region: &SvmRegion, segment: &SsvmPrivate) -> *mut SvmRegionHeader {
    segment
        .offset_ptr(region.header_offset(), size_of::<SvmRegionHeader>(), 64)
        .expect("region header inside mapping")
        .cast()
}

fn read_payload(segment: &SsvmPrivate, region: &SvmRegion, offset: u64, length: usize) -> Vec<u8> {
    let pointer = segment
        .offset_ptr(region.header_offset() + offset, length, 1)
        .expect("payload range inside mapping");
    // SAFETY: the range was validated against the mapping, and the segment owns
    // that mapping for the duration of the borrow.
    unsafe { std::slice::from_raw_parts(pointer, length) }.to_vec()
}

fn write_payload(segment: &SsvmPrivate, region: &SvmRegion, offset: u64, bytes: &[u8]) {
    let pointer = segment
        .offset_ptr(region.header_offset() + offset, bytes.len(), 1)
        .expect("payload range inside mapping");
    // SAFETY: as `read_payload`; the test is the only writer of this block.
    unsafe { std::slice::from_raw_parts_mut(pointer, bytes.len()) }.copy_from_slice(bytes);
}

#[test]
fn region_layout_reserves_header_metadata_and_data() {
    let subdivided = SvmRegion::layout(&subdivided_config()).expect("subdivided layout");
    let regular = SvmRegion::layout(&data_heap_config()).expect("regular layout");
    assert!(
        subdivided.size() as u64 >= REGION_BYTES + SSVM_PAYLOAD_OFFSET,
        "subdivided mapping {} cannot hold the header and a heap",
        subdivided.size()
    );
    assert!(
        regular.size() >= subdivided.size(),
        "the requested payload dominates the mapping size"
    );
    let smallest_subdivided = SvmRegion::layout(&SvmRegionConfig {
        size: 1,
        flags: REGION_FLAG_SUBDIVIDED,
    })
    .expect("minimum subdivided layout");
    let smallest_regular = SvmRegion::layout(&SvmRegionConfig {
        size: 1,
        flags: REGION_FLAG_DATA_HEAP,
    })
    .expect("minimum regular layout");
    assert_eq!(
        smallest_subdivided.size() as u64,
        SSVM_PAYLOAD_OFFSET + header_end() + 64
    );
    assert!(
        smallest_regular.size() > smallest_subdivided.size(),
        "a regular region reserves a metadata heap in front of its data section"
    );
    assert_eq!(subdivided.align(), 64);
    let unknown = SvmRegionConfig {
        size: REGION_BYTES,
        flags: 1 << 5,
    };
    match SvmRegion::layout(&unknown) {
        Err(SvmRegionError::LayoutMismatch { declared, .. }) => assert_eq!(declared, 1 << 5),
        other => panic!("unknown flags must be rejected, got {other:?}"),
    }
    let contradictory = SvmRegionConfig {
        size: REGION_BYTES,
        flags: REGION_FLAG_DATA_HEAP | REGION_FLAG_SUBDIVIDED,
    };
    match SvmRegion::layout(&contradictory) {
        Err(SvmRegionError::UnsupportedOperation { .. }) => {}
        other => panic!("subdivided data heap must be rejected, got {other:?}"),
    }
}

#[test]
fn region_publishes_and_reports_its_shared_header() -> Result<(), SvmRegionError> {
    let (segment, region) = create_region(&subdivided_config())?;
    assert_eq!(region.flags()?, REGION_FLAG_SUBDIVIDED);
    assert_eq!(region.state()?, SvmRegionState::Ready);
    assert_eq!(region.virtual_size()?, segment.payload_len());
    assert_eq!(region.header_offset(), SSVM_PAYLOAD_OFFSET);
    assert!(Arc::ptr_eq(region.ssvm(), &segment));

    let attached = SvmRegion::attach(Arc::clone(&segment))?;
    assert_eq!(attached.flags()?, REGION_FLAG_SUBDIVIDED);
    assert_eq!(attached.state()?, SvmRegionState::Ready);
    assert_eq!(attached.virtual_size()?, region.virtual_size()?);
    assert_eq!(attached.find_or_create_subregion("echo")?, (1, true));
    assert_eq!(
        region.subregion_id("echo")?,
        Some(1),
        "both handles see one registry"
    );
    Ok(())
}

#[test]
fn region_registry_assigns_monotonic_ids() -> Result<(), SvmRegionError> {
    let (segment, region) = create_region(&subdivided_config())?;
    let attached = SvmRegion::attach(segment)?;
    assert_eq!(region.subregion_count()?, 0);
    assert!(region.subregion_names()?.next().is_none());
    assert_eq!(region.find_or_create_subregion("echo")?, (1, true));
    assert_eq!(region.find_or_create_subregion("echo")?, (1, false));
    assert_eq!(region.find_or_create_subregion("sip")?, (2, true));
    assert_eq!(region.subregion_count()?, 2);
    assert_eq!(region.subregion_id("sip")?, Some(2));
    let mut names: Vec<&str> = region.subregion_names()?.collect();
    names.sort_unstable();
    assert_eq!(names, ["echo", "sip"]);
    assert_eq!(attached.remove_subregion("echo")?, Some(1));
    assert_eq!(region.subregion_id("echo")?, None);
    assert_eq!(region.subregion_count()?, 1);
    assert_eq!(
        region.find_or_create_subregion("echo")?,
        (3, true),
        "a removed name gets a fresh id, never the old one"
    );
    Ok(())
}

#[test]
fn region_registry_reuse_does_not_grow_the_table() -> Result<(), SvmRegionError> {
    let (_, region) = create_region(&subdivided_config())?;
    assert_eq!(region.find_or_create_subregion("loop")?, (1, true));
    assert_eq!(region.remove_subregion("loop")?, Some(1));
    let baseline = region.used_bytes()?;
    for round in 2..=201 {
        assert_eq!(region.find_or_create_subregion("loop")?, (round, true));
        assert_eq!(region.remove_subregion("loop")?, Some(round));
    }
    assert_eq!(
        region.used_bytes()?,
        baseline,
        "repeated create/remove must not leak registry storage"
    );
    assert_eq!(region.subregion_count()?, 0);
    Ok(())
}

#[test]
fn region_registry_rejects_invalid_names() -> Result<(), SvmRegionError> {
    let (_, region) = create_region(&subdivided_config())?;
    match region.find_or_create_subregion("") {
        Err(SvmRegionError::InvalidRegionName { length: 0 }) => {}
        other => panic!("empty name must be rejected, got {other:?}"),
    }
    let longest = "n".repeat(256);
    assert_eq!(region.find_or_create_subregion(&longest)?, (1, true));
    let too_long = "n".repeat(257);
    match region.find_or_create_subregion(&too_long) {
        Err(SvmRegionError::InvalidRegionName { length: 257 }) => {}
        other => panic!("overlong name must be rejected, got {other:?}"),
    }
    assert_eq!(region.subregion_count()?, 1);
    Ok(())
}

#[test]
fn region_data_heap_allocates_reallocates_and_frees() -> Result<(), SvmRegionError> {
    let (segment, region) = create_region(&data_heap_config())?;
    let data_start = unsafe { (*header(&region, &segment)).data_base_offset };
    assert_eq!(region.used_bytes()?, 0);
    assert_eq!(
        region.free_bytes()? + region.used_bytes()?,
        region.virtual_size()? - header_end(),
        "the heaps account for the whole payload behind the header"
    );
    let request = layout(64, 8);
    let offset = region.allocate(request)?;
    assert!(
        offset >= data_start,
        "a data heap region allocates in its data section"
    );
    write_payload(&segment, &region, offset, &[0xAB; 64]);
    assert!(region.used_bytes()? >= 64);
    let grown = region.reallocate(offset, request, 128)?;
    assert_eq!(read_payload(&segment, &region, grown, 64), vec![0xAB; 64]);
    region.deallocate(grown, layout(128, 8))?;
    assert_eq!(region.used_bytes()?, 0);
    assert_eq!(
        region.allocate(request)?,
        offset,
        "the released block is reusable"
    );
    Ok(())
}

#[test]
fn region_root_and_user_context_round_trip() -> Result<(), SvmRegionError> {
    let (segment, region) = create_region(&data_heap_config())?;
    assert_eq!(region.root()?, None);
    assert_eq!(region.user_ctx_offset()?, 0);
    let offset = region.allocate(layout(32, 8))?;
    region.publish_root(offset)?;
    assert_eq!(region.root()?, Some(offset));
    let beyond = region.virtual_size()?;
    match region.publish_root(beyond) {
        Err(SvmRegionError::InvalidRoot { offset: reported }) => assert_eq!(reported, beyond),
        other => panic!("out-of-range root must be rejected, got {other:?}"),
    }
    match region.publish_root(header_end() - 1) {
        Err(SvmRegionError::InvalidRoot { offset }) => assert_eq!(offset, header_end() - 1),
        other => panic!("a root inside the header must be rejected, got {other:?}"),
    }
    let attached = SvmRegion::attach(Arc::clone(&segment))?;
    assert_eq!(attached.root()?, Some(offset));
    attached.publish_user_ctx(offset)?;
    assert_eq!(region.user_ctx_offset()?, offset);
    attached.publish_root(0)?;
    assert_eq!(region.root()?, None);
    Ok(())
}

#[test]
fn region_regular_layout_has_no_registry() -> Result<(), SvmRegionError> {
    let (_, region) = create_region(&data_heap_config())?;
    match region.main() {
        Err(SvmRegionError::UnsupportedOperation { .. }) => {}
        other => panic!("a regular region has no registry, got {other:?}"),
    }
    match region.find_or_create_subregion("echo") {
        Err(SvmRegionError::UnsupportedOperation { .. }) => {}
        other => panic!("a regular region has no registry, got {other:?}"),
    }
    Ok(())
}

#[test]
fn region_create_requires_a_created_segment() -> Result<(), SvmRegionError> {
    let (segment, region) = create_region(&subdivided_config())?;
    drop(region);
    let fd = segment.fd().expect("shared descriptor");
    let attached = SsvmPrivate::client_init_memfd(fd)?;
    match SvmRegion::create(Arc::new(attached), &subdivided_config()) {
        Err(SvmRegionError::UnsupportedOperation { .. }) => Ok(()),
        other => panic!("attached segments cannot create a region, got {other:?}"),
    }
}

#[test]
fn region_attach_validates_the_shared_header() -> Result<(), SvmRegionError> {
    {
        let (segment, region) = create_region(&subdivided_config())?;
        unsafe {
            (*header(&region, &segment))
                .version
                .store(0, Ordering::Release)
        };
        match SvmRegion::attach(Arc::clone(&segment)) {
            Err(SvmRegionError::NotReady {
                state: SvmRegionState::Uninitialized,
            }) => {}
            other => panic!("an unpublished region must be NotReady, got {other:?}"),
        }
    }
    {
        let (segment, region) = create_region(&subdivided_config())?;
        unsafe {
            (*header(&region, &segment))
                .version
                .store(9, Ordering::Release)
        };
        match SvmRegion::attach(Arc::clone(&segment)) {
            Err(SvmRegionError::UnsupportedVersion { found, expected }) => {
                assert_eq!((found, expected), (9, (1 << 16) | 1));
            }
            other => panic!("an unknown version must be rejected, got {other:?}"),
        }
    }
    {
        let (segment, region) = create_region(&subdivided_config())?;
        unsafe { (*header(&region, &segment)).magic = 0 };
        match SvmRegion::attach(Arc::clone(&segment)) {
            Err(SvmRegionError::InvalidMagic { found: 0 }) => {}
            other => panic!("a bad magic must be rejected, got {other:?}"),
        }
    }
    {
        let (segment, region) = create_region(&subdivided_config())?;
        unsafe { (*header(&region, &segment)).virtual_size += 1 };
        match SvmRegion::attach(Arc::clone(&segment)) {
            Err(SvmRegionError::LayoutMismatch { declared, expected }) => {
                assert_eq!(declared, segment.payload_len() + 1);
                assert_eq!(expected, segment.payload_len());
            }
            other => panic!("a size mismatch must be rejected, got {other:?}"),
        }
    }
    {
        let (segment, region) = create_region(&subdivided_config())?;
        unsafe {
            (*header(&region, &segment))
                .flags
                .store(1 << 5, Ordering::Release)
        };
        match SvmRegion::attach(Arc::clone(&segment)) {
            Err(SvmRegionError::LayoutMismatch { declared, expected }) => {
                assert_eq!((declared, expected), (1 << 5, 0b101));
            }
            other => panic!("unknown flags must be rejected, got {other:?}"),
        }
    }
    {
        let (segment, region) = create_region(&subdivided_config())?;
        unsafe { (*header(&region, &segment)).data_base_offset = u64::MAX };
        match SvmRegion::attach(Arc::clone(&segment)) {
            Err(SvmRegionError::InvalidBounds { offset, .. }) => assert_eq!(offset, u64::MAX),
            other => panic!("an out-of-range data section must be rejected, got {other:?}"),
        }
    }
    {
        let (segment, region) = create_region(&subdivided_config())?;
        unsafe { (*header(&region, &segment)).metadata_heap = SvmRegionHeap::new() };
        match SvmRegion::attach(Arc::clone(&segment)) {
            Err(SvmRegionError::LayoutMismatch { declared: 0, .. }) => {}
            other => panic!("an inconsistent heap range must be rejected, got {other:?}"),
        }
    }
    Ok(())
}

#[test]
fn region_membership_tracks_joins_and_leaves() -> Result<(), SvmRegionError> {
    let (_, region) = create_region(&subdivided_config())?;
    let pid = std::process::id() as i32;
    assert_eq!(region.member_count()?, 0);
    assert!(region.client_pids()?.is_empty());
    {
        let membership = region.join()?;
        assert_eq!(region.member_count()?, 1);
        assert_eq!(region.client_pids()?.to_vec(), vec![pid]);
        drop(membership);
    }
    assert_eq!(region.member_count()?, 0);
    let first = region.join()?;
    let second = region.join()?;
    assert_eq!(
        region.member_count()?,
        2,
        "each join records one membership"
    );
    drop(first);
    drop(second);
    assert_eq!(region.member_count()?, 0);
    assert_eq!(
        region.remove_exited_members()?,
        0,
        "this process is alive, so nothing is reclaimed"
    );
    Ok(())
}

#[test]
fn region_reclaims_only_exited_members() -> Result<(), SvmRegionError> {
    let (segment, region) = create_region(&subdivided_config())?;
    let membership = region.join()?;
    let pointer = header(&region, &segment);
    // A pid above the Linux maximum can never exist, so the probe reports it as
    // exited exactly like a process that has already been reaped.
    let dead: i32 = 0x7fff_ffff;
    let members = unsafe { (*pointer).client_pids_offset.load(Ordering::Acquire) };
    write_payload(
        &segment,
        &region,
        members + size_of::<i32>() as u64,
        &dead.to_ne_bytes(),
    );
    unsafe { (*pointer).client_count.store(2, Ordering::Release) };
    assert_eq!(region.remove_exited_members()?, 1);
    assert_eq!(region.member_count()?, 1);
    assert_eq!(
        region.client_pids()?.to_vec(),
        vec![std::process::id() as i32]
    );
    drop(membership);
    Ok(())
}

#[test]
fn region_is_reachable_from_another_process() -> Result<(), SvmRegionError> {
    if let Ok(case) = std::env::var(CHILD_CASE) {
        run_child(&case);
        return Ok(());
    }
    let (segment, region) = create_region(&subdivided_config())?;
    assert_eq!(region.find_or_create_subregion("echo")?, (1, true));
    let output = spawn_child(CHILD_TEST, "shared", &segment);
    assert!(
        output.status.success(),
        "child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reported = String::from_utf8_lossy(&output.stderr);
    assert!(
        reported.contains("child: subregion=1"),
        "child did not see the parent's registry: {reported}"
    );
    assert!(
        reported.contains("child: joined"),
        "child did not join the region: {reported}"
    );
    let child_base = reported
        .lines()
        .find_map(|line| line.strip_prefix("child: base="))
        .expect("child reported its mapping base");
    assert_ne!(
        child_base,
        format!("{:p}", segment.base()),
        "the child maps the same payload at its own address"
    );
    let marker = reported
        .lines()
        .find_map(|line| line.strip_prefix("child: marker="))
        .expect("child reported its allocation")
        .parse::<u64>()
        .expect("marker offset");
    assert_eq!(
        read_payload(&segment, &region, marker, 4),
        vec![0xC5; 4],
        "the parent reads the bytes the child wrote at the same offset"
    );
    assert_eq!(region.subregion_id("echo")?, Some(1));
    assert_eq!(
        region.member_count()?,
        0,
        "the child removed its membership before exiting"
    );
    assert_eq!(region.remove_exited_members()?, 0);
    Ok(())
}

#[test]
fn region_lock_owner_death_fails_the_region() -> Result<(), SvmRegionError> {
    if let Ok(case) = std::env::var(CHILD_CASE) {
        run_child(&case);
        return Ok(());
    }
    let (segment, region) = create_region(&subdivided_config())?;
    let child = spawn_child_process(CHILD_LOCK_TEST, "owner_death", &segment);
    let child_pid = child.id();
    let output = child.wait_with_output().expect("child output");
    assert!(
        output.status.success(),
        "child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("child: locked"),
        "child never held the region mutex"
    );
    match region.subregion_count() {
        Err(SvmRegionError::OwnerDied { pid }) => assert_eq!(pid, child_pid as i32),
        other => panic!("a dead mutex owner must be reported, got {other:?}"),
    }
    assert_eq!(region.state()?, SvmRegionState::Failed);
    match region.allocate(layout(8, 8)) {
        Err(SvmRegionError::RegionFailed { mutex_owner_pid }) => {
            assert_eq!(mutex_owner_pid, child_pid as i32);
        }
        other => panic!("a failed region must stay failed, got {other:?}"),
    }
    match SvmRegion::attach(Arc::clone(&segment)) {
        Err(SvmRegionError::RegionFailed { .. }) => Ok(()),
        other => panic!("a failed region must not attach, got {other:?}"),
    }
}

fn spawn_child(test: &str, case: &str, segment: &Arc<SsvmPrivate>) -> std::process::Output {
    spawn_child_process(test, case, segment)
        .wait_with_output()
        .expect("child output")
}

fn spawn_child_process(test: &str, case: &str, segment: &Arc<SsvmPrivate>) -> std::process::Child {
    let descriptor = segment.fd().expect("shared descriptor");
    // The descriptor crosses exec only once CLOEXEC is cleared.
    let cleared = unsafe { libc::fcntl(descriptor, libc::F_SETFD, 0) };
    assert_eq!(cleared, 0, "clearing FD_CLOEXEC failed");
    Command::new(std::env::current_exe().expect("test binary"))
        .args(["--exact", test, "--nocapture"])
        .env(CHILD_CASE, case)
        .env(CHILD_FD, descriptor.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("child process")
}

/// Child side of the cross-process cases.
fn run_child(case: &str) {
    let descriptor: i32 = std::env::var(CHILD_FD)
        .expect("child descriptor")
        .parse()
        .expect("descriptor number");
    let segment = SsvmPrivate::client_init_memfd(descriptor).expect("child segment attach");
    let region = SvmRegion::attach(Arc::new(segment)).expect("child region attach");
    match case {
        "shared" => {
            let subregion = region
                .subregion_id("echo")
                .expect("child registry read")
                .expect("child sees the parent's subregion");
            eprintln!("child: subregion={subregion}");
            eprintln!("child: base={:p}", region.ssvm().base());
            let membership = region.join().expect("child join");
            eprintln!("child: joined");
            let offset = region
                .allocate(layout(4, 4))
                .expect("child allocation from the shared heap");
            eprintln!("child: marker={offset}");
            let pointer = region
                .ssvm()
                .offset_ptr(region.header_offset() + offset, 4, 1)
                .expect("child marker inside mapping");
            // SAFETY: the child owns the block it just allocated and no other
            // process writes it before the child exits.
            unsafe { std::slice::from_raw_parts_mut(pointer, 4) }.copy_from_slice(&[0xC5; 4]);
            drop(membership);
            std::process::exit(0);
        }
        "owner_death" => {
            let lock = region.lock(RegionLockTag::Scan).expect("child region lock");
            eprintln!("child: locked");
            assert_eq!(region.state().expect("child state"), SvmRegionState::Ready);
            // Leaving without dropping the guard keeps the robust mutex owned by
            // a process that no longer exists.
            std::mem::forget(lock);
            unsafe { libc::_exit(0) };
        }
        other => panic!("unknown child case {other}"),
    }
}
