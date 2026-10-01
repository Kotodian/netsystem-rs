use std::cell::UnsafeCell;
use std::io;
use std::mem::size_of;
use std::os::fd::{AsRawFd, OwnedFd};
use std::ptr;
use std::sync::atomic::{AtomicU16, Ordering};

use hammer_infra::physmem::PhysmemMain;

const VHOST_VIRTIO: u32 = 0xaf;
const IOC_WRITE: u32 = 1;
const IOC_READ: u32 = 2;

const fn ioctl_code(direction: u32, number: u32, size: usize) -> libc::c_ulong {
    ((direction << 30) | ((size as u32) << 16) | (VHOST_VIRTIO << 8) | number) as libc::c_ulong
}

const SET_OWNER: libc::c_ulong = ioctl_code(0, 1, 0);
const GET_FEATURES: libc::c_ulong = ioctl_code(IOC_READ, 0, size_of::<u64>());
const SET_FEATURES: libc::c_ulong = ioctl_code(IOC_WRITE, 0, size_of::<u64>());
const SET_MEM_TABLE: libc::c_ulong = ioctl_code(IOC_WRITE, 3, 8);
const SET_VRING_NUM: libc::c_ulong = ioctl_code(IOC_WRITE, 0x10, size_of::<VringState>());
const SET_VRING_ADDR: libc::c_ulong = ioctl_code(IOC_WRITE, 0x11, size_of::<VringAddress>());
const SET_VRING_KICK: libc::c_ulong = ioctl_code(IOC_WRITE, 0x20, size_of::<VringFile>());
const SET_VRING_CALL: libc::c_ulong = ioctl_code(IOC_WRITE, 0x21, size_of::<VringFile>());
const SET_BACKEND: libc::c_ulong = ioctl_code(IOC_WRITE, 0x30, size_of::<VringFile>());

pub(crate) const FEATURES: u64 = (1 << 15) | (1 << 28) | (1 << 32);
pub(crate) const NET_HEADER_LEN: usize = 12;
pub(crate) const TUN_DATA_OFFSET: usize = 14;
pub(crate) const NEEDS_CHECKSUM: u8 = 1;
pub(crate) const GSO_TCP4: u8 = 1;
pub(crate) const GSO_TCP6: u8 = 4;
pub(crate) const DESC_NEXT: u16 = 1;
pub(crate) const DESC_WRITE: u16 = 2;
pub(crate) const DESC_INDIRECT: u16 = 4;

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub(crate) struct NetHeader {
    pub flags: u8,
    pub gso_type: u8,
    pub header_len: [u8; 2],
    pub gso_size: [u8; 2],
    pub checksum_start: [u8; 2],
    pub checksum_offset: [u8; 2],
    pub num_buffers: [u8; 2],
}

impl NetHeader {
    #[inline(always)]
    pub(crate) fn write_to(self, bytes: &mut [u8]) {
        assert_eq!(
            bytes.len(),
            NET_HEADER_LEN,
            "virtio-net header has fixed size"
        );
        // SAFETY: the asserted C layout is twelve initialized bytes; the
        // Buffer headroom may be unaligned, so this is an unaligned write.
        unsafe { ptr::write_unaligned(bytes.as_mut_ptr().cast::<Self>(), self) };
    }
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub(crate) struct Descriptor {
    pub addr: u64,
    pub len: u32,
    pub flags: u16,
    pub next: u16,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub(crate) struct UsedElement {
    pub id: u32,
    pub len: u32,
}

#[repr(C)]
struct VringState {
    index: u32,
    num: u32,
}

#[repr(C)]
struct VringAddress {
    index: u32,
    flags: u32,
    desc_user_addr: u64,
    used_user_addr: u64,
    avail_user_addr: u64,
    log_guest_addr: u64,
}

#[repr(C)]
struct VringFile {
    index: u32,
    fd: libc::c_int,
}

const _: () = assert!(size_of::<Descriptor>() == 16);
const _: () = assert!(size_of::<NetHeader>() == NET_HEADER_LEN);
const _: () = assert!(size_of::<UsedElement>() == 8);
const _: () = assert!(size_of::<VringState>() == 8);
const _: () = assert!(size_of::<VringAddress>() == 40);
const _: () = assert!(size_of::<VringFile>() == 8);

fn ioctl<T>(fd: &OwnedFd, request: libc::c_ulong, argument: &T) -> io::Result<()> {
    // SAFETY: request and argument have the Linux UAPI layouts asserted above.
    let result = unsafe { libc::ioctl(fd.as_raw_fd(), request, argument) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub(crate) fn set_owner(fd: &OwnedFd) -> io::Result<()> {
    // SAFETY: VHOST_SET_OWNER takes no argument.
    let result = unsafe { libc::ioctl(fd.as_raw_fd(), SET_OWNER) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub(crate) fn get_features(fd: &OwnedFd) -> io::Result<u64> {
    let mut features = 0u64;
    ioctl(fd, GET_FEATURES, &mut features)?;
    Ok(features)
}

pub(crate) fn set_features(fd: &OwnedFd) -> io::Result<()> {
    ioctl(fd, SET_FEATURES, &FEATURES)
}

pub(crate) struct MemoryTable {
    // vhost_memory is { u32 nregions, u32 padding, region[0] };
    // each region consists of four u64 values. The boxed allocation remains
    // live until every vhost fd has been closed.
    fields: Box<[u64]>,
}

impl MemoryTable {
    pub(crate) fn new() -> Self {
        let maps = PhysmemMain::global();
        let mut fields = Vec::with_capacity(1 + maps.maps().count() * 4);
        fields.push(maps.maps().count() as u64);
        for map in maps.maps() {
            let address = map.base() as u64;
            fields.extend_from_slice(&[address, map.size() as u64, address, 0]);
        }
        Self {
            fields: fields.into_boxed_slice(),
        }
    }

    pub(crate) fn install(&self, fd: &OwnedFd) -> io::Result<()> {
        // SAFETY: fields starts with the UAPI header and holds exactly nregions
        // contiguous vhost_memory_region records at a stable aligned address.
        let result = unsafe { libc::ioctl(fd.as_raw_fd(), SET_MEM_TABLE, self.fields.as_ptr()) };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

struct AlignedVring {
    storage: Box<[UnsafeCell<u8>]>,
    offset: usize,
}

impl AlignedVring {
    fn new(length: usize) -> Self {
        let storage = (0..length + 63)
            .map(|_| UnsafeCell::new(0u8))
            .collect::<Box<[_]>>();
        let offset = (64 - (storage.as_ptr() as usize & 63)) & 63;
        Self { storage, offset }
    }

    #[inline(always)]
    fn ptr(&self) -> *mut u8 {
        // SAFETY: offset is bounded to 0..64 and storage holds 63 spare bytes.
        unsafe { (*self.storage.as_ptr().add(self.offset)).get() }
    }
}

pub(crate) struct Virtqueue {
    descriptors: AlignedVring,
    available: AlignedVring,
    used: AlignedVring,
    pub(crate) size: u16,
}

impl Virtqueue {
    pub(crate) fn new(size: u16) -> Self {
        let count = usize::from(size);
        let available = AlignedVring::new(4 + count * 2 + 2);
        // VPP tap.c:327,381 disables per-descriptor interrupts.
        unsafe { ptr::write_volatile(available.ptr().cast::<u16>(), 1) };
        Self {
            descriptors: AlignedVring::new(count * size_of::<Descriptor>()),
            available,
            used: AlignedVring::new(4 + count * size_of::<UsedElement>() + 2),
            size,
        }
    }

    #[inline(always)]
    pub(crate) fn descriptor(&self, index: u16) -> Descriptor {
        assert!(index < self.size);
        unsafe {
            ptr::read_volatile(
                self.descriptors
                    .ptr()
                    .cast::<Descriptor>()
                    .add(index as usize),
            )
        }
    }

    #[inline(always)]
    pub(crate) fn set_descriptor(&self, index: u16, descriptor: Descriptor) {
        assert!(index < self.size);
        unsafe {
            ptr::write_volatile(
                self.descriptors
                    .ptr()
                    .cast::<Descriptor>()
                    .add(index as usize),
                descriptor,
            )
        };
    }

    #[inline(always)]
    pub(crate) fn used_index(&self) -> u16 {
        unsafe { AtomicU16::from_ptr(self.used.ptr().add(2).cast()).load(Ordering::Acquire) }
    }

    #[inline(always)]
    pub(crate) fn used_element(&self, index: u16) -> UsedElement {
        unsafe {
            ptr::read_volatile(
                self.used
                    .ptr()
                    .add(4)
                    .cast::<UsedElement>()
                    .add((index & (self.size - 1)) as usize),
            )
        }
    }

    #[inline(always)]
    pub(crate) fn available_index(&self) -> u16 {
        unsafe { AtomicU16::from_ptr(self.available.ptr().add(2).cast()).load(Ordering::Relaxed) }
    }

    #[inline(always)]
    pub(crate) fn set_available(&self, index: u16, descriptor: u16) {
        unsafe {
            ptr::write_volatile(
                self.available
                    .ptr()
                    .add(4)
                    .cast::<u16>()
                    .add((index & (self.size - 1)) as usize),
                descriptor,
            )
        };
    }

    #[inline(always)]
    pub(crate) fn publish_available(&self, index: u16) {
        unsafe {
            AtomicU16::from_ptr(self.available.ptr().add(2).cast()).store(index, Ordering::Release)
        };
    }

    #[inline(always)]
    pub(crate) fn notify_enabled(&self) -> bool {
        unsafe { AtomicU16::from_ptr(self.used.ptr().cast()).load(Ordering::Acquire) & 1 == 0 }
    }

    pub(crate) fn install(
        &self,
        fd: &OwnedFd,
        index: u32,
        kick: &OwnedFd,
        call: Option<&OwnedFd>,
        backend: &OwnedFd,
    ) -> io::Result<()> {
        ioctl(
            fd,
            SET_VRING_NUM,
            &VringState {
                index,
                num: self.size as u32,
            },
        )?;
        ioctl(
            fd,
            SET_VRING_ADDR,
            &VringAddress {
                index,
                flags: 0,
                desc_user_addr: self.descriptors.ptr() as u64,
                used_user_addr: self.used.ptr() as u64,
                avail_user_addr: self.available.ptr() as u64,
                log_guest_addr: 0,
            },
        )?;
        if let Some(call) = call {
            ioctl(
                fd,
                SET_VRING_CALL,
                &VringFile {
                    index,
                    fd: call.as_raw_fd(),
                },
            )?;
        }
        ioctl(
            fd,
            SET_VRING_KICK,
            &VringFile {
                index,
                fd: kick.as_raw_fd(),
            },
        )?;
        ioctl(
            fd,
            SET_BACKEND,
            &VringFile {
                index,
                fd: backend.as_raw_fd(),
            },
        )
    }

    pub(crate) fn stop(&self, fd: &OwnedFd, index: u32) -> io::Result<()> {
        ioctl(fd, SET_BACKEND, &VringFile { index, fd: -1 })
    }
}

pub(crate) fn eventfd() -> io::Result<OwnedFd> {
    let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: eventfd returned a new owned descriptor.
        Ok(unsafe { std::os::fd::FromRawFd::from_raw_fd(fd) })
    }
}

pub(crate) fn kick(fd: &OwnedFd) {
    let value = 1u64;
    let written = unsafe {
        libc::write(
            fd.as_raw_fd(),
            (&value as *const u64).cast(),
            size_of::<u64>(),
        )
    };
    assert!(
        written == size_of::<u64>() as isize
            || (written < 0 && io::Error::last_os_error().kind() == io::ErrorKind::WouldBlock),
        "vhost queue kick must either complete or already be pending"
    );
}
