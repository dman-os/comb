const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const mod_utils = @import("utils.zig");
const println = mod_utils.println;
const dbg = mod_utils.dbg;

pub const FAN = struct {
    /// Events that user-space can register for.
    pub const EVENT = struct {
        pub const INFO_TYPE = struct {
            pub const FID = 1;
            pub const DFID_NAME = 2;
            pub const DFID = 3;
            pub const PIDFD = 4;
            pub const ERROR = 5;
            pub const OLD_DFID_NAME = 10;
            pub const NEW_DFID_NAME = 12;
        };

        /// File was accessed
        ///
        /// Note: `readdir` does not generate a `FAN_ACCESS` event.
        pub const ACCESS = 0x01;
        /// File is modified (write).
        pub const MODIFY = 0x02;
        /// Metadata for a file or directory has changed.
        ///
        /// (since Linux 5.1)
        pub const ATTRIB = 0x04;
        /// Writable file is closed.
        pub const CLOSE_WRITE = 0x08;
        /// Read-only file or directory is closed.
        pub const CLOSE_NOWRITE =  0x10;
        /// File is closed (CLOSE_WRITE|CLOSE_NOWRITE).
        pub const CLOSE = CLOSE_WRITE | CLOSE_NOWRITE;
        /// File or directory is opened.
        pub const OPEN = 0x20;
        /// File or directory was moved from a monitored parent.
        ///
        /// (since Linux 5.1)
        /// An fanotify group that identifies filesystem objects by file handles is required.
        pub const MOVED_FROM = 0x40;
        /// File or directory was moved to a monitored parent.
        ///
        /// (since Linux 5.1)
        /// An fanotify group that identifies filesystem objects by file handles is required.
        pub const MOVED_TO = 0x80;
        /// File or directory was moved (MOVED_FROM|MOVED_TO)
        pub const MOVE = MOVED_FROM | MOVED_TO;
        /// File or directory has been created.
        ///
        /// (since Linux 5.1)
        /// An fanotify group that identifies filesystem objects by file handles is required.
        pub const CREATE = 0x100;
        /// File or directory was deleted.
        ///
        /// (since Linux 5.1)
        /// An fanotify group that identifies filesystem objects by file handles is required.
        pub const DELETE = 0x200;
        /// A marked file or directory itself is deleted.
        ///
        /// (since Linux 5.1)
        /// An fanotify group that identifies filesystem objects by file handles is required.
        pub const DELETE_SELF = 0x400;
        /// A marked file or directory itself has been moved.
        ///
        /// (since Linux 5.1)
        /// An fanotify group that identifies filesystem objects by file handles is required.
        pub const MOVE_SELF = 0x800;
        /// File is opened with the intent to be executed.'
        ///
        /// (since Linux 5.1)
        /// An fanotify group that identifies filesystem objects by file handles is required.
        pub const OPEN_EXEC = 0x1000;
        /// Permission to open file or directory is requested.
        ///
        /// An fanotify file descriptor created with FAN_CLASS_PRE_CONTENT or FAN_CLASS_CONTENT is required.
        pub const OPEN_PERM = 0x10000;
        /// Permission to read a file or directoryis requested.
        ///
        /// An fanotify file descriptor created with CLASS_PRE_CONTENT or CLASS_CONTENT is required.
        pub const ACCESS_PERM = 0x20000;
        /// Permission to open a file for execution is requested.
        ///
        /// (since Linux 5.0)
        /// An fanotify file descriptor created with CLASS_PRE_CONTENT or CLASS_CONTENT is required.
        pub const OPEN_EXEC_PERM = 0x40000;
        /// Events for the immediate children of marked directories shall be created.
        /// The flag has no effect when marking mounts and filesystems.
        /// Note that events are not generated for children of the subdirectories of marked directories.
        /// More specifically, the directory entry modification events CREATE,
        /// DELETE, MOVED_FROM, and MOVED_TO are not generated for any
        /// entry modifications performed inside subdirectories of marked directories.
        /// Note that the events DELETE_SELF and MOVE_SELF are not generated for
        /// children of marked directories. To monitor complete directory trees it
        /// is necessary to mark the relevant mount or filesystem.
        pub const EVENT_ON_CHILD = 0x08000000;
        /// File was renamed. (TODO: this isn't present in `musl` nor in fanotify (7). Investigate!)
        /// https://lkml.kernel.org/linux-fsdevel/20211119071738.1348957-10-amir73il@gmail.com/
        pub const RENAME = 0x10000000;
        /// Create events for directories—for example, when opendir, readdir and closedir are called.
        ///
        /// Without this flag, events are created only for files.
        ///
        /// In the context of directory entry events, such as FAN_CREATE,FAN_DELETE, FAN_MOVED_FROM, and
        /// FAN_MOVED_TO, specifying the flag FAN_ONDIR is required in order to create events when subdirectory
        /// entries are modified (i.e., mkdir(2)/ rmdir(2)).
        pub const ONDIR = 0x40000000;
    };

    /// Listeners with different notification classes will receive events in the order 
    /// PRE_CONTENT,  CONTENT,  NOTIF.
    /// The order of notification for listeners in the same notification class is undefined.
    /// These are NOT bitwise flags.  Both bits are used together.
    pub const CLASS = enum (c_uint) {
        /// This is the default value. It does not need to be specified. 
        /// This value only allows the receipt of events notifying that a file has
        /// been accessed. Permission decisions before the file is accessed are not possible.
        NOTIF = 0,
        /// This  value allows the receipt of events notifying that a file has
        /// been accessed and events for permission decisions if a file may be
        ///  accessed. It is intended for event listeners that need to access 
        ///  files when they already contain their final content.
        CONTENT = 0x04,
        /// This  value allows the receipt of events notifying that a file has 
        /// been accessed and events for permission decisions if a file may be
        /// accessed.  It is intended for event listeners that need to access files 
        /// before they contain their final data.
        PRE_CONTENT = 0x08,
        // ALL_CLASS_BITS = @compileError("deprecated"),
    };

    /// Flags to customize operation of the fanotify group/file descriptor.
    pub const INIT = struct {
        /// Set the close-on-exec flag (FD.CLOEXEC) on the new file descriptor.
        ///
        /// The fanotify file descriptor will be closed when exec is used to change program.
        pub const CLOEXEC = @as(c_uint, 0x01);
        /// Enable the nonblocking flag (O_NONBLOCK) for the file descriptor.
        pub const NONBLOCK = @as(c_uint, 0x02);
        /// Remove the limit of 16384 events for the event queue. 
        /// Use of this flag requires the CAP_SYS_ADMIN capability.
        pub const UNLIMITED_QUEUE = @as(c_uint, 0x10);
        /// Remove the limit of 8192 marks. 
        /// Use of this flag requires the CAP_SYS_ADMIN capability.
        pub const UNLIMITED_MARKS = @as(c_uint, 0x20);
        /// Enable  generation  of audit log records about access mediation 
        /// performed by permission events.
        ///
        /// (since Linux 4.15)
        /// The permission event response has to be marked with the FAN_AUDIT flag
        /// for an audit log record to be generated.
        pub const ENABLE_AUDIT = @as(c_uint, 0x40);
        /// Report pidfd for event->pid.
        /// TODO: flag undocumented in fanotify(7)
        pub const REPORT_PIDFD = @as(c_uint, 0x00000080);
        
        /// Report thread ID (TID) instead of process ID (PID) in the pid field of the struct fanotify_event_metadata supplied
        ///
        /// (since Linux 4.20)
        pub const REPORT_TID = @as(c_uint, 0x100);
        /// Report using [`file_handle`]. Read `fanotify_init(2)`
        pub const REPORT_FID = @as(c_uint, 0x200);
        /// Report directory [`file_handle`]. Read `fanotify_init(2)`
        pub const REPORT_DIR_FID = @as(c_uint, 0x00000400);
        /// Report events with file name. Read `fanotify_init(2)`
        pub const REPORT_NAME = @as(c_uint, 0x00000800);
        pub const REPORT_TARGET_FID = @as(c_uint, 0x00001000);
        /// This is a synonym for (FAN_REPORT_DIR_FID|FAN_REPORT_NAME).
        pub const REPORT_DFID_NAME = REPORT_DIR_FID | REPORT_NAME;
        /// This is a synonym for (REPORT_DFID_NAME |REPORT_FID | REPORT_TARGET_FID).
        pub const REPORT_DFID_NAME_TARGET = (REPORT_DFID_NAME | REPORT_FID) | REPORT_TARGET_FID;
    };

    pub const MARK = struct {
        pub const MOD = enum (c_uint) {
            /// Register to the events in the mask.
            ADD = 0x01,
            /// Remove the events from the mask.
            REMOVE = 0x02,
            /// Remove  either  all  marks for filesystems, all marks for mounts,
            /// or all marks for directories and files from the fanotify group
            FLUSH = 0x80,
        };
        /// These are NOT bitwise flags.  Both bits can be used togther.
        // pub const TARGET = enum (c_uint) {
        // };
        /// Mark  the  mount  specified by pathname.
        ///
        /// f pathname is not itself a mount point, the mount containing pathname will be marked.
        /// All directories, subdirectories, and the contained files of the mount will be monitored.
        /// The events which require  that  filesystem  objects  are  identified  by  file handles,
        /// such as `FAN_CREATE`, `FAN_ATTRIB`, `FAN_MOVE`, and `FAN_DELETE_SELF`, cannot be provided as a
        /// mask when flags contains `FAN_MARK_MOUNT`. Attempting to do so will result in the error EINVAL being returned.
        pub const MOUNT = 0x10;
        pub const INODE = 0x00;
        /// Mark the filesystem specified by pathname.  The filesystem containing pathname will be marked.
        /// All the contained files and  directories of the filesystem from any mount point will be monitored.
        pub const FILESYSTEM = 0x100;

        /// If pathname is a symbolic link, mark the link itself, rather than the file to which it refers.
        ///
        /// By default, [`fanotify_mark`] dereferences pathname if it is a symbolic link.
        pub const DONT_FOLLOW = 0x04;
        /// If the filesystem object to be marked is not a directory, the error ENOTDIR shall be raised.
        pub const ONLYDIR = 0x08;
        /// The events in mask shall be added to or removed from the ignore mask.
        pub const IGNORED_MASK = 0x20;
        /// The  ignore mask shall survive modify events.
        ///
        /// If this flag is not set, the ignore mask is cleared when a modify event occurs for the ignored file or directory.
        pub const IGNORED_SURV_MODIFY = 0x40;
        // TYPE_MASK = (FAN_MARK.INODE | FAN_MARK.MOUNT) | FAN_MARK.FILESYSTEM,
    };

    /// Event queued overflowed
    pub const Q_OVERFLOW = 0x4000;
    /// Filesystem error
    pub const FS_ERROR = 0x00008000;
    pub const DIR_MODIFY = 0x00080000;

    // ALL_PERM_EVENTS = @compileError("deprecated"),
    // ALL_OUTGOING_EVENTS = @compileError("deprecated"),


    // These maybe used on [`response.response`]
    pub const ALLOW = @as(c_uint, 0x01);
    pub const DENY = @as(c_uint, 0x02);
    pub const AUDIT = @as(c_uint, 0x10);

    // [`event_metadata.fd`] might hold these values
    pub const NOFD = @as(c_int, -1);
    pub const NOPIDFD = NOFD;
    pub const EPIDFD = @as(c_int, -2);
};

pub const METADATA_VERSION = @as(usize, 3);

pub const event_metadata = extern struct {
    event_len: c_uint,
    vers: u8,
    reserved: u8,
    metadata_len: c_ushort,
    mask: c_ulonglong align(8),
    fd: c_int,
    pid: c_int,
};


/// Variable length info record following event metadata
pub const event_info_header = extern struct {
    info_type: u8,
    pad: u8,
    len: c_ushort,
};


/// Unique file identifier info record.
/// This structure is used for records of types FAN_EVENT_INFO_TYPE_FID,
/// EVENT_INFO_TYPE_DFID and EVENT_INFO_TYPE_DFID_NAME.
/// For EVENT_INFO_TYPE_DFID_NAME there is additionally a null terminated
/// name immediately after the file handle.
pub const event_info_fid = extern struct {
    hdr: event_info_header align(4),
    /// Treat this like dev_major/dev_minor fro statx
    fsid: [2]c_int, 
    handle: file_handle,
    // pub fn handle(self: anytype) std.zig.c_translation.FlexibleArrayType(@TypeOf(self), u8) {
    //     const Intermediate = std.zig.c_translation.FlexibleArrayType(@TypeOf(self), u8);
    //     const ReturnType = std.zig.c_translation.FlexibleArrayType(@TypeOf(self), u8);
    //     return @ptrCast(ReturnType, @alignCast(@alignOf(u8), @ptrCast(Intermediate, self) + 12));
    // }
};

pub const file_handle =  extern struct {
    handle_bytes: u32,
    handle_type: i32,
    /// I'm opqaue, just point to me
    f_handle: u8,
};

pub const event_info_pidfd = extern struct {
    hdr: event_info_header,
    pidfd: c_int,
};

pub const event_info_error = extern struct {
    hdr: event_info_header,
    @"error": c_int,
    error_count: c_uint,
};

pub const response = extern struct {
    fd: c_int,
    response: c_uint,
};

pub const FanotifyInitErr = error {
    /// The allocation of memory for the notification group failed.
    SystemResources,
    /// The operation is not permitted because the caller lacks the CAP_SYS_ADMIN capability.
    AccessDenied,
    /// The number of fanotify groups for this user exceeds 128.
    /// or The per-process limit on the number of open file descriptors has been reached.
    ProcessFdQuotaExceeded,
    /// This kernel does not implement fanotify_init().  The fanotify API is 
    /// available only  if  the  kernel  was configured with CONFIG_FANOTIFY.
    OperationNotSupported,
} || std.os.UnexpectedError;

/// Read `fanotify_init(2)`
pub fn init(
    class: FAN.CLASS, 
    flags: c_uint, 
    event_flags: c_uint,
) FanotifyInitErr!std.os.fd_t {
    const resp = std.os.linux.syscall2(
        .fanotify_init, 
        @enumToInt(class) | flags, 
        event_flags
    );
    switch (std.os.errno(resp)) {
        .SUCCESS => return @intCast(std.os.fd_t, resp),
        .INVAL => unreachable,
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NOSYS => return error.OperationNotSupported,
        .NOMEM => return error.SystemResources,
        // .PERM => @panic("permission denied: CAP_SYS_ADMIN required, sudo me"),
        .PERM => return error.AccessDenied,
        else => |err| return std.os.unexpectedErrno(err),
    }
}

pub const FanotifyMarkErr = error {
    /// `path` is relative but dirfd is neither AT_FDCWD nor a valid file descriptor.
    /// or 
    /// The filesystem object indicated by dirfd and pathname does not exist.  This 
    /// error also occurs when trying to remove a mark from an object which is not marked.
    FileNotFound,
    /// The fanotify file descriptor was opened with FAN_CLASS_NOTIF or the 
    /// fanotify group identifies  filesystem objects  by  file  handles  and  
    /// mask  contains  a  flag  for permission events (FAN_OPEN_PERM or FAN_ACCESS_PERM).
    InvalidFlags,
    /// The filesystem object indicated by pathname is not associated with a filesystem 
    /// that supports fsid (e.g.,tmpfs(5)). 
    /// or 
    /// The object  indicated  by pathname is associated with a filesystem that does 
    /// not support the encoding of file handles.
    /// or 
    ///  The filesystem object indicated by pathname resides within a filesystem 
    ///  subvolume (e.g., btrfs(5)) which uses a different fsid than its root superblock.  
    ///
    ///  This error can be returned only with an  fanotify  group that identifies filesystem objects by file handles.
    UnsupportedFS,
    /// The necessary memory could not be allocated.
    SystemResources,
    /// The operation is not permitted because the caller lacks the CAP_SYS_ADMIN capability.
    AccessDenied,
    /// The number of fanotify groups for this user exceeds 128.
    /// or The per-process limit on the number of open file descriptors has been reached.
    ProcessFdQuotaExceeded,
    /// The number of marks exceeds the limit of 8192 and the FAN_UNLIMITED_MARKS 
    /// flag was not specified when the fanotify file descriptor was created with fanotify_init(2).
    NoSpaceLeft,
    /// `flags` contains FAN_MARK_ONLYDIR, and dirfd and pathname do not specify a directory.
    NotDir,
    /// This kernel does not implement fanotify_mark().  The fanotify API is 
    /// available only  if  the  kernel  was configured with CONFIG_FANOTIFY.
    OperationNotSupported,
} || std.os.UnexpectedError;

/// Read `fanotify_mark(2)`
pub fn mark(
    fanotify_fd: std.os.fd_t, 
    action: FAN.MARK.MOD, 
    // target: FAN.MARK.TARGET, 
    flags: c_uint,
    mask: c_uint, 
    dir_fd: std.os.fd_t, 
    path: [:0]const u8
) !void {
    const resp = std.os.linux.syscall5(
        .fanotify_mark, 
        @bitCast(usize, @as(isize, fanotify_fd)),
        @enumToInt(action) | flags, 
        mask,
        @bitCast(usize, @as(isize, dir_fd)),
        @ptrToInt(path.ptr),
    );
    switch (std.os.errno(resp)) {
        .SUCCESS => {},
        .BADF => return error.FileNotFound,
        .INVAL => return error.InvalidFlags,
        .NODEV => return error.UnsupportedFS,
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NOENT => return error.FileNotFound,
        .NOSYS => return error.OperationNotSupported,
        .NOMEM => return error.SystemResources,
        .NOSPC => return error.NoSpaceLeft,
        .NOTDIR => return error.NotDir,
        .OPNOTSUPP => return error.UnsupportedFS,
        .XDEV => return error.UnsupportedFS,
        else => |err| return std.os.unexpectedErrno(err),
    }
}

pub const OpenByHandleErr = error {
    BadFileDescriptor,
    /// The specified handle is not valid.  This error will occur if, for example, the file has been deleted.
    StaleFileHandle,
} || std.os.OpenError;

/// Read the man page
pub fn open_by_handle_at(
    mount_fd: std.os.fd_t,
    handle: *const file_handle,
    flags: @typeInfo(std.os.O).Enum.tag_type, 
) OpenByHandleErr!std.os.fd_t {
    while (true) {
        const resp = std.os.linux.syscall3(.open_by_handle_at, mount_fd, handle, flags);
        switch (std.os.errno(resp)) {
            .SUCCESS => return @intCast(std.os.fd_t, resp),
            // we was interrupted, try again
            .INTR => continue,
            .FAULT => unreachable,
            .INVAL => unreachable,
            .STALE => return error.StaleFileHandle,
            .BADF => return error.BadFileDescriptor,
            .ACCES => return error.AccessDenied,
            .FBIG => return error.FileTooBig,
            .OVERFLOW => return error.FileTooBig,
            .ISDIR => return error.IsDir,
            .LOOP => return error.SymLinkLoop,
            .MFILE => return error.ProcessFdQuotaExceeded,
            .NAMETOOLONG => return error.NameTooLong,
            .NFILE => return error.SystemFdQuotaExceeded,
            .NODEV => return error.NoDevice,
            .NOENT => return error.FileNotFound,
            .NOMEM => return error.SystemResources,
            .NOSPC => return error.NoSpaceLeft,
            .NOTDIR => return error.NotDir,
            .PERM => return error.AccessDenied,
            .EXIST => return error.PathAlreadyExists,
            .BUSY => return error.DeviceBusy,
            .OPNOTSUPP => return error.FileLocksNotSupported,
            .AGAIN => return error.WouldBlock,
            .TXTBSY => return error.FileBusy,
            else => |err| return std.os.unexpectedErrno(err),
        }
    }
}
