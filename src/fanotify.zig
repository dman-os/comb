const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const mod_utils = @import("utils.zig");
const println = mod_utils.println;
const dbg = mod_utils.dbg;
const Option = mod_utils.Option;
const OptionStr = Option([]const u8);

pub const FanotifyEvent = struct {
    const Self = @This();
    dir: ?[]u8,
    name: ?[]u8,
    // mask: c_ulonglong align(8),
    kind: FAN.EVENT,
    pid: c_int,
    timestamp: i64,

    // avoid const optional slices: https://github.com/ziglang/zig/issues/4907
    pub fn init(
        a7r: std.mem.Allocator,
        name: OptionStr,
        dir: OptionStr,
        meta: *const event_metadata,
        timestamp: i64,
    ) !Self {
        return Self{
            .name = if (name.toNative()) |slice| try a7r.dupe(u8, slice) else null,
            .dir = if (dir.toNative()) |slice| try a7r.dupe(u8, slice) else null,
            // .mask = meta.mask,
            .kind = @bitCast(FAN.EVENT, @truncate(u32, meta.mask)),
            .pid = meta.pid,
            .timestamp = timestamp,
        };
    }
    pub fn deinit(self: *Self, a7r: std.mem.Allocator) void {
        if (self.name) |slice| {
            a7r.free(slice);
        }
        if (self.dir) |slice| {
            a7r.free(slice);
        }
    }

    pub fn format(
        self: Self, 
        comptime fmt: []const u8, 
        options: std.fmt.FormatOptions, 
        writer: anytype
    ) !void {
        _ = fmt;
        _ = options;
        try std.fmt.format(
            writer, 
            @typeName(Self) ++ "{{ .name = {?s}, .dir = {?s}, .kind = {}, .pid = {} .timestamp = {} }}", 
            // @typeName(Self) ++ "{{\n .name = {?s},\n .dir = {?s},\n .kind = {},\n .pid = {}\n .timestamp = {} }}", 
            .{ self.name, self.dir, self.kind, self.pid, self.timestamp }
        );
    }
};

pub const FAN = struct {
    /// Events that user-space can register for.
    pub const EVENT = packed struct {
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
        /// 1st bit
        access: bool = false,

        /// File is modified (write).
        /// 2nd bit
        modify: bool = false,

        /// Metadata for a file or directory has changed.
        ///
        /// (since Linux 5.1)
        /// 3rd bit
        attrib: bool = false,

        /// Writable file is closed.
        /// 4th bit
        close_write: bool = false,

        /// Read-only file or directory is closed.
        /// 5th bit
        close_nowrite: bool = false,

        /// File is closed (CLOSE_WRITE|CLOSE_NOWRITE).
        /// File or directory is opened.
        /// 6th bit
        open: bool = false,

        /// File or directory was moved from a monitored parent.
        ///
        /// (since Linux 5.1)
        /// An fanotify group that identifies filesystem objects by file handles is required.
        /// 7th bit
        moved_from: bool = false,

        /// File or directory was moved to a monitored parent.
        ///
        /// (since Linux 5.1)
        /// An fanotify group that identifies filesystem objects by file handles is required.
        /// 8th bit
        moved_to: bool = false,


        /// File or directory has been created.
        ///
        /// (since Linux 5.1)
        /// An fanotify group that identifies filesystem objects by file handles is required.
        /// 9th bit
        create: bool = false,

        /// File or directory was deleted.
        ///
        /// (since Linux 5.1)
        /// An fanotify group that identifies filesystem objects by file handles is required.
        /// 10th bit
        delete: bool = false,

        /// A marked file or directory itself is deleted.
        ///
        /// (since Linux 5.1)
        /// An fanotify group that identifies filesystem objects by file handles is required.
        /// 11th bit
        delete_self: bool = false,

        /// A marked file or directory itself has been moved.
        ///
        /// (since Linux 5.1)
        /// An fanotify group that identifies filesystem objects by file handles is required.
        /// 12th bit
        move_self: bool = false,

        /// File is opened with the intent to be executed.'
        ///
        /// (since Linux 5.1)
        /// An fanotify group that identifies filesystem objects by file handles is required.
        /// 13th bit
        open_exec: bool = false,

        _padding_1: u3 = 0,
        /// Permission to open file or directory is requested.
        ///
        /// An fanotify file descriptor created with FAN_CLASS_PRE_CONTENT or FAN_CLASS_CONTENT is required.
        /// 17th bit
        open_perm: bool = false,

        /// Permission to read a file or directoryis requested.
        ///
        /// An fanotify file descriptor created with CLASS_PRE_CONTENT or CLASS_CONTENT is required.
        /// 18th bit
        access_perm: bool = false,

        /// Permission to open a file for execution is requested.
        ///
        /// (since Linux 5.0)
        /// An fanotify file descriptor created with CLASS_PRE_CONTENT or CLASS_CONTENT is required.
        /// 19th bit
        open_exec_perm: bool = false,

        _padding_2: u8 = 0,

        /// Events for the immediate children of marked directories shall be created.
        /// The flag has no effect when marking mounts and filesystems.
        /// Note that events are not generated for children of the subdirectories of marked directories.
        /// More specifically, the directory entry modification events CREATE,
        /// DELETE, MOVED_FROM, and MOVED_TO are not generated for any
        /// entry modifications performed inside subdirectories of marked directories.
        /// Note that the events DELETE_SELF and MOVE_SELF are not generated for
        /// children of marked directories. To monitor complete directory trees it
        /// is necessary to mark the relevant mount or filesystem.
        /// 28th bit
        event_on_child: bool = false,

        /// File was renamed. (TODO: this isn't present in `musl` nor in fanotify (7). Investigate!)
        /// https://lkml.kernel.org/linux-fsdevel/20211119071738.1348957-10-amir73il@gmail.com/
        /// 28th bit
        rename: bool = false,

        _padding_3: u1 = 0,

        /// Create events for directories—for example, when opendir, readdir and closedir are called.
        ///
        /// Without this flag, events are created only for files.
        ///
        /// In the context of directory entry events, such as FAN_CREATE,FAN_DELETE, FAN_MOVED_FROM, and
        /// FAN_MOVED_TO, specifying the flag FAN_ONDIR is required in order to create events when subdirectory
        /// entries are modified (i.e., mkdir(2)/ rmdir(2)).
        /// 32nd bit
        ondir: bool = false,
        _padding_4: u1 = 0,

        pub const ACCESS = 0x01;
        pub const MODIFY = 0x02;
        pub const ATTRIB = 0x04;
        pub const CLOSE_WRITE = 0x08;
        pub const CLOSE_NOWRITE = 0x10;
        pub const CLOSE = CLOSE_WRITE | CLOSE_NOWRITE;
        pub const OPEN = 0x20;
        pub const MOVED_FROM = 0x40;
        pub const MOVED_TO = 0x80;
        /// File or directory was moved (MOVED_FROM|MOVED_TO)
        pub const MOVE = MOVED_FROM | MOVED_TO;
        pub const CREATE = 0x100;
        pub const DELETE = 0x200;
        pub const DELETE_SELF = 0x400;
        pub const MOVE_SELF = 0x800;
        pub const OPEN_EXEC = 0x1000;
        pub const OPEN_PERM = 0x10000;
        pub const ACCESS_PERM = 0x20000;
        pub const OPEN_EXEC_PERM = 0x40000;
        pub const EVENT_ON_CHILD = 0x08000000;
        pub const RENAME = 0x10000000;
        pub const ONDIR = 0x40000000;

        fn asInt(self: @This()) u32 {
            return @bitCast(u32, self);
        }

        pub fn format(
            self: @This(), 
            comptime fmt: []const u8, 
            options: std.fmt.FormatOptions, 
            writer: anytype
        ) !void {
            _ = fmt;
            _ = options;
            try std.fmt.format(writer, @typeName(@This()) ++ " {{", .{});
            // try std.fmt.format(writer, " {}: ", .{ self.asInt() });
            comptime var tinfo: std.builtin.Type.Struct = @typeInfo(@This()).Struct;
            inline for (tinfo.fields) |field| {
                if (field.field_type == bool)
                    if (@field(self, field.name))
                        try std.fmt.format(writer, " {s},", .{ field.name });
            }
            try std.fmt.format(writer, " }}", .{});
        }

        comptime {
            const Self = @This();
            if (@sizeOf(Self) != @sizeOf(u32)) {
                @compileError(std.fmt.comptimePrint(
                    "unexpected size mismatch: {} !=  {}\n",
                    .{ @sizeOf(Self), @sizeOf(u32) }
                ));
            }
            if (@bitSizeOf(Self) != @bitSizeOf(u32)) {
                @compileError(std.fmt.comptimePrint(
                    "unexpected bitSize mismatch: {} !=  {}\n",
                    .{ @bitSizeOf(Self), @bitSizeOf(u32) }
                ));
            }
            var table = .{
                .{ Self { .access = true }, ACCESS },
                .{ Self { .modify = true }, MODIFY },
                .{ Self { .attrib = true }, ATTRIB },
                .{ Self { .close_write = true }, CLOSE_WRITE },
                .{ Self { .close_nowrite = true }, CLOSE_NOWRITE },
                .{ Self { .open = true }, OPEN },
                .{ Self { .moved_from = true }, MOVED_FROM },
                .{ Self { .moved_to = true }, MOVED_TO },
                .{ Self {.moved_from = true, .moved_to = true }, MOVE },
                .{ Self { .create = true }, CREATE },
                .{ Self { .delete = true }, DELETE },
                .{ Self { .delete_self = true }, DELETE_SELF },
                .{ Self { .move_self = true }, MOVE_SELF },
                .{ Self { .open_exec = true }, OPEN_EXEC },
                .{ Self { .open_perm = true }, OPEN_PERM },
                .{ Self { .access_perm = true }, ACCESS_PERM },
                .{ Self { .open_exec_perm = true }, OPEN_EXEC_PERM },
                .{ Self { .event_on_child = true }, EVENT_ON_CHILD },
                .{ Self { .rename = true }, RENAME },
                .{ Self { .ondir = true }, ONDIR },
            };
            for (table) |case| {
                if (case[0].asInt() != @as(u32, case[1])) {
                    @compileError(std.fmt.comptimePrint(
                        "unexpected bit set: {}({}) !=  {}\n",
                        .{ case[0], case[0].asInt(), case[1] }
                    ));
                }
            }
        }
    };

    /// Listeners with different notification classes will receive events in the order
    /// PRE_CONTENT,  CONTENT,  NOTIF.
    /// The order of notification for listeners in the same notification class is undefined.
    /// These are NOT bitwise flags.  Both bits are used together.
    pub const CLASS = enum(c_uint) {
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
    pub const INIT = packed struct {
        /// Set the close-on-exec flag (FD.CLOEXEC) on the new file descriptor.
        ///
        /// The fanotify file descriptor will be closed when exec is used to change program.
        /// 1st bit
        cloexec: bool = false,
        /// Enable the nonblocking flag (O_NONBLOCK) for the file descriptor.
        /// 2nd bit
        nonblock: bool = false,
        /// Remove the limit of 16384 events for the event queue.
        /// Use of this flag requires the CAP_SYS_ADMIN capability.
        _padding_1: u2 = 0,
        /// 5th bit
        unlimited_queue: bool = false,
        /// Remove the limit of 8192 marks.
        /// Use of this flag requires the CAP_SYS_ADMIN capability.
        /// 6th bit
        unlimited_marks: bool = false,
        /// Enable  generation  of audit log records about access mediation
        /// performed by permission events.
        ///
        /// (since Linux 4.15)
        /// The permission event response has to be marked with the FAN_AUDIT flag
        /// for an audit log record to be generated.
        /// 7th bit
        enable_audit: bool = false,
        /// Report pidfd for event->pid.
        /// TODO: flag undocumented in fanotify(7)
        /// 8th bit
        report_pidfd: bool = false,

        /// Report thread ID (TID) instead of process ID (PID) in the pid field 
        /// of the struct fanotify_event_metadata supplied
        ///
        /// (since Linux 4.20)
        /// 9th bit
        report_tid: bool = false,
        /// Report using [`file_handle`]. Read `fanotify_init(2)`
        /// 10th bit
        report_fid: bool = false,
        /// Report directory [`file_handle`]. Read `fanotify_init(2)`
        /// 11th bit
        report_dir_fid: bool = false,
        /// Report events with file name. Read `fanotify_init(2)`
        /// 12th bit
        report_name: bool = false,
        /// 13th bit
        report_target_fid: bool = false,

        pub const CLOEXEC = 0x01;
        pub const NONBLOCK = 0x02;
        pub const UNLIMITED_QUEUE = 0x10;
        pub const UNLIMITED_MARKS = 0x20;
        pub const ENABLE_AUDIT = 0x40;
        pub const REPORT_PIDFD = 0x80;
        pub const REPORT_TID = 0x100;
        pub const REPORT_FID = 0x200;
        pub const REPORT_DIR_FID = 0x400;
        pub const REPORT_NAME = 0x800;
        pub const REPORT_TARGET_FID = 0x1000;
        /// This is a synonym for (FAN_REPORT_DIR_FID|FAN_REPORT_NAME).
        pub const REPORT_DFID_NAME = REPORT_DIR_FID | REPORT_NAME;
        /// This is a synonym for (REPORT_DFID_NAME |REPORT_FID | REPORT_TARGET_FID).
        pub const REPORT_DFID_NAME_TARGET = (REPORT_DFID_NAME | REPORT_FID) | REPORT_TARGET_FID;

        fn asInt(self: @This()) u13 {
            return @bitCast(u13, self);
        }

        comptime {
            const Self = @This();
            if (@sizeOf(Self) != @sizeOf(u13)) {
                @compileError(std.fmt.comptimePrint(
                    "unexpected size mismatch: {} !=  {}\n",
                    .{ @sizeOf(Self), @sizeOf(u13) }
                ));
            }
            if (@bitSizeOf(Self) != @bitSizeOf(u13)) {
                @compileError(std.fmt.comptimePrint(
                    "unexpected bitSize mismatch: {} !=  {}\n",
                    .{ @bitSizeOf(Self), @bitSizeOf(u13) }
                ));
            }
            var table = .{
                .{ Self { .cloexec = true }, CLOEXEC },
                .{ Self { .nonblock = true }, NONBLOCK },
                .{ Self { .unlimited_queue = true }, UNLIMITED_QUEUE },
                .{ Self { .unlimited_marks = true }, UNLIMITED_MARKS },
                .{ Self { .enable_audit = true }, ENABLE_AUDIT },
                .{ Self { .report_pidfd = true }, REPORT_PIDFD },
                .{ Self { .report_tid = true }, REPORT_TID },
                .{ Self { .report_fid = true }, REPORT_FID },
                .{ Self { .report_dir_fid = true }, REPORT_DIR_FID },
                .{ Self { .report_name = true }, REPORT_NAME },
                .{ Self { .report_target_fid = true }, REPORT_TARGET_FID },
                .{ Self { .report_dir_fid = true, .report_name = true }, REPORT_DFID_NAME },
                .{ Self { 
                    .report_dir_fid = true, 
                    .report_name = true, 
                    .report_fid = true,
                    .report_target_fid = true 
                }, REPORT_DFID_NAME_TARGET },
            };
            for (table) |case| {
                if (case[0].asInt() != @as(u13, case[1])) {
                    @compileError(std.fmt.comptimePrint(
                        "unexpected bit set: {}({}) !=  {}\n",
                        .{ case[0], case[0].asInt(), case[1] }
                    ));
                }
            }
        }
    };

    pub const MARK = struct {
        pub const MOD = enum(c_uint) {
            /// Register to the events in the mask.
            ADD = 0x01,
            /// Remove the events from the mask.
            REMOVE = 0x02,
            /// Remove  either  all  marks for filesystems, all marks for mounts,
            /// or all marks for directories and files from the fanotify group
            FLUSH = 0x80,
        };
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

pub const file_handle = extern struct {
    handle_bytes: u32,
    handle_type: i32,
    /// I'm opqaue, just point to me
    f_handle: u8,

    pub fn file_name(self: @This()) [*:0]const u8 {
        return @intToPtr([*:0]u8, @ptrToInt(&self.f_handle) + @as(usize, self.handle_bytes));
    }
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

pub const FanotifyInitErr = error{
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
    flags: FAN.INIT,
    event_flags: c_uint,
) FanotifyInitErr!std.os.fd_t {
    const resp = std.os.linux.syscall2(.fanotify_init, @enumToInt(class) | flags.asInt(), event_flags);
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

pub const FanotifyMarkErr = error{
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
    mask: FAN.EVENT,
    dir_fd: std.os.fd_t,
    path: [:0]const u8,
) !void {
    const resp = std.os.linux.syscall5(
        .fanotify_mark,
        @bitCast(usize, @as(isize, fanotify_fd)),
        @enumToInt(action) | flags,
        @as(usize, mask.asInt()),
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

pub const OpenByHandleErr = error{
    BadFileDescriptor,
    /// The specified handle is not valid.  This error will occur if, for example, the file has been deleted.
    StaleFileHandle,
} || std.os.OpenError;

/// Read the man page
pub fn open_by_handle_at(
    mount_fd: std.os.fd_t,
    handle: *const file_handle,
    flags: c_int,
) OpenByHandleErr!std.os.fd_t {
    while (true) {
        const resp = std.os.linux.syscall3(
            .open_by_handle_at,
            @bitCast(usize, @as(isize, mount_fd)),
            @ptrToInt(handle),
            @bitCast(usize, @as(isize, flags)),
        );
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

pub const MountErr = error{
    AccessDenied,
    DeviceBusy,
} || std.os.UnexpectedError;

/// Read the man page
pub fn mount(
    source: [*:0]const u8, 
    dir: [*:0]const u8, 
    fstype: ?[*:0]const u8, 
    flags: u32, 
    data: usize
) MountErr!void {
    const resp = std.os.linux.mount(source, dir, fstype, flags, data);
    switch (std.os.errno(resp)) {
        .SUCCESS => {},
        // we was interrupted, try again
        .ACCES => return error.AccessDenied,
        .BUSY => return error.DeviceBusy,
        else => |err| return std.os.unexpectedErrno(err),
    }
}

const TestFanotify = struct {
    const Self = @This();
    pub const TouchFn = fn (a7r: Allocator, dir: std.fs.Dir) anyerror!void;

    events: std.ArrayList(FanotifyEvent),
    tmpfs_path: [:0]u8,
    tmp_dir: std.testing.TmpDir,
    tmpfs_dir: std.fs.Dir,

    /// Context for the test threads
    const Context = struct {
        const ContexSelf = @This();
        path: [:0]const u8,
        dir: std.fs.Dir,

        prePollTouch: TouchFn,
        touch: TouchFn,

        doDone: std.Thread.ResetEvent = .{},
        listenDone: std.Thread.ResetEvent = .{},
        startListen: std.Thread.ResetEvent = .{},
        listenReady: std.Thread.ResetEvent = .{},
        events: *std.ArrayList(FanotifyEvent),

        doErr: ?anyerror = null,
        listenErr: ?anyerror = null,

        fn doFn(ctx: *ContexSelf, alloc8orOuter: Allocator) void {
            defer ctx.doDone.set();
            ctx.doActual(alloc8orOuter) catch |err| {
                ctx.doErr = err;
            };
        }
        fn doActual(ctx: *ContexSelf, alloc8or: Allocator) !void {
            try @call(.{}, ctx.prePollTouch, .{ alloc8or, ctx.dir });
            // make sure they're ready to poll before you start doing shit
            ctx.listenReady.wait();
            // give them the heads up to start polling
            ctx.startListen.set();
            try @call(.{}, ctx.touch, .{ alloc8or, ctx.dir });
        }

        fn listenFn(ctx: *ContexSelf, alloc8or: Allocator) void {
            defer ctx.listenDone.set();
            ctx.listenActual(alloc8or) catch |err| {
                ctx.listenErr = err;
            };
        }

        fn listenActual(ctx: *ContexSelf, alloc8or: Allocator) !void {
            const fd = blk: {
                const fd = try init(
                    FAN.CLASS.NOTIF,
                    FAN.INIT {
                        .report_fid = true,
                        .report_dir_fid = true,
                        .report_name = true,
                    },
                    std.os.O.RDONLY | std.os.O.CLOEXEC,
                );
                try mark(
                    fd, 
                    FAN.MARK.MOD.ADD, 
                    FAN.MARK.FILESYSTEM, 
                    FAN.EVENT {
                        .create = true,
                        .modify = true,
                        .attrib = true,
                        .delete = true,
                        .moved_to = true,
                        .ondir = true,
                    },
                    std.os.AT.FDCWD, 
                    ctx.path
                );
                break :blk fd;
            };
            defer std.os.close(fd);

            var pollfds = [_]std.os.pollfd{std.os.pollfd{
                .fd = fd,
                .events = std.os.POLL.IN,
                .revents = 0,
            }};
            std.log.info("entering poll loop", .{});
            var buf = [_]u8{0} ** 256;

            // tell them we're ready to poll
            ctx.listenReady.set();
            // wait until they give us the heads up inorder to avoid
            // catching events they don't want us seeing
            ctx.startListen.wait();
            var breakOnNext = false;
            while (true) {
                if (breakOnNext) break;
                if (ctx.doDone.isSet()) {
                    // poll at least one cycle after they're done
                    // in case they give set `startListen` and `finishPoll`
                    // before we get to run
                    breakOnNext = true;
                }
                // println("looping", .{});

                // set timeout to zero to avoid polling forver incase
                // we start polling just before they set `doDone`
                const poll_num = try std.os.poll(&pollfds, 0);

                if (poll_num < 1) {
                    continue;
                    // std.log.err("err on poll: {}", .{ std.os.errno(poll_num) });
                    // @panic("err on poll");
                }

                const timestamp = std.time.timestamp();

                for (&pollfds) |pollfd| {
                    if (pollfd.revents == 0 or (pollfd.revents & std.os.POLL.IN) == 0) {
                        continue;
                    }
                    const fanotify_fd = pollfd.fd;
                    const read_len = try std.os.read(fanotify_fd, &buf);

                    var ptr = &buf[0];
                    var left_bytes = read_len;
                    var event_count: usize = 0;
                    while (true) {
                        const meta_ptr = @ptrCast(*align(1) const event_metadata, ptr);
                        const meta = meta_ptr.*;
                        if (
                            left_bytes < @sizeOf(event_metadata) 
                            or left_bytes < @intCast(usize, meta.event_len)
                            // FIXME: is this case even possible?
                            or meta.event_len < @sizeOf(event_metadata)
                        ) {
                            break;
                        }

                        if (meta.vers != METADATA_VERSION) {
                            @panic("unexpected " ++ @typeName(event_metadata) ++ " version");
                        }

                        if ((meta.mask & FAN.Q_OVERFLOW) > 0) @panic("queue overflowed");

                        if (try parseRawEvent(alloc8or, timestamp, meta_ptr)) |event|
                            try ctx.events.append(event);

                        ptr = @intToPtr(*u8, @ptrToInt(ptr) + meta.event_len);
                        left_bytes -= meta.event_len;
                        event_count += 1;
                    }
                    // std.log.info("{} events read this cycle:", .{ event_count, });
                    // for (events.items) |event, ii| {
                    //     std.log.info(
                    //         "Event: {} at {} with mask {} from pid {}",
                    //         .{ ii, event.timestamp, event.mask, event.pid }
                    //     );
                    //     std.log.info("  - {s}/{s}", .{ event.dir, event.name });
                    // }
                }
            }
        }
    };

    pub fn deinit(self: *Self, a7r: Allocator) void {
        for (self.events.items) |*evt| {
            evt.deinit(a7r);
        }
        self.events.deinit();
        self.tmpfs_dir.close();
        var sleep_time: usize = 0;
        while (true) {
            const resp = std.os.linux.umount(self.tmpfs_path);
            switch (std.os.errno(resp)) {
                .SUCCESS => break,
                .BUSY => {
                    if (sleep_time > 1 * 1_000_000_000) {
                        @panic("umount way too busy");
                    }
                    const ns = 1_000_000_00;
                    sleep_time += ns;
                    std.time.sleep(ns);
                },
                else => |err| {
                    println("err: {}", .{err});
                    @panic("umount failed");
                },
            }
        }
        a7r.free(self.tmpfs_path);
        self.tmp_dir.cleanup();
    }

    pub fn run(
        a7r: Allocator,
        prePollTouchFn: TouchFn,
        touchFn: TouchFn,
    ) !Self {
        var tmp_dir = std.testing.tmpDir(.{});

        const tmpfs_name = "tmpfs";
        try tmp_dir.dir.makeDir(tmpfs_name);

        var tmpfs_path = blk: {
            var path = try tmp_dir.dir.realpathAlloc(a7r, tmpfs_name);
            defer a7r.free(path);
            break :blk try std.mem.concatWithSentinel(a7r, u8, &.{path}, 0);
        };

        try mount("tmpfs", tmpfs_path, "tmpfs", 0, 0);

        var tmpfs_dir = try tmp_dir.dir.openDir("tmpfs", .{});

        var out = std.ArrayList(FanotifyEvent).init(a7r);
        var ctx = Context{
            .dir = tmpfs_dir,
            .path = tmpfs_path,
            .events = &out,
            .touch = touchFn,
            .prePollTouch = prePollTouchFn,
        };

        var listen_thread = try std.Thread.spawn(.{}, Context.listenFn, .{ &ctx, a7r });
        var do_thread = try std.Thread.spawn(.{}, Context.doFn, .{ &ctx, a7r });
        do_thread.detach();
        listen_thread.detach();

        ctx.doDone.timedWait(3 * 1_000_000_000) catch @panic("timeout waiting for do");
        ctx.listenDone.timedWait(3 * 1_000_000_000) catch @panic("timeout waiting for listen");

        if (ctx.doErr) |err| return err;
        if (ctx.listenErr) |err| return err;

        // @call(.{}, testFn, .{ a7r, ctx.events, })
        return Self{
            .tmp_dir = tmp_dir,
            .tmpfs_dir = tmpfs_dir,
            .tmpfs_path = tmpfs_path,
            .events = out,
        };
    }
};

pub fn demo() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    var a7r = gpa.allocator();

    const fd = blk: {
        const fd = try init(
            FAN.CLASS.NOTIF,
            FAN.INIT {
                .report_fid = true,
                .report_dir_fid = true,
                .report_name = true,
            },
            std.os.O.RDONLY | std.os.O.CLOEXEC,
        );
        try mark(
            fd, 
            FAN.MARK.MOD.ADD, 
            FAN.MARK.FILESYSTEM, 
            FAN.EVENT {
                .create = true,
                .modify = true,
                .attrib = true,
                .delete = true,
                .moved_to = true,
                .ondir = true,
            },
            std.os.AT.FDCWD, 
            "/"
        );
        break :blk fd;
    };
    defer std.os.close(fd);

    var pollfds = [_]std.os.pollfd{std.os.pollfd{
        .fd = fd,
        .events = std.os.POLL.IN,
        .revents = 0,
    }};

    var buf = [_]u8{0} ** 256;
    var events = std.ArrayList(FanotifyEvent).init(a7r);
    defer events.deinit();

    std.log.info("entering poll loop", .{});
    while (true) {
        // set timeout to zero to avoid polling forver incase
        // we start polling just before they set `doDone`
        const poll_num = try std.os.poll(&pollfds, -1);

        if (poll_num < 1) {
            continue;
            // std.log.err("err on poll: {}", .{ std.os.errno(poll_num) });
            // @panic("err on poll");
        }

        const timestamp = std.time.timestamp();

        for (&pollfds) |pollfd| {
            if (pollfd.revents == 0 or (pollfd.revents & std.os.POLL.IN) == 0) {
                continue;
            }
            const fanotify_fd = pollfd.fd;
            const read_len = try std.os.read(fanotify_fd, &buf);

            var ptr = &buf[0];
            var left_bytes = read_len;
            var event_count: usize = 0;
            while (true) {
                const meta_ptr = @ptrCast(*align(1) const event_metadata, ptr);
                const meta = meta_ptr.*;
                if (
                    left_bytes < @sizeOf(event_metadata) 
                    or left_bytes < @intCast(usize, meta.event_len)
                    // FIXME: is this case even possible?
                    or meta.event_len < @sizeOf(event_metadata)
                ) {
                    break;
                }

                if (meta.vers != METADATA_VERSION) {
                    @panic("unexpected " ++ @typeName(event_metadata) ++ " version");
                }

                if ((meta.mask & FAN.Q_OVERFLOW) > 0) @panic("queue overflowed");

                if (try parseRawEvent(a7r, timestamp, meta_ptr)) |event|
                    try events.append(event);

                ptr = @intToPtr(*u8, @ptrToInt(ptr) + meta.event_len);
                left_bytes -= meta.event_len;
                event_count += 1;
            }
            std.log.info("{} events read this cycle:", .{ event_count, });
            for (events.items) |event, ii| {
                std.log.info(
                    "{}: {}",
                    .{ ii, event }
                );
            }
            events.clearAndFree();
        }
    }
}

pub fn listener(
    ha7r: Allocator,
    fs_path: [:0]const u8,
    die_signal: *const bool,
    event_sink: mod_utils.Appender(FanotifyEvent),
) !void {
    const fd = blk: {
        const fd = try init(
            FAN.CLASS.NOTIF,
            FAN.INIT {
                .report_fid = true,
                .report_dir_fid = true,
                .report_name = true,
            },
            std.os.O.RDONLY | std.os.O.CLOEXEC,
        );
        try mark(
            fd, 
            FAN.MARK.MOD.ADD, 
            FAN.MARK.FILESYSTEM, 
            FAN.EVENT {
                .create = true,
                .modify = true,
                .attrib = true,
                .delete = true,
                .moved_to = true,
                .ondir = true,
            },
            std.os.AT.FDCWD, 
            fs_path
        );
        break :blk fd;
    };
    defer std.os.close(fd);

    var pollfds = [_]std.os.pollfd{std.os.pollfd{
        .fd = fd,
        .events = std.os.POLL.IN,
        .revents = 0,
    }};
    std.log.info("entering fanotify poll loop", .{});
    var buf = [_]u8{0} ** 256;

    while (true) {
        if (die_signal.*) break;
        const poll_num = try std.os.poll(&pollfds, -1);
        if (die_signal.*) break;

        if (poll_num < 1) {
            continue;
            // std.log.err("err on poll: {}", .{ std.os.errno(poll_num) });
            // @panic("err on poll");
        }

        const timestamp = std.time.timestamp();

        for (&pollfds) |pollfd| {
            if (pollfd.revents == 0 or (pollfd.revents & std.os.POLL.IN) == 0) {
                continue;
            }
            const fanotify_fd = pollfd.fd;
            const read_len = try std.os.read(fanotify_fd, &buf);

            var ptr = &buf[0];
            var left_bytes = read_len;
            var event_count: usize = 0;
            while (true) {
                const meta_ptr = @ptrCast(*align(1) const event_metadata, ptr);
                const meta = meta_ptr.*;
                if (
                    left_bytes < @sizeOf(event_metadata) 
                    or left_bytes < @intCast(usize, meta.event_len)
                    // FIXME: is this case even possible?
                    or meta.event_len < @sizeOf(event_metadata)
                ) {
                    break;
                }

                if (meta.vers != METADATA_VERSION) {
                    @panic("unexpected " ++ @typeName(event_metadata) ++ " version");
                }

                if ((meta.mask & FAN.Q_OVERFLOW) > 0) @panic("queue overflowed");

                if (try parseRawEvent(ha7r, timestamp, meta_ptr)) |event|
                    try event_sink.append(event);

                ptr = @intToPtr(*u8, @ptrToInt(ptr) + meta.event_len);
                left_bytes -= meta.event_len;
                event_count += 1;
            }
            // std.log.info("{} events read this cycle:", .{ event_count, });
            // for (events.items) |event, ii| {
            //     std.log.info(
            //         "Event: {} at {} with mask {} from pid {}",
            //         .{ ii, event.timestamp, event.mask, event.pid }
            //     );
            //     std.log.info("  - {s}/{s}", .{ event.dir, event.name });
            // }
        }
    }
}

inline fn parseRawEvent(
    a7r: Allocator, 
    timestamp: i64, 
    meta_ptr: *align(1) const event_metadata
) !?FanotifyEvent {
    const meta = meta_ptr.*;
    // if event is this short
    if (meta.event_len == @as(c_uint, meta.metadata_len)) {
        // it's not reporting file handles
        // and we're not interested
        @panic("todo");
    } else {
        const fid_info = @intToPtr(*event_info_fid, @ptrToInt(meta_ptr) + @sizeOf(event_metadata));
        // lifted from the manual fanotify(7)
        // Note that for the directory entry modification events FAN_CREATE, FAN_DELETE, and FAN_MOVE, 
        // the file_handle identifies the modified directory and not the created/deleted/moved child 
        // object. If the value of  info_type  field is FAN_EVENT_INFO_TYPE_DFID_NAME, the file handle 
        // is followed by a null terminated string that identifies the created/deleted/moved  
        // directory entry name.  

        // For other events such as FAN_OPEN, FAN_ATTRIB, FAN_DELETE_SELF,  and 
        // FAN_MOVE_SELF, if the value of info_type field is FAN_EVENT_INFO_TYPE_FID,
        // the file_handle identifies the object correlated to the event.   
        // If the value of info_type field is FAN_EVENT_INFO_TYPE_DFID, the file_handle 
        // identifies the directory object correlated to the event or the parent 
        // directory of a non-directory object correlated to the event.  
        // If the value of info_type field is FAN_EVENT_INFO_TYPE_DFID_NAME, the 
        // file_handle identifies the same directory object that would be reported 
        // with FAN_EVENT_INFO_TYPE_DFID and the file handle is followed by a null
        // terminated string that identifies the name of a directory entry in that directory, 
        // or '.' to identify the directory object itself.
        const handle = &fid_info.handle;

        // if just fid
        if (fid_info.hdr.info_type == FAN.EVENT.INFO_TYPE.FID) {
            // read the name and dir from the handle and assume that it belongs 
            // to the file of interest. Simple.
            if (open_by_handle_at(
                std.os.AT.FDCWD,
                handle,
                std.os.O.PATH,
            )) |dir_fd| {
                defer std.os.close(dir_fd);

                // read the path into a stack buffer
                const path = try mod_utils.fdPath(dir_fd);
                const name = std.fs.path.basename(path);
                const dir = std.fs.path.dirname(path) orelse "/"[0..];

                return try FanotifyEvent.init(
                    a7r, 
                    OptionStr{ .some = name }, 
                    OptionStr{ .some = dir }, 
                    &meta,
                    timestamp
                );
            } else |err| switch (err) {
                OpenByHandleErr.StaleFileHandle =>
                // // FIXME: move filtering to a higher abstraction
                // // we explicitly filter out the numerous ATTRIB events with no
                // // `name` or `dir` attached
                // return if (meta.mask == @as(c_ulonglong, FAN.EVENT.ATTRIB))
                //     null
                // else
                    return try FanotifyEvent.init(a7r, OptionStr.None, OptionStr.None, &meta, timestamp),
                else => {
                    std.log.warn("open_by_handle_at failed with {} at {}", .{ err, meta });
                    @panic("open_by_handle_at failed");
                    // break :eb try FanotifyEvent.init(
                    //     alloc8or,
                    //     OptionStr.None,
                    //     OptionStr.None,
                    //     &meta,
                    //     timestamp
                    // );
                },
            }
        } else if (fid_info.hdr.info_type == FAN.EVENT.INFO_TYPE.DFID_NAME) {
            const name = blk: {
                const name_ptr = handle.file_name();
                break :blk name_ptr[0..std.mem.len(name_ptr)];
            };

            // var path_buf = [_]u8{0} ** std.fs.MAX_PATH_BYTES;
            const opt_path = if (open_by_handle_at(
                std.os.AT.FDCWD,
                handle,
                std.os.O.PATH,
            )) |dir_fd| blk: {
                defer std.os.close(dir_fd);
                break :blk try mod_utils.fdPath(dir_fd);
            } else |err| switch (err) {
                OpenByHandleErr.StaleFileHandle => null,
                else => @panic("open_by_handle_at failed"),
            };

            var opt_name = OptionStr.None;
            var opt_dir = OptionStr.None;
            // if it's the dir itself that was "evented" on
            if (std.mem.eql(u8, name, ".")) {
                if (opt_path) |path| {
                    opt_name = OptionStr{ .some = std.fs.path.basename(path) };
                    opt_dir = OptionStr{ .some = std.fs.path.dirname(path) orelse "/"[0..] };
                } else {
                    // if we don't have a path or a name
                    println("this fucking happened", .{});
                }
            } else {
                opt_name = OptionStr{ .some = name };
                if (opt_path) |dir| {
                    opt_dir = OptionStr{ .some = dir };
                }
            }
            return try FanotifyEvent.init(
                a7r, opt_name, opt_dir, &meta, timestamp
            );
        } 
        else {
            @panic("unexpected info type");
        }
    }
}

fn isElevated() bool {
    return std.os.linux.geteuid() == 0;
}

fn expectEvent(
    expected_kind: FAN.EVENT, 
    expected_name: ?[]const u8, 
    expected_dir: ?[]const u8, 
    events: []const FanotifyEvent
) !void {
    for (events) |event| {
        if ((expected_kind.asInt() & event.kind.asInt()) > 0) {
            if (expected_name) |name| {
                std.testing.expectEqualSlices(
                    u8, 
                    name, 
                    event.name orelse {
                        // println(
                        //     "found event but name was null: {s} != null", 
                        //     .{ name }
                        // );
                        continue;
                    }
                ) catch {
                    // println(
                    //     "found event but name slices weren't equal: {s} != {s}", 
                    //     .{ name, event.name orelse unreachable }
                    // );
                    continue;
                };
            }
            if (expected_dir) |dir| {
                std.testing.expectEqualSlices(
                    u8, 
                    dir, 
                    event.dir orelse {
                        // println(
                        //     "found event but dir was null: {s} != null", 
                        //     .{ dir }
                        // );
                        continue;
                    }
                ) catch {
                    // println(
                    //     "found event but dir slices weren't equal {s} != {s}", 
                    //     .{ dir, event.dir orelse unreachable }
                    // );
                    continue;
                };
            }
            return;
        }
    }
    // @panic("unable to find expected event");
    return error.EventNotFound;
}

test "fanotify_create_file" {
    if (!isElevated()) return error.SkipZigTest;
    const file_name = "wheredidyouparkthecar";
    const actions = struct {
        fn prePoll(a7r: Allocator, dir: std.fs.Dir) !void {
            _ = a7r;
            _ = dir;
        }
        fn touch(a7r: Allocator, dir: std.fs.Dir) !void {
            _ = a7r;
            var file = try dir.createFile(file_name, .{});
            defer file.close();
        }
    };
    var a7r = std.testing.allocator;
    var res = try TestFanotify.run(a7r, actions.prePoll, actions.touch);
    defer res.deinit(a7r);

    expectEvent(
        FAN.EVENT { .create = true }, 
        file_name, 
        null,
        res.events.items
    ) catch |err| {
        println("{any}", .{ res.events.items });
        return err;
    };
}

test "fanotify_create_file_nested" {
    // FIXME: nested create is not being detected and I reckon it's the same for
    // the other events. My suspicion is that this's a `tmpfs` quirk as I have
    // tested the machinery implemented herewithin manually on an `ext4` file 
    // and "nested" works without a hitch.
    if (true) return error.SkipZigTest;
    if (!isElevated()) return error.SkipZigTest;
    const file_name = "wheredidyouparkthecar";
    const file_dir = "weirdcities";
    const actions = struct {
        fn prePoll(a7r: Allocator, dir: std.fs.Dir) !void {
            _ = a7r;
            _ = dir;
            try dir.makeDir(file_dir);
        }
        fn touch(a7r: Allocator, dir: std.fs.Dir) !void {
            _ = a7r;
            var parent = try dir.openDir(file_dir, .{});
            defer parent.close();
            var file = try parent.createFile(file_name, .{});
            defer file.close();
            // println(file.p)
        }
    };
    var a7r = std.testing.allocator;
    var res = try TestFanotify.run(a7r, actions.prePoll, actions.touch);
    defer res.deinit(a7r);

    expectEvent(
        FAN.EVENT { .create = true }, 
        file_name, 
        file_dir,
        res.events.items
    ) catch |err| {
        println("{any}", .{ res.events.items });
        return err;
    };
}

test "fanotify_create_dir" {
    if (!isElevated()) return error.SkipZigTest;
    const dir_name = "wheredidyouparkthecar";
    const actions = struct {
        fn prePoll(a7r: Allocator, dir: std.fs.Dir) !void {
            _ = a7r;
            _ = dir;
        }
        fn touch(a7r: Allocator, dir: std.fs.Dir) !void {
            _ = a7r;
            try dir.makeDir(dir_name);
        }
    };
    var a7r = std.testing.allocator;
    var res = try TestFanotify.run(a7r, actions.prePoll, actions.touch);
    defer res.deinit(a7r);

    expectEvent(
        FAN.EVENT { .create = true, .ondir = true },
        dir_name, 
        null,
        res.events.items
    ) catch |err| {
        println("{any}", .{ res.events.items });
        return err;
    };
}

test "fanotify_delete_file" {
    if (!isElevated()) return error.SkipZigTest;
    const file_name = "wheredidyouparkthecar";
    const actions = struct {
        fn prePoll(a7r: Allocator, dir: std.fs.Dir) !void {
            _ = a7r;
            var file = try dir.createFile(file_name, .{});
            defer file.close();
        }
        fn touch(a7r: Allocator, dir: std.fs.Dir) !void {
            _ = a7r;
            try dir.deleteFile(file_name);
        }
    };
    var a7r = std.testing.allocator;
    var res = try TestFanotify.run(a7r, actions.prePoll, actions.touch);
    defer res.deinit(a7r);

    expectEvent(
        FAN.EVENT { .delete = true, },
        file_name, 
        null,
        res.events.items
    ) catch |err| {
        println("{any}", .{ res.events.items });
        return err;
    };
}

test "fanotify_delete_dir" {
    if (!isElevated()) return error.SkipZigTest;
    const dir_name = "wheredidyouparkthecar";
    const actions = struct {
        fn prePoll(a7r: Allocator, dir: std.fs.Dir) !void {
            _ = a7r;
            try dir.makeDir(dir_name);
        }
        fn touch(a7r: Allocator, dir: std.fs.Dir) !void {
            _ = a7r;
            try dir.deleteDir(dir_name);
        }
    };
    var a7r = std.testing.allocator;
    var res = try TestFanotify.run(a7r, actions.prePoll, actions.touch);
    defer res.deinit(a7r);

    expectEvent(
        FAN.EVENT { .delete = true, .ondir = true },
        dir_name, 
        null,
        res.events.items
    ) catch |err| {
        println("{any}", .{ res.events.items });
        return err;
    };
}

test "fanotify_move_file" {
    if (!isElevated()) return error.SkipZigTest;
    const file_name = "wheredidyouparkthecar";
    const new_name = "pensuspendsuspend";
    const actions = struct {
        fn prePoll(a7r: Allocator, dir: std.fs.Dir) !void {
            _ = a7r;
            var file = try dir.createFile(file_name, .{});
            defer file.close();
        }
        fn touch(a7r: Allocator, dir: std.fs.Dir) !void {
            _ = a7r;
            try dir.rename(file_name, new_name);
        }
    };
    var a7r = std.testing.allocator;
    var res = try TestFanotify.run(a7r, actions.prePoll, actions.touch);
    defer res.deinit(a7r);

    expectEvent(
        FAN.EVENT { .moved_to = true, },
        new_name, 
        null,
        res.events.items
    ) catch |err| {
        println("{any}", .{ res.events.items });
        return err;
    };
}

test "fanotify_move_dir" {
    if (!isElevated()) return error.SkipZigTest;
    const dir_name = "wheredidyouparkthecar";
    const new_name = "pensuspendsuspend";
    const actions = struct {
        fn prePoll(a7r: Allocator, dir: std.fs.Dir) !void {
            _ = a7r;
            try dir.makeDir(dir_name);
        }
        fn touch(a7r: Allocator, dir: std.fs.Dir) !void {
            _ = a7r;
            try dir.rename(dir_name, new_name);
        }
    };
    var a7r = std.testing.allocator;
    var res = try TestFanotify.run(a7r, actions.prePoll, actions.touch);
    defer res.deinit(a7r);

    expectEvent(
        FAN.EVENT { .moved_to = true, .ondir = true },
        new_name, 
        null,
        res.events.items
    ) catch |err| {
        println("{any}", .{ res.events.items });
        return err;
    };
}

test "fanotify_mod_file" {
    if (!isElevated()) return error.SkipZigTest;
    const file_name = "wheredidyouparkthecar";
    const actions = struct {
        fn prePoll(a7r: Allocator, dir: std.fs.Dir) !void {
            _ = a7r;
            var file = try dir.createFile(file_name, .{});
            defer file.close();
        }
        fn touch(a7r: Allocator, dir: std.fs.Dir) !void {
            _ = a7r;
            try dir.writeFile(file_name, "stuff");
        }
    };
    var a7r = std.testing.allocator;
    var res = try TestFanotify.run(a7r, actions.prePoll, actions.touch);
    defer res.deinit(a7r);

    expectEvent(
        FAN.EVENT { .modify = true, },
        file_name, 
        null,
        res.events.items
    ) catch |err| {
        println("{any}", .{ res.events.items });
        return err;
    };
}

test "fanotify_attrib_file" {
    if (!isElevated()) return error.SkipZigTest;
    const file_name = "wheredidyouparkthecar";
    const actions = struct {
        fn prePoll(a7r: Allocator, dir: std.fs.Dir) !void {
            _ = a7r;
            var file = try dir.createFile(file_name, .{});
            defer file.close();
        }
        fn touch(a7r: Allocator, dir: std.fs.Dir) !void {
            _ = a7r;
            var file = try dir.openFile(file_name, .{});
            defer file.close();
            try file.setPermissions(
                std.fs.File.Permissions{ .inner = std.fs.File.PermissionsUnix.unixNew(777) }
            );
        }
    };
    var a7r = std.testing.allocator;
    var res = try TestFanotify.run(a7r, actions.prePoll, actions.touch);
    defer res.deinit(a7r);

    expectEvent(
        FAN.EVENT { .attrib = true },
        file_name, 
        null,
        res.events.items
    ) catch |err| {
        println("{any}", .{ res.events.items });
        return err;
    };
}

test "fanotify_attrib_dir" {
    // FIXME: attrib changes on dir an unrealiable
    if (true) return error.SkipZigTest;
    if (!isElevated()) return error.SkipZigTest;
    const dir_name = "wheredidyouparkthecar";
    const actions = struct {
        fn prePoll(a7r: Allocator, dir: std.fs.Dir) !void {
            _ = a7r;
            try dir.makeDir(dir_name);
        }
        fn touch(a7r: Allocator, dir: std.fs.Dir) !void {
            _ = a7r;
            var subdir = try dir.openIterableDir(dir_name, .{});
            defer subdir.close();
            try subdir.dir.setPermissions(
                std.fs.File.Permissions{ .inner = std.fs.File.PermissionsUnix.unixNew(777) }
            );
        }
    };
    var a7r = std.testing.allocator;
    var res = try TestFanotify.run(a7r, actions.prePoll, actions.touch);
    defer res.deinit(a7r);

    expectEvent(
        FAN.EVENT { .attrib = true, .ondir = true },
        dir_name, 
        null,
        res.events.items
    ) catch |err| {
        println("{any}", .{ res.events.items });
        return err;
    };
}
