const std = @import("std");
const builtin = @import("builtin");

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

    pub fn asInt(self: @This()) u32 {
        return @as(u32, @bitCast(self));
    }

    pub fn format(self: @This(), comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = options;
        try std.fmt.format(writer, @typeName(@This()) ++ " {{", .{});
        // try std.fmt.format(writer, " {}: ", .{ self.asInt() });
        comptime var tinfo: std.builtin.Type.Struct = @typeInfo(@This()).Struct;
        inline for (tinfo.fields) |field| {
            if (field.type == bool)
                if (@field(self, field.name))
                    try std.fmt.format(writer, " {s},", .{field.name});
        }
        try std.fmt.format(writer, " }}", .{});
    }

    comptime {
        const Self = @This();
        if (@sizeOf(Self) != @sizeOf(u32)) {
            @compileError(std.fmt.comptimePrint("unexpected size mismatch: {} !=  {}\n", .{ @sizeOf(Self), @sizeOf(u32) }));
        }
        if (@bitSizeOf(Self) != @bitSizeOf(u32)) {
            @compileError(std.fmt.comptimePrint("unexpected bitSize mismatch: {} !=  {}\n", .{ @bitSizeOf(Self), @bitSizeOf(u32) }));
        }
        var table = .{
            .{ Self{ .access = true }, ACCESS },
            .{ Self{ .modify = true }, MODIFY },
            .{ Self{ .attrib = true }, ATTRIB },
            .{ Self{ .close_write = true }, CLOSE_WRITE },
            .{ Self{ .close_nowrite = true }, CLOSE_NOWRITE },
            .{ Self{ .open = true }, OPEN },
            .{ Self{ .moved_from = true }, MOVED_FROM },
            .{ Self{ .moved_to = true }, MOVED_TO },
            .{ Self{ .moved_from = true, .moved_to = true }, MOVE },
            .{ Self{ .create = true }, CREATE },
            .{ Self{ .delete = true }, DELETE },
            .{ Self{ .delete_self = true }, DELETE_SELF },
            .{ Self{ .move_self = true }, MOVE_SELF },
            .{ Self{ .open_exec = true }, OPEN_EXEC },
            .{ Self{ .open_perm = true }, OPEN_PERM },
            .{ Self{ .access_perm = true }, ACCESS_PERM },
            .{ Self{ .open_exec_perm = true }, OPEN_EXEC_PERM },
            .{ Self{ .event_on_child = true }, EVENT_ON_CHILD },
            .{ Self{ .rename = true }, RENAME },
            .{ Self{ .ondir = true }, ONDIR },
        };
        for (table) |case| {
            if (case[0].asInt() != @as(u32, case[1])) {
                @compileError(std.fmt.comptimePrint("unexpected bit set: {}({}) !=  {}\n", .{ case[0], case[0].asInt(), case[1] }));
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

    pub fn asInt(self: @This()) u13 {
        return @as(u13, @bitCast(self));
    }

    comptime {
        const Self = @This();
        if (@sizeOf(Self) != @sizeOf(u13)) {
            @compileError(std.fmt.comptimePrint("unexpected size mismatch: {} !=  {}\n", .{ @sizeOf(Self), @sizeOf(u13) }));
        }
        if (@bitSizeOf(Self) != @bitSizeOf(u13)) {
            @compileError(std.fmt.comptimePrint("unexpected bitSize mismatch: {} !=  {}\n", .{ @bitSizeOf(Self), @bitSizeOf(u13) }));
        }
        var table = .{
            .{ Self{ .cloexec = true }, CLOEXEC },
            .{ Self{ .nonblock = true }, NONBLOCK },
            .{ Self{ .unlimited_queue = true }, UNLIMITED_QUEUE },
            .{ Self{ .unlimited_marks = true }, UNLIMITED_MARKS },
            .{ Self{ .enable_audit = true }, ENABLE_AUDIT },
            .{ Self{ .report_pidfd = true }, REPORT_PIDFD },
            .{ Self{ .report_tid = true }, REPORT_TID },
            .{ Self{ .report_fid = true }, REPORT_FID },
            .{ Self{ .report_dir_fid = true }, REPORT_DIR_FID },
            .{ Self{ .report_name = true }, REPORT_NAME },
            .{ Self{ .report_target_fid = true }, REPORT_TARGET_FID },
            .{ Self{ .report_dir_fid = true, .report_name = true }, REPORT_DFID_NAME },
            .{ Self{ .report_dir_fid = true, .report_name = true, .report_fid = true, .report_target_fid = true }, REPORT_DFID_NAME_TARGET },
        };
        for (table) |case| {
            if (case[0].asInt() != @as(u13, case[1])) {
                @compileError(std.fmt.comptimePrint("unexpected bit set: {}({}) !=  {}\n", .{ case[0], case[0].asInt(), case[1] }));
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
