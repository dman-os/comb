const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const mod_utils = @import("utils.zig");
const println = mod_utils.println;
const dbg = mod_utils.dbg;
const isElevated = mod_utils.isElevated;
const Option = mod_utils.Option;
const OptionStr = Option([]const u8);
const Queue = mod_utils.Mpmc;

const Db = @import("Database.zig");
const Query = @import("Query.zig");

const mod_treewalking = @import("treewalking.zig");
const FsEntry = mod_treewalking.FsEntry;

const mod_mmap = @import("mmap.zig");

pub const FanotifyWorker = @import("fanotify/FanotifyWorker.zig");
pub const FAN = @import("fanotify/FAN.zig");

pub const FanotifyEvent = struct {
    const Self = @This();
    dir: ?[]u8,
    name: ?[]u8,
    old_dir: ?[]u8,
    old_name: ?[]u8,
    // mask: c_ulonglong align(8),
    kind: FAN.EVENT,
    pid: c_int,
    timestamp: i64,

    // avoid const optional slices: https://github.com/ziglang/zig/issues/4907
    pub fn init(
        a7r: std.mem.Allocator,
        name: ?[]const u8,
        dir: ?[]const u8,
        old_name: ?[]const u8,
        old_dir: ?[]const u8,
        meta: *const event_metadata,
        timestamp: i64,
    ) !Self {
        return Self{
            .name = if (name) |slice| try a7r.dupe(u8, slice) else null,
            .dir = if (dir) |slice| try a7r.dupe(u8, slice) else null,
            .old_name = if (old_name) |slice| try a7r.dupe(u8, slice) else null,
            .old_dir = if (old_dir) |slice| try a7r.dupe(u8, slice) else null,
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
        if (self.old_name) |slice| {
            a7r.free(slice);
        }
        if (self.old_dir) |slice| {
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
            @typeName(Self) ++ "{{ .name = {?s}, .dir = {?s}, .old_name = {?s}, .old_dir = {?s}, .kind = {}, .pid = {} .timestamp = {} }}", 
            // @typeName(Self) ++ "{{\n .name = {?s},\n .dir = {?s},\n .kind = {},\n .pid = {}\n .timestamp = {} }}", 
            .{ self.name, self.dir, self.old_name, self.old_dir, self.kind, self.pid, self.timestamp }
        );
    }
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

    pub fn file_name(self: *@This()) [*:0]const u8 {
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

/// FIXME: what's this? It doesn't seem to be in use.
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

pub fn demo() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    var ha7r = gpa.allocator();

    var die_signal = std.Thread.ResetEvent{};
    const Sink = struct {
        ha7r: Allocator,
        fn append(self: *@This(), event_in: FanotifyEvent) !void {
            var event = event_in;
            defer event.deinit(self.ha7r);
            println("recieved evt: {}", .{ event });
        }
    };
    var sink = Sink {
        .ha7r = ha7r
    };
    try listener(
        ha7r, 
        &die_signal, 
        mod_utils.Appender(FanotifyEvent).new(
            &sink,
            Sink.append
        ),
        .{ 
            .mark_event_mask = .{
                .create = true,
                .modify = true,
                // .attrib = true,
                .delete = true,
                .moved_to = true,
                // .moved_from = true,
                .ondir = true,
                .rename = true
            } 
        }
    );
}

pub const Config = struct {
    // the path at which to listen
    mark_fs_path: [:0]const u8 = "/",
    /// the events we're interested in
    mark_event_mask: FAN.EVENT = .{
        .create = true,
        .modify = true,
        .attrib = true,
        .delete = true,
        .moved_to = true,
        // .moved_from = true,
        .ondir = true,
        .rename = true
    },
    /// milliseconds for which `poll` on the `fanotify`
    /// handle will wait before timing out. Set to `-1` to wait forever.
    poll_timeout: i32 = -1,
};

pub fn listener(
    ha7r: Allocator,
    die_signal: *const std.Thread.ResetEvent,
    event_sink: mod_utils.Appender(FanotifyEvent),
    config: Config,
) !void {
    const fd = blk: {
        const fd = try init(
            FAN.CLASS.NOTIF,
            FAN.INIT {
                // .report_fid = true,
                .report_dir_fid = true,
                .report_name = true,
                // .report_target_fid = true,
            },
            std.os.O.RDONLY | std.os.O.CLOEXEC,
        );
        try mark(
            fd, 
            FAN.MARK.MOD.ADD, 
            // FIXME: consider monitoring specific mounts instead of the fs
            FAN.MARK.FILESYSTEM, 
            config.mark_event_mask,
            std.os.AT.FDCWD, 
            config.mark_fs_path
        );
        break :blk fd;
    };
    defer std.os.close(fd);
    var mount_fd = try std.os.openZ(
        config.mark_fs_path,
        // std.os.O.RDONLY | std.os.O.PATH | std.os.O.DIRECTORY,
        std.os.O.RDONLY | std.os.O.DIRECTORY,
        0
    ); 
    defer std.os.close(mount_fd);

    var pollfds = [_]std.os.pollfd{std.os.pollfd{
        .fd = fd,
        .events = std.os.POLL.IN,
        .revents = 0,
    }};
    std.log.info("entering fanotify poll loop", .{});
    var buf = [_]u8{0} ** 4096;

    while (true) {
        if (die_signal.isSet()) break;
        const poll_num = try std.os.poll(&pollfds, config.poll_timeout);
        if (die_signal.isSet()) break;

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

                if (try parseRawEvent(ha7r, timestamp, meta_ptr, mount_fd)) |event|
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

fn parseRawEvent(
    ha7r: Allocator, 
    timestamp: i64, 
    meta_ptr: *align(1) const event_metadata,
    mount_fd: std.os.fd_t,
) !?FanotifyEvent {
    const meta = meta_ptr.*;
    // if event is this short
    if (meta.event_len == @as(c_uint, meta.metadata_len)) {
        // it's not reporting file handles
        // and we're not interested
        @panic("todo");
    } else {
        // lifted from the manual fanotify(7)
        //
        // When an fanotify file  de‐ scriptor is created using FAN_REPORT_FID,
        // a single information record is expected to be attached to the event
        // with info_type field value of FAN_EVENT_INFO_TYPE_FID.  When an
        // fanotify  file de‐ scriptor is created using the combination of
        // FAN_REPORT_FID and FAN_REPORT_DIR_FID, there may be two  information
        // records attached to  the  event:  one  with   info_type   field
        // value of FAN_EVENT_INFO_TYPE_DFID,  identifying  a  parent directory
        // object, and one with info_type field value of
        // FAN_EVENT_INFO_TYPE_FID, identifying a child object.  Note that for
        // the directory entry modification  events  FAN_CREATE, FAN_DELETE,
        // FAN_MOVE,  and FAN_RENAME, an information record identifying the
        // created/deleted/moved child object is reported only if  an  fanotify
        // group  was initialized with the flag FAN_REPORT_TARGET_FID.

        const kind = @bitCast(FAN.EVENT, @truncate(u32, meta.mask));

        var arena = std.heap.ArenaAllocator.init(ha7r);
        defer arena.deinit();
        var aha7r = arena.allocator();
        var opt_name: ?[]u8 = null;
        var opt_dir: ?[]u8 = null;
        var opt_old_name: ?[]u8 = null;
        var opt_old_dir: ?[]u8 = null;

        var start_addr = @ptrToInt(meta_ptr) + @sizeOf(event_metadata);
        while (
            start_addr < @ptrToInt(meta_ptr) + meta.event_len
        ) {
            const info_hdr = @intToPtr(
                *event_info_header, 
                start_addr
            );
            defer start_addr = start_addr + info_hdr.len;
            // lifted from the manual fanotify(7)
            // Note that for the directory entry modification events FAN_CREATE,
            // FAN_DELETE, and FAN_MOVE, the file_handle identifies the modified
            // directory and not the created/deleted/moved child object. If the
            // value of  info_type  field is FAN_EVENT_INFO_TYPE_DFID_NAME, the
            // file handle is followed by a null terminated string that identifies
            // the created/deleted/moved  directory entry name.  

            // For other events such as FAN_OPEN, FAN_ATTRIB, FAN_DELETE_SELF,  and
            // FAN_MOVE_SELF, if the value of info_type field is
            // FAN_EVENT_INFO_TYPE_FID, the file_handle identifies the object
            // correlated to the event.   If the value of info_type field is
            // FAN_EVENT_INFO_TYPE_DFID, the file_handle identifies the directory
            // object correlated to the event or the parent directory of a
            // non-directory object correlated to the event.  If the value of
            // info_type field is FAN_EVENT_INFO_TYPE_DFID_NAME, the file_handle
            // identifies the same directory object that would be reported with
            // FAN_EVENT_INFO_TYPE_DFID and the file handle is followed by a null
            // terminated string that identifies the name of a directory entry in
            // that directory, or '.' to identify the directory object itself.

            // if just fid
            if (info_hdr.info_type == FAN.EVENT.INFO_TYPE.FID) {
                const fid_info = @intToPtr(
                    *event_info_fid, 
                    start_addr
                );
                const handle = &fid_info.handle;
                // defer println(
                //     "raw event FID: {} dir={?s} name={?s}", 
                //     .{ .{ meta_ptr, fid_info, }, opt_dir.some, opt_name.some }
                // );
                // read the name and dir from the handle and assume that it belongs 
                // to the file of interest. Simple.
                if (open_by_handle_at(
                    mount_fd,
                    handle,
                    std.os.O.PATH | std.os.O.NOFOLLOW,
                )) |dir_fd| {
                    defer std.os.close(dir_fd);

                    // read the path into a stack buffer
                    const path = try mod_utils.fdPath(dir_fd);
                    // std.debug.assert(opt_name == null);
                    // std.debug.assert(opt_dir == null);
                    if (opt_name != null or opt_dir != null) {
                        println(
                            "opt_name={?s} opt_dir={?s} path={?s} kind={}", 
                            .{ opt_name, opt_dir, path, kind }
                        );
                    }
                    opt_name = try aha7r.dupe(u8, std.fs.path.basename(path));
                    opt_dir = try aha7r.dupe(u8, std.fs.path.dirname(path) orelse "/"[0..]);
                } else |err| switch (err) {
                    OpenByHandleErr.StaleFileHandle => {
                        std.log.debug("err: {}", .{ err });
                    },
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
            } else if (
                info_hdr.info_type == FAN.EVENT.INFO_TYPE.DFID_NAME
                or 
                info_hdr.info_type == FAN.EVENT.INFO_TYPE.NEW_DFID_NAME
                or 
                info_hdr.info_type == FAN.EVENT.INFO_TYPE.OLD_DFID_NAME
            ) {
                const fid_info = @intToPtr(
                    *event_info_fid, 
                    start_addr
                );
                // defer println(
                //     "raw event DFID: {} dir={?s} name={?s}", 
                //     .{ .{ meta_ptr, fid_info, }, opt_dir.some, opt_name.some }
                // );

                const handle = &fid_info.handle;

                const handle_name = blk: {
                    const name_ptr = handle.file_name();
                    break :blk name_ptr[0..std.mem.len(name_ptr)];
                };

                // var path_buf = [_]u8{0} ** std.fs.MAX_PATH_BYTES;

                const opt_path = if (open_by_handle_at(
                    mount_fd,
                    handle,
                    std.os.O.PATH | std.os.O.NOFOLLOW,
                )) |dir_fd| blk: {
                    defer std.os.close(dir_fd);
                    break :blk try mod_utils.fdPath(dir_fd);
                } else |err| blk: {
                    switch (err) {
                        OpenByHandleErr.StaleFileHandle => {
                            std.log.debug("err: {}", .{ err });
                        },
                        else => {
                            var stuff = try std.os.fcntl(mount_fd, std.os.F.GETFD, 0);
                            println("stuff={} err={}", .{ stuff, err });
                            @panic("open_by_handle_at failed");
                        },
                    }
                    break :blk null;
                };

                var name: ?[]const u8 = null;
                var dir: ?[]const u8 = null;
                // if it's the dir itself that was "evented" on
                if (std.mem.eql(u8, handle_name, ".")) {
                    if (opt_path) |path| {
                        name = std.fs.path.basename(path);
                        dir = std.fs.path.dirname(path) orelse "/"[0..];
                    } else {
                        // if we don't have a path or a name - probably an attrib event
                        std.log.warn("this fucking happened", .{});
                    }
                } else {
                    name = handle_name;
                    if (opt_path) |path| {
                        dir = path;
                    }
                } 
                if (
                    info_hdr.info_type == FAN.EVENT.INFO_TYPE.OLD_DFID_NAME
                ) {
                    std.debug.assert(opt_old_name == null);
                    std.debug.assert(opt_old_dir == null);
                    if (name) |slice| {
                        opt_old_name = try aha7r.dupe(u8, slice);
                    }
                    if (dir) |slice| {
                        opt_old_dir = try aha7r.dupe(u8, slice);
                    }
                } 
                // info_hdr.info_type == FAN.EVENT.INFO_TYPE.DFID_NAME
                // or info_hdr.info_type == FAN.EVENT.INFO_TYPE.NEW_DFID_NAME
                else {
                    std.debug.assert(opt_name == null);
                    std.debug.assert(opt_dir == null);
                    if (name) |slice| {
                        opt_name = try aha7r.dupe(u8, slice);
                    }
                    if (dir) |slice| {
                        opt_dir = try aha7r.dupe(u8, slice);
                    }
                }
            } else {
                println("raw event UNEXPECTED: {}", .{ .{ meta_ptr, info_hdr } });
                @panic("unexpected info type");
            }
        }
        const event = try FanotifyEvent.init(
            ha7r, opt_name, opt_dir, opt_old_name, opt_old_dir, &meta, timestamp
        );
        return event;
    }
}

const TestFanotify = struct {
    const Self = @This();
    pub const TouchFn = *const fn (a7r: Allocator, dir: std.fs.Dir) anyerror!void;

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
            try @call(.auto, ctx.prePollTouch, .{ alloc8or, ctx.dir });
            // make sure they're ready to poll before you start doing shit
            ctx.listenReady.wait();
            // give them the heads up to start polling
            ctx.startListen.set();
            try @call(.auto, ctx.touch, .{ alloc8or, ctx.dir });
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
                        // .report_fid = true,
                        .report_dir_fid = true,
                        .report_name = true,
                        // .report_target_fid = true,
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
                        // .moved_from = true,
                        .ondir = true,
                        .rename = true,
                    },
                    std.os.AT.FDCWD, 
                    ctx.path
                );
                break :blk fd;
            };
            defer std.os.close(fd);

            var mount_fd = try std.os.openZ(
                ctx.path,
                // std.os.O.RDONLY | std.os.O.PATH | std.os.O.DIRECTORY,
                std.os.O.RDONLY | std.os.O.DIRECTORY,
                0
            ); 
            defer std.os.close(mount_fd);

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
            while (!breakOnNext) {
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

                        if (
                            try parseRawEvent(alloc8or, timestamp, meta_ptr, mount_fd)
                        ) |event|
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
        errdefer tmp_dir.cleanup();

        var tmpfs_path = blk: {
            const tmpfs_name = "tmpfs";
            try tmp_dir.dir.makeDir(tmpfs_name);
            var path = try mod_utils.fdPath(tmp_dir.dir.fd);
            // var path = try tmp_dir.dir.realpath(a7r, tmpfs_name);
            // defer a7r.free(path);
            break :blk try std.fs.path.joinZ(a7r, &.{path, tmpfs_name});
        };
        errdefer a7r.free(tmpfs_path);

        try mount("tmpfs", tmpfs_path, "tmpfs", 0, 0);
        errdefer {
            const resp = std.os.linux.umount(tmpfs_path);
            switch (std.os.errno(resp)) {
                .SUCCESS => {},
                else => |err| {
                    println("err: {}", .{err});
                    @panic("umount failed");
                },
            }
        }

        var tmpfs_dir = try tmp_dir.dir.openDir("tmpfs", .{});
        errdefer tmpfs_dir.close();

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

    fn expectEvent(
        self: *const Self,
        expected_kind: FAN.EVENT, 
        expected_name: ?[]const u8, 
        expected_dir: ?[]const u8, 
    ) !FanotifyEvent {
        for (self.events.items) |event| {
            var common = expected_kind.asInt() & event.kind.asInt();
            if (
                common > 0 
                // if ondir is all they have in common, not interested
                and common != (FAN.EVENT { .ondir = true }).asInt()
            ) {
                if (expected_name) |name| {
                    if (!std.mem.eql(
                        u8, 
                        name, 
                        event.name orelse {
                            // println(
                            //     "found event but name was null: {s} != null", 
                            //     .{ name }
                            // );
                            continue;
                        }
                    )) {
                        // println(
                        //     "found event but name slices weren't equal: {s} != {s}", 
                        //     .{ name, event.name orelse unreachable }
                        // );
                        continue;
                    }
                }
                if (expected_dir) |dir| {
                    const event_dir = std.fs.path.basename(
                        event.dir orelse {
                            // println(
                            //     "found event but dir was null: {s} != null", 
                            //     .{ dir }
                            // );
                            continue;
                        }
                    );
                    if (!std.mem.eql(
                        u8, 
                        dir, 
                        event_dir
                    )) {
                        // println(
                        //     "found event but dir slices weren't equal {s} != {s}", 
                        //     .{ dir, event.dir orelse unreachable }
                        // );
                        continue;
                    }
                }
                return event;
            }
        }
        // @panic("unable to find expected event");
        return error.EventNotFound;
    }
};


test "fanotify_create_file" {
    if (builtin.single_threaded) return error.SkipZigTest;
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

    _ = res.expectEvent(
        FAN.EVENT { .create = true }, 
        file_name, 
        null,
    ) catch |err| {
        println("{any}", .{ res.events.items });
        return err;
    };
}

test "fanotify_create_file_nested" {
    if (builtin.single_threaded) return error.SkipZigTest;
    if (!isElevated()) return error.SkipZigTest;
    const file_name = "wheredidyouparkthecar";
    const file_dir = "weirdcities";
    const actions = struct {
        fn prePoll(a7r: Allocator, dir: std.fs.Dir) !void {
            _ = a7r;
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

    _ = res.expectEvent(
        FAN.EVENT { .create = true }, 
        file_name, 
        file_dir,
    ) catch |err| {
        println("{any}", .{ res.events.items });
        return err;
    };
}

test "fanotify_create_dir" {
    if (builtin.single_threaded) return error.SkipZigTest;
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

    _ = res.expectEvent(
        FAN.EVENT { .create = true, .ondir = true },
        dir_name, 
        null,
    ) catch |err| {
        println("{any}", .{ res.events.items });
        return err;
    };
}

test "fanotify_create_dir_nested" {
    if (true) return error.SkipZigTest;
}

test "fanotify_delete_file" {
    if (builtin.single_threaded) return error.SkipZigTest;
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

    _ = res.expectEvent(
        FAN.EVENT { .delete = true, },
        file_name, 
        null,
    ) catch |err| {
        println("{any}", .{ res.events.items });
        return err;
    };
}

test "fanotify_create_dir_nested" {
    if (true) return error.SkipZigTest;
}

test "fanotify_delete_dir" {
    if (builtin.single_threaded) return error.SkipZigTest;
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

    _ = res.expectEvent(
        FAN.EVENT { .delete = true, .ondir = true },
        dir_name, 
        null,
    ) catch |err| {
        println("{any}", .{ res.events.items });
        return err;
    };
}

test "fanotify_delete_dir_nested" {
    if (true) return error.SkipZigTest;
}

test "fanotify_move_file" {
    if (builtin.single_threaded) return error.SkipZigTest;
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

    const event = res.expectEvent(
        FAN.EVENT { .rename = true, },
        new_name, 
        null,
    ) catch |err| {
        println("{any}", .{ res.events.items });
        return err;
    };
    const old_name = event.old_name orelse unreachable;
    try std.testing.expectEqualSlices(u8, file_name, old_name);
    // res.expectEvent(
    //     FAN.EVENT { .moved_from = true, },
    //     file_name, 
    //     null,
    // ) catch |err| {
    //     println("{any}", .{ res.events.items });
    //     return err;
    // };
}

test "fanotify_move_file_nested" {
    if (true) return error.SkipZigTest;
}

test "fanotify_move_dir" {
    if (builtin.single_threaded) return error.SkipZigTest;
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

    const event = res.expectEvent(
        FAN.EVENT { .rename = true, .ondir = true },
        new_name, 
        null,
    ) catch |err| {
        println("{any}", .{ res.events.items });
        return err;
    };
    const old_name = event.old_name orelse unreachable;
    try std.testing.expectEqualSlices(u8, dir_name, old_name);
}

test "fanotify_move_dir_nested" {
    if (true) return error.SkipZigTest;
}

test "fanotify_mod_file" {
    if (builtin.single_threaded) return error.SkipZigTest;
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

    _ = res.expectEvent(
        FAN.EVENT { .modify = true, },
        file_name, 
        null,
    ) catch |err| {
        println("{any}", .{ res.events.items });
        return err;
    };
}

test "fanotify_mod_file_nested" {
    if (true) return error.SkipZigTest;
}

test "fanotify_attrib_file" {
    if (builtin.single_threaded) return error.SkipZigTest;
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

    _ = res.expectEvent(
        FAN.EVENT { .attrib = true },
        file_name, 
        null,
    ) catch |err| {
        println("{any}", .{ res.events.items });
        return err;
    };
}

test "fanotify_attrib_nested" {
    if (true) return error.SkipZigTest;
}

test "fanotify_attrib_dir" {
    // FIXME: attrib changes on dir an unrealiable
    if (true) return error.SkipZigTest;
    if (builtin.single_threaded) return error.SkipZigTest;
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

    _ = res.expectEvent(
        FAN.EVENT { .attrib = true, .ondir = true },
        dir_name, 
        null,
    ) catch |err| {
        println("{any}", .{ res.events.items });
        return err;
    };
}

test "fanotify_attrib_dir_nested" {
    if (true) return error.SkipZigTest;
}

