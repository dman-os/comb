//! TODO: remove dependence on heap allocator
//! FIXME: none of these are threadsafe
//! FIXME: this is too complex. Get rid of Pager and just use the Allocator

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

pub const mod_utils = @import("utils.zig");
const println = mod_utils.println;
const dbg = mod_utils.println;

const mmap_align = std.mem.page_size;

pub const Pager = struct {
    const Self = @This();
    pub const Error = AllocError || SwapInError;

    pub const AllocError = error {
        FileTooBig,
        FileBusy,
        InputOutput,
    } || Allocator.Error || std.os.UnexpectedError;

    pub const SwapInError = error {
        OutOfMemory,
        LockedMemoryLimitExceeded,
    } || std.os.UnexpectedError;

    pub const PageNo = usize;
    pub const PageSlice = []align(mmap_align)u8;

    const VTable = struct {
        alloc: fn (self: *anyopaque) AllocError!Pager.PageNo,
        free: fn (self: *anyopaque, no: Pager.PageNo) void,
        swapIn: fn (self: *anyopaque, no: Pager.PageNo) SwapInError!Pager.PageSlice,
        swapOut: fn (self: *anyopaque, no: Pager.PageNo) void,
        isSwappedIn: fn (self: *const anyopaque, no: Pager.PageNo) bool,
        pageSize: fn (self: *const anyopaque) usize,
    };

    ptr: *anyopaque,
    vtable: *const VTable,

    pub inline fn alloc(self: Self) AllocError!Pager.PageNo {
        return self.vtable.alloc(self.ptr);
    }

    /// This doesn't swap out the page.
    pub inline fn free(self: Self, no: Pager.PageNo) void {
        return self.vtable.free(self.ptr, no);
    }

    pub inline fn swapIn(self: Self, no: Pager.PageNo) SwapInError!Pager.PageSlice {
        return self.vtable.swapIn(self.ptr, no);
    }

    pub inline fn isSwappedIn(self: Self, no: Pager.PageNo) bool {
        return self.vtable.isSwappedIn(self.ptr, no);
    }

    pub inline fn swapOut(self: Self, no: Pager.PageNo) void {
        return self.vtable.swapOut(self.ptr, no);
    }

    pub inline fn pageSize(self: Self) usize {
        return self.vtable.pageSize(self.ptr);
    }

    pub inline fn swapAndFree(self: Self, no: Pager.PageNo) void {
        self.swapOut(no);
        self.free(no);
    }

    /// Modified from std.mem.Allocator
    pub fn init(
        pointer: anytype,
        const_ptr: anytype,
        comptime allocFn: fn (self: @TypeOf(pointer)) AllocError!Pager.PageNo,
        comptime freeFn: fn (self: @TypeOf(pointer), no: Pager.PageNo) void,
        comptime swapInFn: fn (self: @TypeOf(pointer), no: Pager.PageNo) SwapInError!Pager.PageSlice,
        comptime swapOutFn: fn (self: @TypeOf(pointer), no: Pager.PageNo) void,
        comptime pageSizeFn: fn (self: @TypeOf(const_ptr)) usize,
        comptime isSwappedInFn: fn (self: @TypeOf(const_ptr), no: Pager.PageNo) bool,
    ) Self {
        const PagerPtr = @TypeOf(pointer);
        const ptr_info = @typeInfo(PagerPtr);
        std.debug.assert(ptr_info == .Pointer); // Must be a pointer
        std.debug.assert(ptr_info.Pointer.size == .One); // Must be a single-item pointer

        const ConstPagerPtr = @TypeOf(const_ptr);
        const const_ptr_info = @typeInfo(ConstPagerPtr);
        std.debug.assert(const_ptr_info == .Pointer); // Must be a pointer
        std.debug.assert(const_ptr_info.Pointer.size == .One); // Must be a single-item pointer
        std.debug.assert(const_ptr_info.Pointer.is_const); // Must be const 

        const alignment = ptr_info.Pointer.alignment;

        const gen = struct {
            fn allocImpl(alloc_ptr: *anyopaque) AllocError!Pager.PageNo {
                const self = @ptrCast(PagerPtr, @alignCast(alignment, alloc_ptr));
                return @call(.{ .modifier = .always_inline }, allocFn, .{ self, });
            }
            fn swapInImpl(alloc_ptr: *anyopaque, no: Pager.PageNo) SwapInError!Pager.PageSlice {
                const self = @ptrCast(PagerPtr, @alignCast(alignment, alloc_ptr));
                return @call(.{ .modifier = .always_inline }, swapInFn, .{ self, no });
            }

            fn swapOutImpl(alloc_ptr: *anyopaque, no: Pager.PageNo) void {
                const self = @ptrCast(PagerPtr, @alignCast(alignment, alloc_ptr));
                return @call(.{ .modifier = .always_inline }, swapOutFn, .{ self, no });
            }

            fn freeImpl(alloc_ptr: *anyopaque, no: Pager.PageNo) void {
                const self = @ptrCast(PagerPtr, @alignCast(alignment, alloc_ptr));
                return @call(.{ .modifier = .always_inline }, freeFn, .{ self, no });
            }

            fn isSwappedInImpl(alloc_ptr: *const anyopaque, no: Pager.PageNo) bool {
                const self = @ptrCast(*const PagerPtr, &@alignCast(alignment, alloc_ptr)).*;
                return @call(.{ .modifier = .always_inline }, isSwappedInFn, .{ self, no });
            }

            fn pageSizeImpl(alloc_ptr: *const anyopaque) usize {
                const self = @ptrCast(*const PagerPtr, &@alignCast(alignment, alloc_ptr)).*;
                return @call(.{ .modifier = .always_inline }, pageSizeFn, .{ self });
            }

            const vtable = VTable {
                .alloc = allocImpl,
                .free = freeImpl,
                .swapIn = swapInImpl,
                .swapOut = swapOutImpl,
                .pageSize = pageSizeImpl,
                .isSwappedIn = isSwappedInImpl,
            };
        };

        return .{
            .ptr = pointer,
            .vtable = &gen.vtable,
        };
    }
};

const PageNo = Pager.PageNo;
const PageSlice = Pager.PageSlice;

// TODO: add support for page ranges
//  - the free_list is going to be tricky; we'd need to recognize and adjacent 
//    free pages
pub const MmapPager = struct {
    const Self = @This();

    pub const Config = struct {
        page_size: usize = std.mem.page_size,
        /// Weather or not to delete swap file on deinit.
        /// If there's an error while deleting it, the error is ignored.
        deinit_clean_up_file: bool = true,
        /// Weather or not to swap out pages that were found to be swapped in on deinit.
        deinit_leak_swapped_in_pages: bool = true,
    };

    const Page = struct {
        slice: ?PageSlice,
        free: bool,
        // len: usize,
    };

    const FreeList = std.PriorityQueue(
        PageNo, 
        void, 
        struct {
            // we wan't earlier pages to be filled in earlier
            fn cmp(_: void, a: PageNo, b: PageNo) std.math.Order {
                return std.math.order(a, b);
            }
        }.cmp
    );

    config: Config,
    a7r: Allocator,

    backing_file: std.fs.File,
    pages: std.ArrayListUnmanaged(Page),
    free_list: FreeList,

    pub fn init(a7r: Allocator, backing_file_path: []const u8, config: Config) !Self {
        // const backing_file_size = 1024 * 1024 * 1024;
        var mmap_file = try std.fs.createFileAbsolute(backing_file_path, .{
            .read = true,
        });
        var self = Self{
            .config = config,
            .a7r = a7r,
            .backing_file = mmap_file,
            .pages = std.ArrayListUnmanaged(Page){},
            .free_list = FreeList.init(a7r, .{}),
        };
        return self;
    }

    pub fn deinit(self: *Self) void {
        if (self.config.deinit_clean_up_file) {
            var fd_buf = [_]u8{0} ** 128;
            const fd_path = std.fmt.bufPrint(&fd_buf, "/proc/self/fd/{}", .{ self.backing_file.handle }) catch @panic("so this happened");
            var buf = [_]u8{0} ** std.fs.MAX_PATH_BYTES;
            if (std.fs.readLinkAbsolute(fd_path, &buf)) |actual_path| {
                self.backing_file.close();
                std.fs.deleteFileAbsolute(actual_path) catch |err|{
                    std.log.warn("error deleting file  when trying to remove mmap file: {}",. { err });
                };
            } else |err| {
                std.log.warn("trouble with readlink when trying to remove mmap file: {}",. { err });
            }
        }
        var active_count: usize = 0;
        var swapped_in_count: usize = 0;
        for (self.pages.items) |page| {
            if (!page.free) {
                active_count += 1;
            }
            if (page.slice) |slice| {
                swapped_in_count += 1;
                if (!self.config.deinit_leak_swapped_in_pages) {
                    std.os.munmap(slice);
                }
            }
        }
        if (active_count > 0) {
            std.log.warn("{} allocated pages at deinit of " ++ @typeName(Self), .{ active_count });
        }
        if (swapped_in_count > 0) {
            if (self.config.deinit_leak_swapped_in_pages) {
                std.log.warn("leaked {} swapped in pages at deinit of " ++ @typeName(Self), .{ swapped_in_count });
            } else {
                std.log.warn("swapped out {} pages that were swapped in and in use at deinit of " ++ @typeName(Self), .{ swapped_in_count });
            }
        }
        self.free_list.deinit();
        self.pages.deinit(self.a7r);
    }

    pub fn pager(self: *Self) Pager {
        return Pager.init(
            self, 
            @as(*const Self, self), 
            alloc, 
            free, 
            swapIn, 
            swapOut, 
            pageSize,
            isSwappedIn,
        );
    }

    pub fn pageSize(self: *const Self) usize {
        return self.config.page_size;
    }

    pub fn alloc(self: *Self) Pager.AllocError!PageNo {
        if (self.free_list.removeOrNull()) |no| {
            var page = &self.pages.items[no];
            page.free = false;
            return no;
        } else {
            // ensure capacity before trying anything
            try self.pages.ensureUnusedCapacity(self.a7r, 1);
            // ensure capacity on the free list to avoid error checking on free calls
            try self.free_list.ensureUnusedCapacity(1);
            try std.os.ftruncate(self.backing_file.handle, (self.pages.items.len + 1) * self.config.page_size)
                catch |err| switch (err) {
                    std.os.TruncateError.AccessDenied => unreachable,
                    std.os.TruncateError.FileTooBig => error.FileTooBig,
                    std.os.TruncateError.InputOutput=> error.InputOutput,
                    std.os.TruncateError.FileBusy=> error.FileBusy,
                    std.os.UnexpectedError.Unexpected => std.os.UnexpectedError.Unexpected,
                };
            try self.pages.append(self.a7r, Page{
                .slice = null,
                .free = false,
            });
            return self.pages.items.len - 1;
        }
    }

    /// This doesn't swap out the page.
    pub fn free(self: *Self, no: PageNo) void {
        if (no >= self.pages.items.len) {
            return;
        }
        var page = &self.pages.items[no];
        page.free = true;

        // TODO: conside turning this into an assertion
        if (page.slice != null) {
            std.log.warn("page {} was freed while swapped in", .{ no });
            @panic("ehh");
        }

        if (no == self.pages.items.len - 1) {
            var ii = no;
            while (true) : ({
                ii -= 1;
            }) {
                if (ii == 0 or !(self.pages.items[ii].free)) break;
                _ = self.pages.popOrNull();
            }
        } else {
            // we've been growing the free list along the page list so it's ok
            self.free_list.add(no) catch unreachable;
        }
    }

    pub fn swapIn(self: *Self, no: PageNo) Pager.SwapInError!PageSlice {
        var page = &self.pages.items[no];
        if (page.free) {
            // return error.PageNotAllocated;
            @panic("Page is free");
        }
        if (page.slice) |slice| {
            return slice;
        } else {
            var slice = try std.os.mmap(
                null,
                self.config.page_size,
                std.os.PROT.READ | std.os.PROT.WRITE,
                std.os.MAP.SHARED, // | std.os.MAP.ANONYMOUS,
                self.backing_file.handle,
                // -1,
                no * self.config.page_size,
            ) catch |err| switch(err) {
                std.os.MMapError.MemoryMappingNotSupported => @panic("system doesn't support mmaping"),
                std.os.MMapError.AccessDenied => unreachable,
                std.os.MMapError.PermissionDenied => unreachable,
                std.os.MMapError.LockedMemoryLimitExceeded => error.LockedMemoryLimitExceeded,
                std.os.MMapError.OutOfMemory => error.OutOfMemory,
                std.os.UnexpectedError.Unexpected => std.os.UnexpectedError.Unexpected,
            };
            errdefer std.os.munmap(slice);
            page.slice = slice;
            return slice;
        }
    }

    pub fn swapOut(self: *Self, no: PageNo) void {
        if (self.pages.items.len <= no) {
            std.log.warn(@typeName(Self) ++ " received swapOut for unrecognized page no {}", .{no});
            @panic("eawoo");
        }
        var page = &self.pages.items[no];
        if (page.slice) |slice| {
            // _ = slice;
            std.os.munmap(slice);
            page.slice = null;
        } else {
            std.log.warn(@typeName(Self) ++ " received swapOut for page not in memory at no {}", .{no});
            @panic("eawoo");
        }
    }

    pub fn isSwappedIn(self: *const Self, no: PageNo) bool {
        const page = self.pages.items[no];
        return page.slice != null;
    }
};

test "MmapPager.usage" {
    var a7r = std.testing.allocator;
    const page_size = std.mem.page_size;
    var mmap_pager = try MmapPager.init(a7r, "/tmp/MmmapUsage.test", .{
        .page_size = page_size,
    });
    defer mmap_pager.deinit();
    var pager = mmap_pager.pager();

    const item_per_page = page_size / @sizeOf(usize);
    const page_count = 10;
    var nums = [_]usize{0} ** (page_count * item_per_page);
    for (nums) |*num, ii| {
        num.* = ii;
    }
    var pages: [page_count]PageNo = undefined;
    for (pages) |*no, pii| {
        no.* = try pager.alloc();
        var bytes = try pager.swapIn(no.*);
        defer pager.swapOut(no.*);
        for (std.mem.bytesAsSlice(usize, bytes)) |*num, ii| {
            num.* = nums[(pii * item_per_page) + ii];
        }
    }
    defer for (pages) |no| {
        pager.free(no);
    };

    for (pages) |no, pii| {
        var bytes = try pager.swapIn(no);
        defer pager.swapOut(no);
        for (std.mem.bytesAsSlice(usize, bytes)) |num, ii| {
            try std.testing.expectEqual(nums[(pii * item_per_page) + ii], num);
        }
    }
    {
        const page_no = page_count / 2;
        pager.free(pages[page_no]);
        // TODO: find a way to test panics?
        // try std.testing.expectError(error.PageNotAllocated, pager.swapIn(pages[page_no]));

        var new_stuff = [_]usize{0} ** item_per_page;
        for (new_stuff) |*num, ii| {
            num.* = item_per_page - ii;
            nums[(page_no * item_per_page) + ii] = item_per_page - ii;
        }

        var no = try pager.alloc();
        var bytes = try pager.swapIn(no);
        defer pager.swapOut(no);
        for (std.mem.bytesAsSlice(usize, bytes)) |*num, ii| {
            num.* = new_stuff[ii];
        }
    }
    for (pages) |no, pii| {
        var bytes = try pager.swapIn(no);
        defer pager.swapOut(no);
        for (std.mem.bytesAsSlice(usize, bytes)) |num, ii| {
            try std.testing.expectEqual(nums[(pii * item_per_page) + ii], num);
        }
    }
}

/// This type mainly avoids unessary syscalls and double map of the same page
pub const LRUSwapCache = struct {
    const Self = @This();
    
    const HotPage = struct {
        /// the net number of swap in and swap outs on this page
        /// put in cold list when this hits zero
        swap_ins: usize, 
        slice: PageSlice,
    };

    const ColdPage = struct {
        no: PageNo,
        /// timestamp from our monotonic timer
        last_used: usize, 
        slice: PageSlice,
    };

    const HotList = std.AutoHashMapUnmanaged(PageNo, HotPage);
    const ColdList = struct {
        const Que = std.TailQueue(ColdPage);
        const Vec = std.ArrayListUnmanaged(Que.Node); 
        // Mpas to the an index in the Vec
        const Map = std.AutoHashMapUnmanaged(PageNo, usize);

        que: Que,
        vec: Vec,
        map: Map,
        free_list: std.ArrayListUnmanaged(usize),

        fn init(a7r: Allocator, max_cold: u32) !ColdList {
            if (max_cold == 0) @panic("max_cold set too low on " ++ @typeName(Self));
            var self = ColdList {
                .vec = Vec{},
                .map = Map{},
                .que = Que{},
                .free_list = std.ArrayListUnmanaged(usize){},
            };
            try self.vec.ensureTotalCapacity(a7r, max_cold);
            try self.map.ensureTotalCapacity(a7r, max_cold);
            try self.free_list.ensureTotalCapacity(a7r, max_cold);
            return self;
        }

        fn deinit(self: *ColdList, a7r: Allocator) void {
            self.map.deinit(a7r);
            self.vec.deinit(a7r);
            self.free_list.deinit(a7r);
        }
    };


    a7r: Allocator,
    backing_pager: Pager,
    hot: HotList,
    cold: ColdList,
    timer: std.time.Timer,
    max_cold: u32,

    pub fn init(
        a7r: Allocator, 
        backing_pager: Pager, 
        max_cold: u32,
    ) !Self {
        var self = Self {
            .a7r = a7r,
            .backing_pager = backing_pager,
            .hot = HotList{},
            .cold = try ColdList.init(a7r, max_cold),
            .timer = try std.time.Timer.start(),
            .max_cold = max_cold,
        };
        return self;
    }
    
    pub fn deinit(self: *Self) void {
        if (self.hot.count() > 0) {
            std.log.warn("{} hot pages in " ++ @typeName(Self) ++ " at time of deinit", .{ self.hot.count() });
        }
        var it = self.cold.map.keyIterator();
        while (it.next()) |no| {
            self.backing_pager.swapOut(no.*);
        }
        self.hot.deinit(self.a7r);
        self.cold.deinit(self.a7r);
    }

    pub fn hot_count(self: *const Self) u32 {
        return self.hot.count();
    }
    
    pub fn cold_count(self: *const Self) u32 {
        return self.cold.map.count();
    }

    pub fn pager(self: *Self) Pager {
        return Pager.init(
            self, 
            @as(*const Self, self), 
            alloc, 
            free, 
            swapIn, 
            swapOut, 
            pageSize,
            isSwappedIn,
        );
    }

    pub fn pageSize(self: *const Self) usize {
        return self.backing_pager.pageSize();
    }

    pub fn alloc(self: *Self) Pager.AllocError!PageNo {
        return self.backing_pager.alloc();
    }

    pub fn free(self: *Self, no: PageNo) void {
        // println("freeing page {}", .{no});
        // TODO: consider adding a wrning if page is hot
        // instead of relying on the backing pager behavior
        if (self.cold.map.fetchRemove(no)) |pair| {
            const idx = pair.value;
            self.cold.que.remove(&self.cold.vec.items[idx]);
            self.cold.free_list.append(self.a7r, idx) catch @panic("impossible");
            // println("swapping out page {} from cold cache to free", .{no});
            self.backing_pager.swapOut(no);
        }
        self.backing_pager.free(no);
    }

    pub fn swapIn(self: *Self, no: PageNo) Pager.SwapInError!PageSlice {
        if (self.hot.getPtr(no)) |hot| {
            hot.swap_ins += 1;
            // println("swappingIn already active page {}", .{ no });
            return hot.slice;
        } else if (self.cold.map.getEntry(no)) |pair| {
            // println("swappingIn cold page {}", .{ no });
            const idx = pair.value_ptr.*;
            const cold_p = self.cold.vec.items[idx].data;
            const slice = cold_p.slice;
            std.debug.assert(
                (
                    try self.hot.fetchPut(
                            self.a7r, 
                            no, 
                            HotPage { .slice = slice, .swap_ins = 1 }
                    )
                ) == null
            );
            self.cold.que.remove(&self.cold.vec.items[idx]);
            self.cold.map.removeByPtr(pair.key_ptr);
            self.cold.free_list.append(self.a7r, idx) catch @panic("impossible");
            return slice;
        } else {
            // println("swappingIn fresh page {}", .{ no });
            const slice = try self.backing_pager.swapIn(no);
            errdefer self.backing_pager.swapOut(no);
            std.debug.assert(
                (
                    try self.hot.fetchPut(
                            self.a7r, 
                            no, 
                            HotPage { .slice = slice, .swap_ins = 1 }
                    )
                ) == null
            );
            return slice;
        }
    }

    pub fn swapOut(self: *Self, no: PageNo) void {
        // TODO: consider case when page is in cold cache?
        if (self.hot.getPtr(no)) |hot| {
            hot.swap_ins -= 1;
            if (hot.swap_ins == 0) {
                const slice = hot.slice;
                // NOTE: remove after you get the slice
                _ = self.hot.remove(no); // TODO: prolly can optimize this call

                // if cold list is full, evict the oldest entry
                if (self.cold.map.count() == self.max_cold) {
                    const min = self.cold.que.popFirst().?;
                    const min_no = min.data.no;
                    const idx = self.cold.map.fetchRemove(min_no).?.value;
                    self.cold.free_list.append(self.a7r, idx) catch @panic("impossible");
                    // println("swapping out page {} FROM cold list", .{ min_k });
                    self.backing_pager.swapOut(min_no);
                }

                // check the free list for free spots
                const idx = if (self.cold.free_list.popOrNull()) |idx| blk: {
                    self.cold.vec.items[idx] = ColdList.Que.Node {
                        .data = ColdPage { 
                            .no = no,
                            .last_used  = self.timer.read(),
                            .slice = slice,
                        },
                    };
                    break :blk idx;
                } else blk: {
                    self.cold.vec.append(self.a7r, ColdList.Que.Node {
                        .data = ColdPage { 
                            .no = no,
                            .last_used  = self.timer.read(),
                            .slice = slice,
                        },
                    }) catch @panic("impossible");
                    break :blk self.cold.vec.items.len - 1;
                };
                self.cold.que.append(&self.cold.vec.items[idx]);
                self.cold.map.put(
                    self.a7r, 
                    no, 
                    idx,
                ) catch @panic("impossible");
            } else {
                // println("deceasing swap in ctr for page {}", .{ no });
            }
        } else {
            self.backing_pager.swapOut(no);
            std.log.warn(
                "unrecognized page no swapped Out through " ++ 
                @typeName(Self) ++ 
                ": you're prolly misusing the cache", .{}
            );
            @panic("eawoo");
        }
    }

    pub fn isSwappedIn(self: *const Self, no: PageNo) bool {
        return self.hot.contains(no);
    }
};

// pub const SwapInTracker = struct {
//     const Self = @This();

//     swappedInPages: std.AutoHashMapUnmanaged(PageNo, void),

//     pub fn init() Self {
//         return Self {
//             .swappedInPages = std.AutoHashMapUnmanaged(PageNo, .{}),
//         };
//     }

//     pub fn deinit(self: *Self, ha7r: Allocator, pager: *Pager) void {
//         var it = self.swappedInPages.iterator();
//         while (it.next()) |no| {
//             pager.swapOut(no);
//         }
//         self.swappedInPages.deinit(ha7r);
//     }

//     fn swapIn(self: *Self, ha7r: Allocator, pager: *Pager, no: PageNo) !PageSlice {
//         try self.swappedInPages.put(ha7r, no, .{});
//         errdefer _ = self.swappedInPages.remove(no);
//         const slice = try pager.swapIn(no);
//         return slice;
//     }

//     fn swapOut(self: *Self, ha7r: Allocator, pager: *Pager, no: PageNo) void {
//         if (!self.swappedInPages.remove(ha7r, no)){
//             std.log.warn("unrecognized page {} swapped out through" ++ @typeName(Self), .{no});
//         }
//         pager.swapOut(no);
//     }
// };

pub const SinglePageCache = struct {
    const Self = @This();

    const InPage = struct {
        no: PageNo,
        slice: []align(mmap_align)u8,
    };

    backing_pager: Pager,
    in_page: ?InPage = null,

    pub fn init(backing_pager: Pager) Self {
        return Self { .in_page = null, .backing_pager = backing_pager };
    }

    pub fn deinit(self: *Self) void {
        if (self.in_page != null) {
            std.log.warn("active page in " ++ @typeName(SinglePageCache) ++ " at time of deinit", .{});
        }
    }

    // pub fn pager(self: *Self) Pager {
    //     return Pager.init(
    //         self, 
    //         @as(*const Self, self), 
    //         alloc, 
    //         free, 
    //         swapIn, 
    //         swapOut, 
    //         pageSize,
    //         isSwappedIn,
    //     );
    // }

    // pub fn pageSize(self: *const Self) usize {
    //     return self.backing_pager.pageSize();
    // }

    pub fn alloc(self: *Self) Pager.AllocError!PageNo {
        return self.backing_pager.alloc();
    }

    pub fn free(self: *Self, no: PageNo) void {
        if (self.in_page) |in_p| {
            if (no == in_p.no) {
                // TODO: consider giving a warning for free calls while swapped in
                self.in_page = null;
            } else {
                std.log.warn(
                    "unrecognized page no freed through " ++ 
                    @typeName(SinglePageCache) ++ 
                    ": you're prolly misusing the cache", .{}
                );
            }
        } 
        self.backing_pager.free(no);
    }

    pub fn swapIn(self: *Self, no: PageNo) !PageSlice {
        // println("goo from {}", .{ @ptrToInt(self) });
        // defer println("gaa", .{});
        if (self.in_page) |in_p| {
            if (in_p.no == no) {
                // println("swapping in for access to cached in slice for page {}", .{no});
                return in_p.slice;
            } else {
                // println("page swap for cache from {} to {}", .{in_p.no, no});
                self.backing_pager.swapOut(in_p.no);
                const slice = try self.backing_pager.swapIn(no);
                self.in_page = InPage {
                    .no = no,
                    .slice = slice,
                };
                return slice;
            }
        } else {
            // println("swapping in page {} and adding to cache", .{no});
            const slice = try self.backing_pager.swapIn(no);
            self.in_page = InPage {
                .no = no,
                .slice = slice,
            };
            return slice;
        }
    }

    pub fn swapOut(self: *Self, no: PageNo) void {
        if (self.in_page) |in_p| {
            if (no == in_p.no) {
                self.in_page = null;
                // println("swapping out page {} from cache", .{no});
            } else{
                std.log.warn(
                    "unrecognized page no swapped Out through " ++ 
                    @typeName(SinglePageCache) ++ 
                    ": you're prolly misusing the cache", .{}
                );
            }
        } else {
            std.log.warn(
                "page swapped Out through an empty " ++ 
                @typeName(SinglePageCache) ++ 
                ": you're prolly misusing the cache", .{}
            );
        }
        self.backing_pager.swapOut(no);
    }

    pub inline fn swapOutResident(self: *Self) void {
        if (self.in_page) |in_p| {
            // println("swapping out resident page {}", .{in_p.no});
            self.backing_pager.swapOut(in_p.no);
            self.in_page = null;
        }
    }
    pub fn isSwappedIn(self: *const Self, no: PageNo) bool {
        if (self.in_page) |in_p| {
            return in_p.no == no;
        } else {
            return false;
        }
    }
};

pub const SwappingAllocator = struct {
    pub const Ptr = usize;
    // TODO: clean me up
    pub const Error = Pager.Error;

    const VTable = struct {
        alloc: fn (self: *anyopaque, len: usize) Error!Ptr,
        swapIn: fn (self: *anyopaque, ptr: Ptr) Error![]u8,
        swapOut: fn (self: *anyopaque, ptr: Ptr) void,
        free: fn (self: *anyopaque, ptr: Ptr) void,
    };

    ptr: *anyopaque,
    vtable: *const VTable,

    pub fn alloc(self: SwappingAllocator, len: usize) Error!Ptr {
        return self.vtable.alloc(self.ptr, len);
    }

    pub fn swapIn(self: SwappingAllocator, ptr: Ptr) Error![]u8 {
        return self.vtable.swapIn(self.ptr, ptr);
    }

    pub fn swapOut(self: SwappingAllocator, ptr: Ptr) void {
        return self.vtable.swapOut(self.ptr, ptr);
    }

    pub fn free(self: SwappingAllocator, ptr: Ptr) void {
        return self.vtable.free(self.ptr, ptr);
    }

    pub const DupeRes = struct {
        ptr: Ptr,
        slice: []u8,
    };

    /// Don't forget to swap out the ptr if you don't need the slice.
    pub fn dupe(self: SwappingAllocator, bytes: []const u8) Error!DupeRes {
        const ptr = try self.alloc(bytes.len);
        var slice = try self.swapIn(ptr);
        std.mem.copy(u8, slice, bytes);
        return DupeRes {
            .ptr = ptr,
            .slice = slice,
        };
    }

    pub fn dupeJustPtr(self: SwappingAllocator, bytes: []const u8) Error!Ptr {
        const ptr = try self.alloc(bytes.len);
        var slice = try self.swapIn(ptr);
        defer self.swapOut(ptr);
        std.mem.copy(u8, slice, bytes);
        return ptr;
    }

    /// Modified from std.mem.Allocator
    pub fn init(
        pointer: anytype,
        comptime allocFn: fn (ptr: @TypeOf(pointer), len: usize) Error!Ptr,
        comptime swapInFn: fn (ptr: @TypeOf(pointer), ptr: Ptr) Error![]u8,
        comptime swapOutFn: fn (ptr: @TypeOf(pointer), ptr: Ptr) void,
        comptime freeFn: fn (ptr: @TypeOf(pointer), ptr: Ptr) void,
    ) SwappingAllocator {
        const AllocPtr = @TypeOf(pointer);
        const ptr_info = @typeInfo(AllocPtr);
        std.debug.assert(ptr_info == .Pointer); // Must be a pointer
        std.debug.assert(ptr_info.Pointer.size == .One); // Must be a single-item pointer

        const alignment = ptr_info.Pointer.alignment;

        const gen = struct {
            fn allocImpl(alloc_ptr: *anyopaque, len: usize) Error!Ptr {
                const self = @ptrCast(AllocPtr, @alignCast(alignment, alloc_ptr));
                return @call(.{ .modifier = .always_inline }, allocFn, .{ self, len,  });
            }

            fn swapInImpl(alloc_ptr: *anyopaque, ptr: Ptr) Error![]u8 {
                const self = @ptrCast(AllocPtr, @alignCast(alignment, alloc_ptr));
                return @call(.{ .modifier = .always_inline }, swapInFn, .{ self, ptr,  });
            }

            fn swapOutImpl(alloc_ptr: *anyopaque, ptr: Ptr) void {
                const self = @ptrCast(AllocPtr, @alignCast(alignment, alloc_ptr));
                return @call(.{ .modifier = .always_inline }, swapOutFn, .{ self, ptr, });
            }

            fn freeImpl(alloc_ptr: *anyopaque, ptr: Ptr) void {
                const self = @ptrCast(AllocPtr, @alignCast(alignment, alloc_ptr));
                return @call(.{ .modifier = .always_inline }, freeFn, .{ self, ptr, });
            }

            const vtable = VTable{
                .alloc = allocImpl,
                .swapIn = swapInImpl,
                .swapOut = swapOutImpl,
                .free = freeImpl,
            };
        };

        return .{
            .ptr = pointer,
            .vtable = &gen.vtable,
        };
    }
};


pub const MmapAllocatorConfig = struct {
    page_size: usize = std.mem.page_size,
};

pub fn MmapSwappingAllocator(config: MmapAllocatorConfig) type {
    return struct {
        const Self = @This();

        const bucket_count = std.math.log2(config.page_size);
        const BuckIndex = std.meta.Int(.unsigned, std.math.log2(std.math.ceilPowerOfTwoAssert(u16, bucket_count)));
        const SlotIndex = std.meta.Int(
            .unsigned, 
            @bitSizeOf(SwappingAllocator.Ptr) - @bitSizeOf(BuckIndex)
        );
        /// TODO: consnoer using a usize and bittweedling to improve performance
        /// TODO: hate the name
        pub const CustomPtr = packed struct {
            buck: BuckIndex,
            slot: SlotIndex,

            fn toGpPtr(self: CustomPtr) SwappingAllocator.Ptr {
                return @bitCast(SwappingAllocator.Ptr, self);
            }
            fn fromGpPtr(ptr: SwappingAllocator.Ptr) CustomPtr {
                return @bitCast(CustomPtr, ptr);
            }
        };

        comptime {
            // TODO: THiS gets fucked if usize bytes are above 255
            if (@sizeOf(CustomPtr) != @sizeOf(SwappingAllocator.Ptr)) {
                var buf = [_]u8{0} ** 64;
                var msg = std.fmt.bufPrint(
                    &buf, 
                    "CustomPtr size mismatch: {} !=  {}", 
                    .{ @sizeOf(CustomPtr), @sizeOf(SwappingAllocator.Ptr) }
                ) catch @panic("wtf");
                @compileError(msg);
            }
        }

        const Bucket = struct {
            // const Class = std.meta.Int(.unsigned, bucket_count);
            const Allocation = struct {
                // gen: usize,
                free: bool,
                len: usize, // FIXME: usize is too big for this purpose
            };
            const FreeList = std.PriorityQueue(
                SlotIndex,
                void,
                struct {
                    fn cmp(_: void, a: SlotIndex, b: SlotIndex) std.math.Order {
                        return std.math.order(a, b);
                    }
                }.cmp,
            );

            // allocs: std.MultiArrayList(Allocation),
            allocs: std.ArrayListUnmanaged(Allocation),
            pages: std.ArrayListUnmanaged(PageNo),
            free_list: FreeList,
            // class: Class,
            per_page: usize,

            fn init(ha7r: Allocator, per_page: usize) Bucket {
                return .{
                    // .allocs = std.MultiArrayList(Allocation){},
                    .allocs = std.ArrayListUnmanaged(Allocation){},
                    .pages = std.ArrayListUnmanaged(PageNo){},
                    .free_list = FreeList.init(ha7r, .{}),
                    // .class = class,
                    .per_page = per_page,
                };
            }

            fn deinit(self: *Bucket, sa6r: *Self) void{
                self.allocs.deinit(sa6r.ha7r);
                for (self.pages.items) |no| {
                    sa6r.pager.free(no);
                    // since this bucket is exclusively using the page
                    // this should be safe from double swap out
                    if (sa6r.pager.isSwappedIn(no))
                    {
                        sa6r.pager.swapAndFree(no);
                    }
                    else sa6r.pager.free(no);
                }
                self.pages.deinit(sa6r.ha7r);
                self.free_list.deinit();
            }

            fn addOne(self: *Bucket, alloc_len: usize, sa6r: *Self) !SlotIndex {
                // std.debug.print("alloc_len = {}, slot_size = {}\n", .{ alloc_len, (sa6r.pager.config.page_size / self.per_page)});
                std.debug.assert(alloc_len <= (config.page_size / self.per_page));
                if (self.free_list.removeOrNull()) |slot| {
                    self.allocs.items[slot].free = false;
                    self.allocs.items[slot].len = alloc_len;
                    return slot;
                } else {
                    // check if we have capacity in our allocated pages
                    {
                        const len = self.allocs.items.len;
                        if (len == std.math.maxInt(SlotIndex)) @panic(@typeName(Bucket) ++ " is full");
                        const cap = self.pages.items.len * self.per_page;
                        if (len >= cap) {
                            // check if our heap's in good shape before talking to swap
                            try self.pages.ensureUnusedCapacity(sa6r.ha7r, 1);
                            const new_page = try sa6r.pager.alloc();
                            // println("bucket of class {} allocated page {}", .{ config.page_size / self.per_page, new_page });;
                            _ = try self.pages.append(sa6r.ha7r, new_page);
                        }
                    }
                    try self.allocs.ensureUnusedCapacity(sa6r.ha7r, 1);
                    try self.allocs.append(sa6r.ha7r, Allocation {
                        // .gen = 0,
                        .free = false,
                        .len = alloc_len,
                    });
                    return @intCast(SlotIndex, self.allocs.items.len - 1);
                }
            }

            pub fn swapIn(self: *Bucket, slot: SlotIndex, pager: *Pager) ![]u8 {
                // println("bucket of class {} swapping in slot {}", .{ config.page_size / self.per_page, slot });
                const meta = self.allocs.items[slot];
                if (meta.free) @panic("Ptr points to free allocation");
                const page_idx = slot / self.per_page;
                const page_slice = try pager.swapIn(self.pages.items[page_idx]);
                // const page_slice = std.mem.bytesAsSlice(T, bytes[0..((bytes.len / @sizeOf(T)) * @sizeOf(T))]);
                const size = @divExact(config.page_size, self.per_page);
                const page_slot = slot - (page_idx * self.per_page);
                return page_slice[page_slot * size..(page_slot * size) + meta.len];
            }

            pub fn swapOut(self: *Bucket, slot: SlotIndex, pager: *Pager) void {
                if (self.allocs.items[slot].free) @panic("Ptr points to free allocation");
                const page_idx = slot / self.per_page;
                pager.swapOut(self.pages.items[page_idx]);
            }

            pub fn free(self: *Bucket, slot: SlotIndex) void {
                var meta = &self.allocs.items[slot];
                if (meta.free) @panic("Double free");
                meta.free = true;
                self.free_list.add(slot) catch @panic("so, this happened");
            }
        };

        ha7r: Allocator,
        pager: Pager,
        buckets: [bucket_count]?Bucket,

        pub fn init(ha7r: Allocator, pager: Pager) Self {
            return .{
                .ha7r = ha7r,
                .pager = pager,
                .buckets = [_]?Bucket{null} ** bucket_count,
            };
        }

        pub fn deinit(self: *Self) void {
            for (self.buckets) |*opt| {
                if (opt.*) |*buck| {
                    buck.deinit(self);
                }
            }
        }

        pub fn allocator(self: *Self) SwappingAllocator {
            return SwappingAllocator.init(self, alloc, swapIn, swapOut, free);
        }

        pub fn swapIn(self: *Self, gpptr: SwappingAllocator.Ptr) SwappingAllocator.Error![]u8 {
            const ptr = CustomPtr.fromGpPtr(gpptr);
            const buck_idx = ptr.buck;
            if (self.buckets[buck_idx]) |*buck|{
                return try buck.swapIn(ptr.slot, &self.pager);
            } else {
                @panic("Ptr points to empty bucket");
            }
        }

        pub fn swapOut(self: *Self, gpptr: SwappingAllocator.Ptr) void {
            const ptr = CustomPtr.fromGpPtr(gpptr);
            const buck_idx = ptr.buck;
            if (self.buckets[buck_idx]) |*buck|{
                buck.swapOut(ptr.slot, &self.pager);
            } else {
                @panic("Ptr points to empty bucket");
            }
        }

        pub fn free(self: *Self, gpptr: SwappingAllocator.Ptr) void {
            const ptr = CustomPtr.fromGpPtr(gpptr);
            const buck_idx = ptr.buck;
            if (self.buckets[buck_idx]) |*buck|{
                buck.free(ptr.slot);
            } else {
                @panic("Ptr points to empty bucket");
            }
        }

        pub fn alloc(self: *Self, len: usize) SwappingAllocator.Error!SwappingAllocator.Ptr {
            const buck_idx = std.math.log2(std.math.ceilPowerOfTwoAssert(usize, len));
            if (buck_idx > std.math.maxInt(BuckIndex)) {
                std.debug.print("we got a big one boys. len = {}, buck_idx = {}, buck_count = {}, idx type = {any}\n", .{ len, buck_idx, bucket_count, @typeInfo(BuckIndex) });
                // std.debug.todo("large allocations");
                @panic("todo");
            } else {
                const buck_idx_fit = @intCast(BuckIndex, buck_idx);
                // println("allocating len {} in bucket of size {}", .{ len, @as(usize, 1) << buck_idx_fit });
                // std.debug.print("buck for len {} == {}\n", .{ len, @as(usize, 1) << buck_idx_fit });
                if (self.buckets[buck_idx_fit] == null) {
                    const slot_size = @as(usize, 1) << buck_idx_fit;
                    self.buckets[buck_idx_fit] = Bucket.init(
                        self.ha7r, 
                        @divExact(config.page_size, slot_size),
                    );
                }
                var buck = &(self.buckets[buck_idx_fit].?);
                var slot = try buck.addOne(len, self);
                return (CustomPtr{ .buck = buck_idx_fit, .slot = slot }).toGpPtr();
            }
        }
    };
}

test "MmapSwappingAllocator.usage" {
    const page_size = std.mem.page_size;
    const ha7r = std.testing.allocator;
    var mmap_pager = try MmapPager.init(ha7r, "/tmp/comb.test.SwappingAllocator.usage", .{
        .page_size = page_size,
    });
    defer mmap_pager.deinit();

    var lru = try LRUSwapCache.init(ha7r, mmap_pager.pager(), 1);
    defer lru.deinit();
    
    var ma7r = MmapSwappingAllocator(.{}).init(ha7r, lru.pager());
    defer ma7r.deinit();

    var sa7r = ma7r.allocator();

    // std.debug.print("Size MPA: {}\n", .{@sizeOf(MmapSwappingAllocator(.{ .page_size = 4 * 1024 }))});
    // std.debug.print("Size GPA: {}\n", .{@sizeOf(std.heap.GeneralPurposeAllocator(.{}))});

    const table = [_][]const u8{
        "i wondered all night"[0..],
        "i wondered all night"[0..],
        "i wondered all night all about you"[0..],
        "i've been here for years just wondering around about the neighborhood"[0..],
    };
    var ptrs: [table.len]SwappingAllocator.Ptr = undefined;

    for (&table) |case, ii| {
        ptrs[ii] = try sa7r.alloc(case.len);
        var string = try sa7r.swapIn(ptrs[ii]);
        defer sa7r.swapOut(ptrs[ii]);
        std.mem.copy(u8, string, case);
    }
    defer for (ptrs) |ptr|{
        sa7r.free(ptr);
    };

    // std.debug.print("Allocer\n{any}\n", .{ sa7r.buckets });
    for (&ptrs) |ptr, ii| {
        const string = try sa7r.swapIn(ptr);
        defer sa7r.swapOut(ptr);
        // println("{s} != {s}", .{ table[ii], string });
        try std.testing.expectEqualSlices(u8, table[ii], string);
    }

    const replacement = "i wonder still oh I doo";
    {
        sa7r.free(ptrs[1]);
        ptrs[1] = try sa7r.alloc(replacement.len);
        var string = try sa7r.swapIn(ptrs[1]);
        defer sa7r.swapOut(ptrs[1]);
        std.mem.copy(u8, string, replacement);
    }
    {
        const string = try sa7r.swapIn(ptrs[1]);
        defer sa7r.swapOut(ptrs[1]);
        try std.testing.expectEqualSlices(u8, replacement, string);
    }
}

pub fn SwappingList(comptime T: type) type {
    return struct {
        const Self = @This();

        // const SmallList = struct {
        //     ptr: SwappingAllocator.Ptr,
        //     slice: ?[]u8
        // };

        /// items per page
        per_page: usize,

        len: usize = 0,
        small_vec: ?std.ArrayListUnmanaged(T) = null,
        pages: std.ArrayListUnmanaged(PageNo) = .{},

        pub fn init(page_size: usize) Self {
            return Self {
                .per_page = page_size / @sizeOf(T),
            };
        }

        pub fn deinit(self: *Self, ha7r: Allocator, pager: Pager) void {
            for (self.pages.items) |no| {
                pager.free(no);
            }
            self.pages.deinit(ha7r);
            if (self.small_vec) |*vec| {
                vec.deinit(ha7r);
            }
        }

        pub inline fn capacity(self: *const Self) usize {
            if (self.small_vec) |vec| {
                return vec.items.len;
            } else {
                return self.pages.items.len * self.per_page;
            }
        }

        pub fn ensureUnusedCapacity(
            self: *Self, 
            ha7r: Allocator, 
            pager: Pager, 
            n_items: usize
        ) !void {
            const cap = self.capacity();
            const desired_cap = self.len + n_items;
            if (cap >= desired_cap) {
                return;
            }
            const new_cap = if (cap == 0) 
                    std.math.ceilPowerOfTwoAssert(usize, n_items)
                else 
                (
                    (
                        std.math.divCeil(usize, desired_cap, cap) 
                        catch @panic("overflow")
                    )
                    * cap
                );
            if (new_cap < self.per_page) {
                if (self.small_vec) |*vec| {
                    try vec.resize(ha7r, new_cap);
                } else {
                    self.small_vec = std.ArrayListUnmanaged(T){};
                    try self.small_vec.?.resize(ha7r, new_cap);
                }
            } else {
                var new_pages_req = 
                    (
                        std.math.divCeil(usize, new_cap, self.per_page) catch @panic("overflow")
                    ) - self.pages.items.len;
                // println(
                //     "expanding cap to {}, from {} with new pages {}, desired = {}",
                //     .{ new_cap, cap, new_pages_req, desired_cap }
                // );
                std.debug.assert(new_pages_req > 0);
                try self.pages.ensureUnusedCapacity(ha7r, new_pages_req);
                while (new_pages_req > 0) : ({
                    new_pages_req -= 1;
                }) {
                    const no = try pager.alloc();
                    errdefer pager.free(no);
                    // println("allocated page {} for " ++ @typeName(T) ++ " list", .{ no });
                    try self.pages.append(ha7r, no);
                }
                // NOTE: we won't necessarily double the first time we ditch the 
                // small_list since the old capacity might not be page flush
                if (self.small_vec) |*vec| {
                    var bytes = try pager.swapIn(self.pages.items[0]);
                    defer pager.swapOut(self.pages.items[0]);
                    const page_slice = std.mem.bytesAsSlice(T, bytes[0..((bytes.len / @sizeOf(T)) * @sizeOf(T))]);
                    std.mem.copy(T, page_slice, vec.items);
                    vec.deinit(ha7r);
                    self.small_vec = null;
                }
                std.debug.assert(self.pages.items.len * self.per_page > desired_cap);
            }
        }

        pub fn swapIn(self: *const Self, pager: Pager, idx: usize) !*T {
            if (idx >= self.len) @panic("out of bounds bich");
            return try self.swapInUnchecked(pager, idx);
        }

        /// Does no bound checking. Use to access slots with-in capacity
        /// but not necessarily length. Use `swapIn` for safer alternative.
        fn swapInUnchecked(self: *const Self, pager: Pager, idx: usize) !*T {
            if (self.small_vec) |vec| {
                return &vec.items[idx];
            } else {
                const page_idx = idx / self.per_page;
                const bytes = try pager.swapIn(self.pages.items[page_idx]);
                const page_slice = std.mem.bytesAsSlice(T, bytes[0..((bytes.len / @sizeOf(T)) * @sizeOf(T))]);
                return &page_slice[idx - (page_idx * self.per_page)];
            }
        }

        pub fn swapOut(self: *const Self, pager: Pager, idx: usize) void {
            if (idx >= self.len) @panic("out of bounds");
            self.swapOutUnchecked(pager, idx);
        }

        fn swapOutUnchecked(self: *const Self, pager: Pager, idx: usize) void {
            if (self.small_vec == null) {
                const page_idx = idx / self.per_page;
                pager.swapOut(self.pages.items[page_idx]);
            }
        }

        pub fn set(self: *const Self, pager: Pager, idx: usize, item: T) !void {
            if (idx >= self.len) @panic("out of bounds bich");
            var ptr = try self.swapIn(pager, idx);
            defer self.swapOut(pager, idx);
            ptr.* = item;
        }

        pub fn pop(self: *Self, pager: Pager) !T {
            if (self.len == 0) @panic("iss empty");
            const idx = self.len - 1;
            var item = (try self.swapIn(pager, idx)).*;
            defer self.swapOut(pager, idx);
            self.len -= 1;
            return item;
        }

        pub fn append(self: *Self, ha7r: Allocator, pager: Pager, item: T) !void {
            try self.ensureUnusedCapacity(ha7r, pager, 1);
            const idx = self.len;
            var ptr = try self.swapInUnchecked(pager, idx);
            defer self.swapOut(pager, idx);
            ptr.* = item;
            self.len += 1;
        }

        const Iterator = struct {
            cur: usize,
            stop: usize,
            pager: Pager,
            list: *const Self,
            in_idx: ?usize = null,

            pub fn new(list: *const Self, from: usize, to: usize, pager: Pager) Iterator {
                return Iterator{
                    .cur = from,
                    .stop = to,
                    .list = list,
                    .pager = pager,
                };
            }

            pub fn close(self: *Iterator) void {
                self.swapOutResident();
            }

            fn swapOutResident(self: *Iterator) void {
                if (self.in_idx) |idx| {
                    self.list.swapOut(self.pager, idx);
                    self.in_idx = null;
                }
            }


            pub fn next(self: *Iterator) !?*T {
                if (self.cur == self.stop) {
                    return null;
                }
                self.swapOutResident();
                var ptr = try self.list.swapIn(self.pager, self.cur);
                self.in_idx = self.cur;
                self.cur += 1;
                return ptr;
            }
        };

        /// Might panic if the list is modified before the iterator is closed.
        /// Be sure to close the iterator after usage.
        /// This reuses the list's built in pager.
        pub fn iterator(self: *const Self, pager: Pager) Iterator {
            // we give it the backing pager since we don't want it sharing our
            // single page cache in case of simultaneous usage
            return Iterator.new(self, 0, self.len, pager);
        }
    };
}

test "SwappingList.usage" {
    const page_size = std.mem.page_size;
    const ha7r = std.testing.allocator;
    var mmap_pager = try MmapPager.init(ha7r, "/tmp/comb.test.SwappingList.usage", .{
        .page_size = page_size,
    });
    defer mmap_pager.deinit();

    var lru = try LRUSwapCache.init(ha7r, mmap_pager.pager(), 1);
    defer lru.deinit();

    var pager = lru.pager();

    var list = SwappingList(usize).init(pager.pageSize());
    defer list.deinit(ha7r, pager);

    const item_per_page = page_size / @sizeOf(usize);
    const page_count = 10;
    var nums = [_]usize{0} ** (page_count * item_per_page);
    for (nums) |*num, ii| {
        num.* = ii;
        try list.append(ha7r, pager, ii);
    }
    {
        var it = list.iterator(pager);
        defer it.close();
        var ii: usize = 0;
        while (try it.next()) |num| {
            try std.testing.expectEqual(ii, num.*);
            ii += 1;
        }
    }
}
