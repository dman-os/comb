//! TODO: remove dependence on heap allocator
//! FIXME: none of these are threadsafe
//! FIXME: this is too complex. Pager + SwapAllocator combo seem superfluous.
//! -- If we were distentangle `SwapList` and `Pager`, it being the primary
//!    consumer of most of these types, we can make the latter private.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

pub const mod_utils = @import("utils.zig");
const println = mod_utils.println;
const dbg = mod_utils.println;

const mmap_align = std.mem.page_size;

/// Fat pointer dynamic dispatch interface in the vein of `std.mem.Allocator`
/// for page sized memory blocks with swapping support.
pub const Pager = struct {
    const Self = @This();
    pub const Error = AllocError || SwapInError;

    pub const AllocError = error{
        FileTooBig,
        FileBusy,
        InputOutput,
    } || Allocator.Error || std.os.UnexpectedError;

    pub const SwapInError = error{
        OutOfMemory,
        KernelResourceExhausted,
    } || std.os.UnexpectedError;

    pub const PageNo = u32;
    pub const PageSlice = []align(mmap_align) u8;

    pub const BlockId = packed struct {
        idx: u32,
        len: u32,
    };

    const VTable = struct {
        alloc: *const fn (self: *anyopaque) AllocError!Pager.PageNo,
        allocBlock: *const fn (self: *anyopaque, len: u32) AllocError!Pager.BlockId,
        free: *const fn (self: *anyopaque, no: Pager.PageNo) void,
        freeBlock: *const fn (self: *anyopaque, id: Pager.BlockId) void,
        swapIn: *const fn (self: *anyopaque, no: Pager.PageNo) SwapInError!Pager.PageSlice,
        swapInBlock: *const fn (self: *anyopaque, id: Pager.BlockId) SwapInError!Pager.PageSlice,
        swapOut: *const fn (self: *anyopaque, no: Pager.PageNo) void,
        swapOutBlock: *const fn (self: *anyopaque, id: Pager.BlockId) void,
        isSwappedIn: *const fn (self: *const anyopaque, no: Pager.PageNo) bool,
        pageSize: *const fn (self: *const anyopaque) usize,
    };

    ptr: *anyopaque,
    vtable: *const VTable,

    pub inline fn alloc(self: Self) AllocError!Pager.PageNo {
        return self.vtable.alloc(self.ptr);
    }

    pub inline fn allocBlock(self: Self, len: u32) AllocError!Pager.BlockId {
        return self.vtable.allocBlock(self.ptr, len);
    }

    /// This doesn't swap out the page.
    pub inline fn free(self: Self, no: Pager.PageNo) void {
        return self.vtable.free(self.ptr, no);
    }

    pub inline fn freeBlock(self: Self, id: Pager.BlockId) void {
        return self.vtable.freeBlock(self.ptr, id);
    }

    pub inline fn swapIn(self: Self, no: Pager.PageNo) SwapInError!Pager.PageSlice {
        return self.vtable.swapIn(self.ptr, no);
    }

    pub inline fn swapInBlock(self: Self, id: Pager.BlockId) SwapInError!Pager.PageSlice {
        return self.vtable.swapInBlock(self.ptr, id);
    }

    pub inline fn isSwappedIn(self: Self, no: Pager.PageNo) bool {
        return self.vtable.isSwappedIn(self.ptr, no);
    }

    pub inline fn swapOut(self: Self, no: Pager.PageNo) void {
        return self.vtable.swapOut(self.ptr, no);
    }

    pub inline fn swapOutBlock(self: Self, id: Pager.BlockId) void {
        return self.vtable.swapOutBlock(self.ptr, id);
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
        comptime allocFn: *const fn (self: @TypeOf(pointer)) AllocError!Pager.PageNo,
        comptime allocBlockFn: *const fn (self: @TypeOf(pointer), len: u32) AllocError!Pager.BlockId,
        comptime freeFn: *const fn (self: @TypeOf(pointer), no: Pager.PageNo) void,
        comptime freeBlockFn: *const fn (self: @TypeOf(pointer), no: Pager.BlockId) void,
        comptime swapInFn: *const fn (self: @TypeOf(pointer), no: Pager.PageNo) SwapInError!Pager.PageSlice,
        comptime swapInBlockFn: *const fn (self: @TypeOf(pointer), id: Pager.BlockId) SwapInError!Pager.PageSlice,
        comptime swapOutFn: *const fn (self: @TypeOf(pointer), no: Pager.PageNo) void,
        comptime swapOutBlockFn: *const fn (self: @TypeOf(pointer), id: Pager.BlockId) void,
        comptime pageSizeFn: *const fn (self: @TypeOf(const_ptr)) usize,
        comptime isSwappedInFn: *const fn (self: @TypeOf(const_ptr), no: Pager.PageNo) bool,
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
                const self = @as(PagerPtr, @ptrCast(@as(*align(alignment) anyopaque, @alignCast(alloc_ptr))));
                return @call(.always_inline, allocFn, .{
                    self,
                });
            }

            fn allocBlockImpl(alloc_ptr: *anyopaque, len: u32) AllocError!Pager.BlockId {
                const self = @as(PagerPtr, @ptrCast(@as(*align(alignment) anyopaque, @alignCast(alloc_ptr))));
                return @call(.always_inline, allocBlockFn, .{ self, len });
            }

            fn swapInImpl(alloc_ptr: *anyopaque, no: Pager.PageNo) SwapInError!Pager.PageSlice {
                const self = @as(PagerPtr, @ptrCast(@as(*align(alignment) anyopaque, @alignCast(alloc_ptr))));
                return @call(.always_inline, swapInFn, .{ self, no });
            }

            fn swapInBlockImpl(alloc_ptr: *anyopaque, id: Pager.BlockId) SwapInError!Pager.PageSlice {
                const self = @as(PagerPtr, @ptrCast(@as(*align(alignment) anyopaque, @alignCast(alloc_ptr))));
                return @call(.always_inline, swapInBlockFn, .{ self, id });
            }

            fn swapOutImpl(alloc_ptr: *anyopaque, no: Pager.PageNo) void {
                const self = @as(PagerPtr, @ptrCast(@as(*align(alignment) anyopaque, @alignCast(alloc_ptr))));
                return @call(.always_inline, swapOutFn, .{ self, no });
            }

            fn swapOutBlockImpl(alloc_ptr: *anyopaque, id: Pager.BlockId) void {
                const self = @as(PagerPtr, @ptrCast(@as(*align(alignment) anyopaque, @alignCast(alloc_ptr))));
                return @call(.always_inline, swapOutBlockFn, .{ self, id });
            }

            fn freeImpl(alloc_ptr: *anyopaque, no: Pager.PageNo) void {
                const self = @as(PagerPtr, @ptrCast(@as(*align(alignment) anyopaque, @alignCast(alloc_ptr))));
                return @call(.always_inline, freeFn, .{ self, no });
            }

            fn freeBlockImpl(alloc_ptr: *anyopaque, id: Pager.BlockId) void {
                const self = @as(PagerPtr, @ptrCast(@as(*align(alignment) anyopaque, @alignCast(alloc_ptr))));
                return @call(.always_inline, freeBlockFn, .{ self, id });
            }

            fn isSwappedInImpl(alloc_ptr: *const anyopaque, no: Pager.PageNo) bool {
                const self = @as(*const PagerPtr, @ptrCast(&@as(*align(alignment) const anyopaque, @alignCast(alloc_ptr)))).*;
                return @call(.always_inline, isSwappedInFn, .{ self, no });
            }

            fn pageSizeImpl(alloc_ptr: *const anyopaque) usize {
                const self = @as(*const PagerPtr, @ptrCast(&@as(*align(alignment) const anyopaque, @alignCast(alloc_ptr)))).*;
                return @call(.always_inline, pageSizeFn, .{self});
            }

            const vtable = VTable{
                .alloc = allocImpl,
                .allocBlock = allocBlockImpl,
                .free = freeImpl,
                .freeBlock = freeBlockImpl,
                .swapIn = swapInImpl,
                .swapInBlock = swapInBlockImpl,
                .swapOut = swapOutImpl,
                .swapOutBlock = swapOutBlockImpl,
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
const BlockId = Pager.BlockId;

/// Implements the `Pager` interface using `mmap` over a backing file.
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

    const PageBlocks = std.AutoHashMapUnmanaged(
        u32,
        std.AutoHashMapUnmanaged(PageNo, ?PageSlice),
    );

    const FreeList = std.AutoHashMapUnmanaged(
        u32,
        std.AutoHashMapUnmanaged(PageNo, void),
    );

    config: Config,
    a7r: Allocator,

    backing_file: std.fs.File,
    pages: std.ArrayListUnmanaged(Page),
    blocks: PageBlocks,
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
            .blocks = PageBlocks{},
            .free_list = FreeList{},
        };
        return self;
    }

    pub fn deinit(self: *Self) void {
        if (self.config.deinit_clean_up_file) {
            if (mod_utils.fdPath(self.backing_file.handle)) |actual_path| {
                self.backing_file.close();
                std.fs.deleteFileAbsolute(actual_path) catch |err| {
                    std.log.warn("error deleting file  when trying to remove mmap file: {}", .{err});
                };
            } else |err| {
                std.log.warn("trouble with readlink when trying to remove mmap file: {}", .{err});
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
            std.log.warn("{} allocated pages at deinit of " ++ @typeName(Self), .{active_count});
        }
        if (swapped_in_count > 0) {
            if (self.config.deinit_leak_swapped_in_pages) {
                std.log.warn("leaked {} swapped in pages at deinit of " ++ @typeName(Self), .{swapped_in_count});
            } else {
                std.log.warn("swapped out {} pages that were swapped in and in use at deinit of " ++ @typeName(Self), .{swapped_in_count});
            }
        }
        self.pages.deinit(self.a7r);
        {
            var it = self.free_list.valueIterator();
            while (it.next()) |list| {
                list.deinit(self.a7r);
            }
        }
        self.free_list.deinit(self.a7r);
        {
            var it = self.blocks.valueIterator();
            while (it.next()) |list| {
                list.deinit(self.a7r);
            }
        }
        self.blocks.deinit(self.a7r);
    }

    pub fn pager(self: *Self) Pager {
        return Pager.init(
            self,
            @as(*const Self, self),
            alloc,
            allocBlock,
            free,
            freeBlock,
            swapIn,
            swapInBlock,
            swapOut,
            swapOutBlock,
            pageSize,
            isSwappedIn,
        );
    }

    pub fn pageSize(self: *const Self) usize {
        return self.config.page_size;
    }

    fn markBlockFree(self: *Self, id: BlockId) !void {
        // println("goo: id = {}", .{ id });
        // defer println("gaa", .{});
        for (self.pages.items[id.idx..(id.idx + id.len)]) |*page| {
            page.free = true;
        }
        // TODO: if we encounter the end of the file, truncate it to smaller size
        var block_len = blk: {
            // look for free blocks to the right and find out their len
            var right_free_len: u32 = 0;
            var ii: u32 = id.idx + id.len;
            while (ii < self.pages.items.len and
                self.pages.items[ii].free)
            {
                ii += 1;
                right_free_len += 1;
            }
            // if there are free blocks to the right
            // combine them into the new block
            if (right_free_len > 0) {
                // println(
                //     "idx == {}, right_free_len = {}, free_classes = {}",
                //     .{ id.idx, right_free_len, self.free_list.count() }
                // );
                // println(
                //     "is {} actually free = {}",
                //     .{ ii - 1, self.pages.items[ii - 1] }
                // );
                var list = self.free_list.get(right_free_len).?;
                // remove it from the free list
                std.debug.assert(list.remove(id.idx + 1));
                if (list.count() == 0) {
                    _ = self.free_list.remove(right_free_len);
                    list.deinit(self.a7r);
                }
            }
            break :blk id.len + right_free_len;
        };
        const start = blk: {
            // look for free blocks to the right and find out their len
            var left_free_len: u32 = 0;
            var ii: u32 = id.idx;
            while (ii > 0) {
                ii -= 1;
                if (!self.pages.items[ii].free) break;
                left_free_len += 1;
            }
            // if there are free blocks to the right
            // combine them into a larger block
            if (left_free_len > 0) {
                var list = self.free_list.get(left_free_len).?;
                // remove it from the free list
                std.debug.assert(list.remove(id.idx - left_free_len));
                if (list.count() == 0) {
                    _ = self.free_list.remove(left_free_len);
                    list.deinit(self.a7r);
                }
            }
            block_len += left_free_len;
            break :blk id.idx - left_free_len;
        };
        var list = blk: {
            var res = try self.free_list.getOrPut(
                self.a7r,
                block_len,
            );
            if (!res.found_existing) {
                res.value_ptr.* = std.AutoHashMapUnmanaged(PageNo, void){};
            }
            break :blk res.value_ptr;
        };
        // println("putting in free block of len {} at slot {}", .{ block_len, start });
        try list.put(self.a7r, start, {});
    }

    fn allocFreeBlock(self: *Self, len: u32) !?BlockId {
        // println("joo: len = {}", .{ len });
        // defer println("jaa", .{});
        var min_len: u32 = std.math.maxInt(u32);
        var min_len_list: ?*std.AutoHashMapUnmanaged(PageNo, void) = null;
        {
            var it = self.free_list.iterator();
            while (it.next()) |pair| {
                const block_len = pair.key_ptr.*;
                if (block_len < len) continue;
                if (block_len < min_len) {
                    min_len = block_len;
                    min_len_list = pair.value_ptr;
                    if (block_len == len) break;
                }
            }
        }
        if (min_len_list) |list| {
            const start = blk: {
                // HashMaps don't have `pop()`
                // so do this whole fuckin thing
                var it = list.iterator();
                const pair = it.next().?;
                const no = pair.key_ptr.*;
                list.removeByPtr(pair.key_ptr);
                break :blk no;
            };
            if (list.count() == 0) {
                list.deinit(self.a7r);
                _ = self.free_list.remove(min_len);
            }
            const left = min_len - len;
            if (left > 0) {
                try self.markBlockFree(BlockId{ .idx = start + len, .len = left });
            }
            for (self.pages.items[start..(start + len)]) |*page| {
                page.free = false;
            }
            // println("reallocing block at {} with len {}", .{ start, len });
            return BlockId{
                .idx = start,
                .len = len,
            };
        } else {
            return null;
        }
    }

    pub fn alloc(self: *Self) Pager.AllocError!PageNo {
        const block = try self.allocBlock(1);
        return block.idx;
    }

    pub fn allocBlock(self: *Self, len: u32) Pager.AllocError!BlockId {
        // println("koo: len = {}", .{ len });
        // defer println("kaa", .{});
        const block_len = len;
        if (try self.allocFreeBlock(block_len)) |id| {
            var set = blk: {
                const pair = try self.blocks.getOrPutValue(
                    self.a7r,
                    id.len,
                    std.AutoHashMapUnmanaged(PageNo, ?PageSlice){},
                );
                break :blk pair.value_ptr;
            };
            try set.put(self.a7r, id.idx, null);
            return id;
        } else {
            // ensure capacity before trying anything
            try self.pages.ensureUnusedCapacity(self.a7r, block_len);
            // // ensure capacity on the free list to avoid error checking on free calls
            // try self.free_list.ensureUnusedCapacity(block_len);
            std.os.ftruncate(self.backing_file.handle, (self.pages.items.len + block_len) * self.config.page_size) catch |err| return switch (err) {
                std.os.TruncateError.AccessDenied => unreachable,
                std.os.TruncateError.FileTooBig => error.FileTooBig,
                std.os.TruncateError.InputOutput => error.InputOutput,
                std.os.TruncateError.FileBusy => error.FileBusy,
                std.os.UnexpectedError.Unexpected => std.os.UnexpectedError.Unexpected,
            };
            var ii: usize = 0;
            while (ii < block_len) : ({
                ii += 1;
            }) {
                try self.pages.append(self.a7r, Page{
                    .slice = null,
                    .free = false,
                });
            }
            var set = b: {
                const pair = try self.blocks.getOrPutValue(
                    self.a7r,
                    block_len,
                    std.AutoHashMapUnmanaged(PageNo, ?PageSlice){},
                );
                break :b pair.value_ptr;
            };
            const idx = @as(u32, @intCast(self.pages.items.len)) - block_len;
            try set.put(self.a7r, idx, null);
            return BlockId{
                .idx = idx,
                .len = block_len,
            };
        }
    }

    pub fn freeBlock(self: *Self, id: BlockId) void {
        if (self.blocks.getPtr(id.len)) |set| {
            if (set.fetchRemove(id.idx)) |pair| {
                if (pair.value != null) {
                    std.log.warn("page {} was freed while swapped in", .{id});
                    @panic("ehh");
                }
                self.markBlockFree(id) catch @panic("oom");
            } else {
                @panic("double free");
            }
        } else {
            @panic("double free");
        }
    }

    /// This doesn't swap out the page.
    pub fn free(self: *Self, no: PageNo) void {
        self.freeBlock(BlockId{ .idx = no, .len = 1 });
        // if (no == self.pages.items.len - 1) {
        //     var ii = no;
        //     while (true) : ({
        //         ii -= 1;
        //     }) {
        //         if (ii == 0 or !(self.pages.items[ii].free)) break;
        //         _ = self.pages.popOrNull();
        //     }
        // } else {
        //     // we've been growing the free list along the page list so it's ok
        //     self.free_list.add(no) catch unreachable;
        // }
    }

    pub fn swapInBlock(self: *Self, id: BlockId) Pager.SwapInError!PageSlice {
        if (self.blocks.get(id.len)) |*set| {
            if (set.getPtr(id.idx)) |opt| {
                if (opt.*) |slice| {
                    return slice;
                } else {
                    var slice = std.os.mmap(
                        null,
                        self.config.page_size * id.len,
                        std.os.PROT.READ | std.os.PROT.WRITE,
                        std.os.MAP.SHARED, // | std.os.MAP.ANONYMOUS,
                        self.backing_file.handle,
                        // -1,
                        id.idx * self.config.page_size,
                    ) catch |err| return switch (err) {
                        std.os.MMapError.MemoryMappingNotSupported => @panic("system doesn't support mmaping"),
                        std.os.MMapError.AccessDenied => unreachable,
                        std.os.MMapError.PermissionDenied => unreachable,
                        std.os.MMapError.LockedMemoryLimitExceeded, std.os.MMapError.ProcessFdQuotaExceeded, std.os.MMapError.SystemFdQuotaExceeded => error.KernelResourceExhausted,
                        std.os.MMapError.OutOfMemory => error.OutOfMemory,
                        std.os.UnexpectedError.Unexpected => std.os.UnexpectedError.Unexpected,
                    };
                    errdefer std.os.munmap(slice);
                    opt.* = slice;
                    return slice;
                }
            } else {
                @panic("Block not found");
            }
        } else {
            @panic("Block not found");
        }
    }

    pub fn swapOutBlock(self: *Self, id: BlockId) void {
        if (self.blocks.get(id.len)) |*set| {
            if (set.getPtr(id.idx)) |opt| {
                if (opt.*) |slice| {
                    std.os.munmap(slice);
                    opt.* = null;
                } else {
                    std.log.warn(@typeName(Self) ++ " received swapOut for page not in memory at no {}", .{id});
                    @panic("eawoo");
                }
            } else {
                @panic("Block not found");
            }
        } else {
            @panic("Block not found");
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
            var slice = std.os.mmap(
                null,
                self.config.page_size,
                std.os.PROT.READ | std.os.PROT.WRITE,
                std.os.MAP.SHARED, // | std.os.MAP.ANONYMOUS,
                self.backing_file.handle,
                // -1,
                no * self.config.page_size,
            ) catch |err| return switch (err) {
                std.os.MMapError.MemoryMappingNotSupported => @panic("system doesn't support mmaping"),
                std.os.MMapError.AccessDenied => unreachable,
                std.os.MMapError.PermissionDenied => unreachable,
                std.os.MMapError.LockedMemoryLimitExceeded, std.os.MMapError.ProcessFdQuotaExceeded, std.os.MMapError.SystemFdQuotaExceeded => error.KernelResourceExhausted,
                std.os.MMapError.OutOfMemory => error.OutOfMemory,
                std.os.UnexpectedError.Unexpected => std.os.UnexpectedError.Unexpected,
            };
            errdefer std.os.munmap(slice);
            page.slice = slice;
            return slice;
        }
    }

    pub fn swapOut(self: *Self, no: PageNo) void {
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
    for (&nums, 0..) |*num, ii| {
        num.* = ii;
    }
    var pages: [page_count]PageNo = undefined;
    for (&pages, 0..) |*no, pii| {
        no.* = try pager.alloc();
        var bytes = try pager.swapIn(no.*);
        defer pager.swapOut(no.*);
        for (std.mem.bytesAsSlice(usize, bytes), 0..) |*num, ii| {
            num.* = nums[(pii * item_per_page) + ii];
        }
    }
    defer for (pages) |no| {
        pager.free(no);
    };

    for (pages, 0..) |no, pii| {
        var bytes = try pager.swapIn(no);
        defer pager.swapOut(no);
        for (std.mem.bytesAsSlice(usize, bytes), 0..) |num, ii| {
            try std.testing.expectEqual(nums[(pii * item_per_page) + ii], num);
        }
    }
    {
        const page_no = page_count / 2;
        pager.free(pages[page_no]);
        // TODO: find a way to test panics?
        // try std.testing.expectError(error.PageNotAllocated, pager.swapIn(pages[page_no]));

        var new_stuff = [_]usize{0} ** item_per_page;
        for (&new_stuff, 0..) |*num, ii| {
            num.* = item_per_page - ii;
            nums[(page_no * item_per_page) + ii] = item_per_page - ii;
        }

        var no = try pager.alloc();
        var bytes = try pager.swapIn(no);
        defer pager.swapOut(no);
        for (std.mem.bytesAsSlice(usize, bytes), 0..) |*num, ii| {
            num.* = new_stuff[ii];
        }
    }
    for (pages, 0..) |no, pii| {
        var bytes = try pager.swapIn(no);
        defer pager.swapOut(no);
        for (std.mem.bytesAsSlice(usize, bytes), 0..) |num, ii| {
            try std.testing.expectEqual(nums[(pii * item_per_page) + ii], num);
        }
    }
    {
        const block_id = try pager.allocBlock(page_count);
        defer pager.freeBlock(block_id);
        {
            const slice = try pager.swapInBlock(block_id);
            defer pager.swapOutBlock(block_id);
            @memcpy(slice, std.mem.sliceAsBytes(&nums));
        }
        {
            const slice = try pager.swapInBlock(block_id);
            defer pager.swapOutBlock(block_id);
            // FIXME: this failed on my machine with the following:
            //    slices differ. first difference occurs at index 36968 (0x9068)
            // though I was unable to recreate it. Cosmic rays?
            try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(&nums), slice);
        }
    }
}

/// Wraps and provides the `Pager` interface with a lru caching for that swapping action.
/// Mainly avoids unessary syscalls and double map of the same page
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
            var self = ColdList{
                .vec = Vec{},
                .map = Map{},
                .que = Que{},
                .free_list = std.ArrayListUnmanaged(usize){},
            };
            errdefer self.deinit(a7r);
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
        var self = Self{
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
            std.log.warn("{} hot pages in " ++ @typeName(Self) ++ " at time of deinit", .{self.hot.count()});
        }
        var it = self.cold.map.keyIterator();
        while (it.next()) |no| {
            self.backing_pager.swapOut(no.*);
        }
        self.hot.deinit(self.a7r);
        self.cold.deinit(self.a7r);
    }

    pub fn hotCount(self: *const Self) u32 {
        return self.hot.count();
    }

    pub fn coldCount(self: *const Self) u32 {
        return self.cold.map.count();
    }

    pub fn pager(self: *Self) Pager {
        return Pager.init(
            self,
            @as(*const Self, self),
            alloc,
            allocBlock,
            free,
            freeBlock,
            swapIn,
            swapInBlock,
            swapOut,
            swapOutBlock,
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

    pub fn allocBlock(self: *Self, len: u32) Pager.AllocError!Pager.BlockId {
        return self.backing_pager.allocBlock(len);
    }

    pub fn freeBlock(self: *Self, id: Pager.BlockId) void {
        return self.backing_pager.freeBlock(id);
    }

    pub fn swapInBlock(self: *Self, id: Pager.BlockId) Pager.SwapInError!Pager.PageSlice {
        return self.backing_pager.swapInBlock(id);
    }

    pub fn swapOutBlock(self: *Self, id: Pager.BlockId) void {
        return self.backing_pager.swapOutBlock(id);
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
            std.debug.assert((try self.hot.fetchPut(self.a7r, no, HotPage{ .slice = slice, .swap_ins = 1 })) == null);
            self.cold.que.remove(&self.cold.vec.items[idx]);
            self.cold.map.removeByPtr(pair.key_ptr);
            self.cold.free_list.append(self.a7r, idx) catch @panic("impossible");
            return slice;
        } else {
            // println("swappingIn fresh page {}", .{ no });
            const slice = try self.backing_pager.swapIn(no);
            errdefer self.backing_pager.swapOut(no);
            std.debug.assert((try self.hot.fetchPut(self.a7r, no, HotPage{ .slice = slice, .swap_ins = 1 })) == null);
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
                    self.cold.vec.items[idx] = ColdList.Que.Node{
                        .data = ColdPage{
                            .no = no,
                            .last_used = self.timer.read(),
                            .slice = slice,
                        },
                    };
                    break :blk idx;
                } else blk: {
                    self.cold.vec.append(self.a7r, ColdList.Que.Node{
                        .data = ColdPage{
                            .no = no,
                            .last_used = self.timer.read(),
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
            std.log.warn("unrecognized page no swapped Out through " ++
                @typeName(Self) ++
                ": you're prolly misusing the cache", .{});
            @panic("eawoo");
        }
    }

    pub fn isSwappedIn(self: *const Self, no: PageNo) bool {
        return self.hot.contains(no);
    }
};

pub const SinglePageCache = struct {
    const Self = @This();

    const InPage = struct {
        no: PageNo,
        slice: []align(mmap_align) u8,
    };

    backing_pager: Pager,
    in_page: ?InPage = null,

    pub fn init(backing_pager: Pager) Self {
        return Self{ .in_page = null, .backing_pager = backing_pager };
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
                std.log.warn("unrecognized page no freed through " ++
                    @typeName(SinglePageCache) ++
                    ": you're prolly misusing the cache", .{});
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
                self.in_page = InPage{
                    .no = no,
                    .slice = slice,
                };
                return slice;
            }
        } else {
            // println("swapping in page {} and adding to cache", .{no});
            const slice = try self.backing_pager.swapIn(no);
            self.in_page = InPage{
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
            } else {
                std.log.warn("unrecognized page no swapped Out through " ++
                    @typeName(SinglePageCache) ++
                    ": you're prolly misusing the cache", .{});
            }
        } else {
            std.log.warn("page swapped Out through an empty " ++
                @typeName(SinglePageCache) ++
                ": you're prolly misusing the cache", .{});
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

/// Another fat pointer interface in the vein of `Pager` allowing arbitrary length
/// allocations and making use of a `Ptr` ptrs instead of the language builtin ptrs.
pub const SwapAllocator = struct {
    pub const Ptr = usize;
    // TODO: clean me up
    pub const AllocError = Pager.AllocError;
    pub const SwapInError = Pager.SwapInError;
    pub const Error = AllocError || SwapInError;

    const VTable = struct {
        alloc: *const fn (self: *anyopaque, len: usize) AllocError!Ptr,
        swapIn: *const fn (self: *anyopaque, ptr: Ptr) SwapInError![]u8,
        swapOut: *const fn (self: *anyopaque, ptr: Ptr) void,
        free: *const fn (self: *anyopaque, ptr: Ptr) void,
    };

    ptr: *anyopaque,
    vtable: *const VTable,

    pub fn alloc(self: SwapAllocator, len: usize) AllocError!Ptr {
        return self.vtable.alloc(self.ptr, len);
    }

    pub fn swapIn(self: SwapAllocator, ptr: Ptr) SwapInError![]u8 {
        return self.vtable.swapIn(self.ptr, ptr);
    }

    pub fn swapOut(self: SwapAllocator, ptr: Ptr) void {
        return self.vtable.swapOut(self.ptr, ptr);
    }

    pub fn free(self: SwapAllocator, ptr: Ptr) void {
        return self.vtable.free(self.ptr, ptr);
    }

    pub const DupeRes = struct {
        ptr: Ptr,
        slice: []u8,
    };

    /// Don't forget to swap out the ptr if you don't need the slice.
    pub fn dupe(self: SwapAllocator, bytes: []const u8) Error!DupeRes {
        const ptr = try self.alloc(bytes.len);
        var slice = try self.swapIn(ptr);
        @memcpy(slice, bytes);
        return DupeRes{
            .ptr = ptr,
            .slice = slice,
        };
    }

    pub fn dupeJustPtr(self: SwapAllocator, bytes: []const u8) Error!Ptr {
        const ptr = try self.alloc(bytes.len);
        var slice = try self.swapIn(ptr);
        defer self.swapOut(ptr);
        @memcpy(slice, bytes);
        return ptr;
    }

    /// Modified from std.mem.Allocator
    pub fn init(
        pointer: anytype,
        comptime allocFn: fn (ptr: @TypeOf(pointer), len: usize) AllocError!Ptr,
        comptime swapInFn: fn (ptr: @TypeOf(pointer), ptr: Ptr) SwapInError![]u8,
        comptime swapOutFn: fn (ptr: @TypeOf(pointer), ptr: Ptr) void,
        comptime freeFn: fn (ptr: @TypeOf(pointer), ptr: Ptr) void,
    ) SwapAllocator {
        const AllocPtr = @TypeOf(pointer);
        const ptr_info = @typeInfo(AllocPtr);
        std.debug.assert(ptr_info == .Pointer); // Must be a pointer
        std.debug.assert(ptr_info.Pointer.size == .One); // Must be a single-item pointer

        const alignment = ptr_info.Pointer.alignment;

        const gen = struct {
            fn allocImpl(alloc_ptr: *anyopaque, len: usize) AllocError!Ptr {
                const self = @as(AllocPtr, @ptrCast(@as(*align(alignment) anyopaque, @alignCast(alloc_ptr))));
                return @call(.always_inline, allocFn, .{
                    self,
                    len,
                });
            }

            fn swapInImpl(alloc_ptr: *anyopaque, ptr: Ptr) SwapInError![]u8 {
                const self = @as(AllocPtr, @ptrCast(@as(*align(alignment) anyopaque, @alignCast(alloc_ptr))));
                return @call(.always_inline, swapInFn, .{
                    self,
                    ptr,
                });
            }

            fn swapOutImpl(alloc_ptr: *anyopaque, ptr: Ptr) void {
                const self = @as(AllocPtr, @ptrCast(@as(*align(alignment) anyopaque, @alignCast(alloc_ptr))));
                return @call(.always_inline, swapOutFn, .{
                    self,
                    ptr,
                });
            }

            fn freeImpl(alloc_ptr: *anyopaque, ptr: Ptr) void {
                const self = @as(AllocPtr, @ptrCast(@as(*align(alignment) anyopaque, @alignCast(alloc_ptr))));
                return @call(.always_inline, freeFn, .{
                    self,
                    ptr,
                });
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

pub const PagingSwapAllocatorConfig = struct {
    page_size: usize = std.mem.page_size,
};

/// Implements the `SwapAllocator` interface using a `Pager` and a similar
/// bucketing approach as the `std.mem.GeneralPurposeAllocator`.
pub fn PagingSwapAllocator(comptime config: PagingSwapAllocatorConfig) type {
    return struct {
        const Self = @This();

        const bucket_count = std.math.log2(config.page_size);
        const BuckIndex = std.meta.Int(.unsigned, std.math.log2(std.math.ceilPowerOfTwoAssert(u16, bucket_count + 1)));
        const SlotIndex = std.meta.Int(.unsigned, @bitSizeOf(SwapAllocator.Ptr) - @bitSizeOf(BuckIndex));
        /// TODO: hate the name
        pub const CustomPtr = packed struct {
            buck: BuckIndex,
            slot: SlotIndex,

            fn toGpPtr(self: CustomPtr) SwapAllocator.Ptr {
                return @as(SwapAllocator.Ptr, @bitCast(self));
            }
            fn fromGpPtr(ptr: SwapAllocator.Ptr) CustomPtr {
                return @as(CustomPtr, @bitCast(ptr));
            }
        };

        comptime {
            // TODO: THiS gets fucked if usize bytes are above 255
            if (@sizeOf(CustomPtr) != @sizeOf(SwapAllocator.Ptr)) {
                @compileError(std.fmt.comptimePrint(@typeName(CustomPtr) ++ " size {} != " ++ @typeName(SwapAllocator.Ptr) ++ " {}", .{ @sizeOf(CustomPtr), @sizeOf(SwapAllocator.Ptr) }));
            }
            // we need this for the puprpose of working with BlockIds
            if (@sizeOf(SlotIndex) < @sizeOf(u32))
                @compileError(std.fmt.comptimePrint(@typeName(SlotIndex) ++ " size {} < " ++ @typeName(u32) ++ " {}", .{ @sizeOf(SlotIndex), @sizeOf(u32) }));
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
                    .free_list = FreeList.init(ha7r, {}),
                    // .class = class,
                    .per_page = per_page,
                };
            }

            fn deinit(self: *Bucket, sa6r: *Self) void {
                self.allocs.deinit(sa6r.ha7r);
                for (self.pages.items) |no| {
                    // since this bucket is exclusively using the page
                    // this should be safe from double swap out
                    if (sa6r.pager.isSwappedIn(no)) {
                        sa6r.pager.swapAndFree(no);
                    } else sa6r.pager.free(no);
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
                    try self.allocs.append(sa6r.ha7r, Allocation{
                        // .gen = 0,
                        .free = false,
                        .len = alloc_len,
                    });
                    return @as(SlotIndex, @intCast(self.allocs.items.len - 1));
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
                return page_slice[page_slot * size .. (page_slot * size) + meta.len];
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
                // FIXME: find a way to avoid this
                self.free_list.add(slot) catch @panic("so, this happened");
            }
        };

        const LargeAllocation = struct { block: BlockId, requested_size: usize };

        ha7r: Allocator,
        pager: Pager,

        buckets: [bucket_count]?Bucket = [_]?Bucket{null} ** bucket_count,
        large_allocs: std.AutoHashMapUnmanaged(SlotIndex, LargeAllocation) = .{},

        pub fn init(ha7r: Allocator, pager: Pager) Self {
            return .{
                .ha7r = ha7r,
                .pager = pager,
            };
        }

        pub fn deinit(self: *Self) void {
            for (&self.buckets) |*opt| {
                if (opt.*) |*buck| {
                    buck.deinit(self);
                }
            }
            self.large_allocs.deinit(self.ha7r);
        }

        pub fn allocator(self: *Self) SwapAllocator {
            return SwapAllocator.init(self, alloc, swapIn, swapOut, free);
        }

        fn buckAlloc(self: *Self, buck_idx: BuckIndex, len: usize) SwapAllocator.AllocError!SwapAllocator.Ptr {
            // println("allocating len {} in bucket of size {}", .{ len, @as(usize, 1) << buck_idx_fit });
            // println("buck for len {} == {}", .{ len, @as(usize, 1) << buck_idx_fit });
            if (self.buckets[buck_idx] == null) {
                const slot_size = @as(usize, 1) << buck_idx;
                self.buckets[buck_idx] = Bucket.init(
                    self.ha7r,
                    @divExact(config.page_size, slot_size),
                );
            }
            var buck = &(self.buckets[buck_idx].?);
            var slot = try buck.addOne(len, self);
            return (CustomPtr{ .buck = buck_idx, .slot = slot }).toGpPtr();
        }

        fn largeAlloc(self: *Self, len: usize) SwapAllocator.AllocError!SwapAllocator.Ptr {
            const page_count = std.math.divCeil(usize, len, config.page_size) catch @panic("overflow");
            const block_id = try self.pager.allocBlock(@as(u32, @intCast(page_count)));
            const slot = @as(SlotIndex, block_id.idx);
            try self.large_allocs.put(
                self.ha7r,
                slot,
                LargeAllocation{
                    .block = block_id,
                    .requested_size = len,
                },
            );
            // println(
            //     "allocated large alloc for len {} using {} pages to block id {}",
            //     .{ len, page_count, block_id}
            // );
            return (CustomPtr{ .buck = bucket_count, .slot = slot }).toGpPtr();
        }

        pub fn alloc(self: *Self, len: usize) SwapAllocator.AllocError!SwapAllocator.Ptr {
            const buck_idx = std.math.log2(std.math.ceilPowerOfTwoAssert(usize, len));
            if (buck_idx >= bucket_count) {
                // printn(
                //     "we got a big one boys. page_size = {}, len = {}, buck_idx = {}, buck_count = {}, idx type = {any}",
                //     .{ config.page_size, len, buck_idx, bucket_count, @typeInfo(BuckIndex) }
                // );
                return try self.largeAlloc(len);
            } else {
                const buck_idx_fit = @as(BuckIndex, @intCast(buck_idx));
                return try self.buckAlloc(buck_idx_fit, len);
            }
        }

        pub fn free(self: *Self, gpptr: SwapAllocator.Ptr) void {
            const ptr = CustomPtr.fromGpPtr(gpptr);
            const buck_idx = ptr.buck;
            if (buck_idx == bucket_count) {
                if (self.large_allocs.fetchRemove(ptr.slot)) |large| {
                    self.pager.freeBlock(large.value.block);
                } else {
                    @panic("double free: Ptr points to an unrecognized large allocation");
                }
            } else if (self.buckets[buck_idx]) |*buck| {
                buck.free(ptr.slot);
            } else {
                @panic("Ptr points to empty bucket");
            }
        }

        pub fn swapIn(self: *Self, gpptr: SwapAllocator.Ptr) SwapAllocator.SwapInError![]u8 {
            const ptr = CustomPtr.fromGpPtr(gpptr);
            const buck_idx = ptr.buck;
            if (buck_idx == bucket_count) {
                if (self.large_allocs.get(ptr.slot)) |large| {
                    const slice = try self.pager.swapInBlock(large.block);
                    return slice[0..large.requested_size];
                } else {
                    @panic("Ptr points to an unrecognized large allocation");
                }
            } else if (self.buckets[buck_idx]) |*buck| {
                return try buck.swapIn(ptr.slot, &self.pager);
            } else {
                @panic("Ptr points to empty bucket");
            }
        }

        pub fn swapOut(self: *Self, gpptr: SwapAllocator.Ptr) void {
            const ptr = CustomPtr.fromGpPtr(gpptr);
            const buck_idx = ptr.buck;
            if (buck_idx == bucket_count) {
                if (self.large_allocs.get(ptr.slot)) |large| {
                    self.pager.swapOutBlock(large.block);
                } else {
                    @panic("Ptr points to an unrecognized large allocation");
                }
            } else if (self.buckets[buck_idx]) |*buck| {
                buck.swapOut(ptr.slot, &self.pager);
            } else {
                @panic("Ptr points to empty bucket");
            }
        }
    };
}

test "PagingSwapAllocator.usage" {
    const page_size = std.mem.page_size;
    const ha7r = std.testing.allocator;
    var mmap_pager = try MmapPager.init(ha7r, "/tmp/comb.test.SwapAllocator.usage", .{
        .page_size = page_size,
    });
    defer mmap_pager.deinit();

    var lru = try LRUSwapCache.init(ha7r, mmap_pager.pager(), 1);
    defer lru.deinit();

    var ma7r = PagingSwapAllocator(.{}).init(ha7r, lru.pager());
    defer ma7r.deinit();

    var sa7r = ma7r.allocator();

    // std.debug.print("Size MPA: {}\n", .{@sizeOf(PagingSwapAllocator(.{ .page_size = 4 * 1024 }))});
    // std.debug.print("Size GPA: {}\n", .{@sizeOf(std.heap.GeneralPurposeAllocator(.{}))});

    const table = [_][]const u8{ "i wondered all night"[0..], "i wondered all night"[0..], "i wondered all night all about you"[0..], "i've been here for years just wondering around about the neighborhood"[0..], ([_]u8{42} ** (10 * page_size))[0..] };
    var ptrs: [table.len]SwapAllocator.Ptr = undefined;

    for (&table, 0..) |case, ii| {
        ptrs[ii] = try sa7r.alloc(case.len);
        var string = try sa7r.swapIn(ptrs[ii]);
        defer sa7r.swapOut(ptrs[ii]);
        @memcpy(string, case);
    }
    defer for (ptrs) |ptr| {
        sa7r.free(ptr);
    };

    // std.debug.print("Allocer\n{any}\n", .{ sa7r.buckets });
    for (&ptrs, 0..) |ptr, ii| {
        const string = try sa7r.swapIn(ptr);
        defer sa7r.swapOut(ptr);
        // println("{s} != {s}", .{ table[ii], string });
        try std.testing.expectEqualStrings(table[ii], string);
    }

    const replacement = "i wonder still oh I doo";
    {
        sa7r.free(ptrs[1]);
        ptrs[1] = try sa7r.alloc(replacement.len);
        var string = try sa7r.swapIn(ptrs[1]);
        defer sa7r.swapOut(ptrs[1]);
        @memcpy(string, replacement);
    }
    {
        const string = try sa7r.swapIn(ptrs[1]);
        defer sa7r.swapOut(ptrs[1]);
        try std.testing.expectEqualStrings(replacement, string);
    }
}

/// A general prurpos array list making use of pages from a `Pager`
/// but falling back to a `SwapAllocator` if size is still under a page.
/// This allows multiple lists sharing the same `SwapAllocator` to share a
/// page.
pub fn SwapList(comptime T: type) type {
    return struct {
        const Self = @This();

        const SmallList = struct {
            ptr: SwapAllocator.Ptr,
            len: usize,
        };

        // const PageHeader = struct {
        //     no: PageNo,
        //     next: PageNo,
        //     prev: PageNo,
        // };

        // const InPage = struct {
        //     idx: usize,
        //     no: PageNo,
        // };
        // in: ?InPage = null,

        /// items per page
        per_page: usize,

        len: usize = 0,
        /// The `SwapAllocator` allocation we make use until we overflow a single page.
        small_list: ?SmallList = null,
        pages: std.ArrayListUnmanaged(PageNo) = .{},

        pub fn init(page_size: usize) Self {
            return Self{
                .per_page = page_size / @sizeOf(T),
            };
        }

        pub fn deinit(self: *Self, ha7r: Allocator, sa7r: SwapAllocator, pager: Pager) void {
            for (self.pages.items) |no| {
                pager.free(no);
            }
            self.pages.deinit(ha7r);
            if (self.small_list) |*small| {
                sa7r.free(small.ptr);
                self.small_list = null;
            }
        }

        pub inline fn capacity(self: *const Self) usize {
            if (self.small_list) |small| {
                return small.len;
            } else {
                return self.pages.items.len * self.per_page;
            }
        }

        pub fn ensureUnusedCapacity(self: *Self, ha7r: Allocator, sa7r: SwapAllocator, pager: Pager, n_items: usize) !void {
            const cap = self.capacity();
            const desired_cap = self.len + n_items;
            if (cap >= desired_cap) {
                return;
            }
            const new_cap = if (cap == 0)
                std.math.ceilPowerOfTwoAssert(usize, n_items)
            else
                (cap * (std.math.divCeil(usize, desired_cap, cap) catch @panic("overflow")));
            if (new_cap < self.per_page) {
                const ptr = try sa7r.alloc(new_cap * @sizeOf(T));
                errdefer sa7r.free(ptr);
                if (self.small_list) |small| {
                    const old = try sa7r.swapIn(small.ptr);
                    defer {
                        sa7r.swapOut(small.ptr);
                        sa7r.free(small.ptr);
                    }
                    var new = try sa7r.swapIn(ptr);
                    defer sa7r.swapOut(ptr);
                    @memcpy(new[0..old.len], old);
                }
                self.small_list = SmallList{
                    .ptr = ptr,
                    .len = new_cap,
                };
            } else {
                var new_pages_req =
                    (std.math.divCeil(usize, new_cap, self.per_page) catch @panic("overflow")) - self.pages.items.len;
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
                if (self.small_list) |small| {
                    const old = try sa7r.swapIn(small.ptr);
                    defer {
                        sa7r.swapOut(small.ptr);
                        sa7r.free(small.ptr);
                    }
                    var new = try pager.swapIn(self.pages.items[0]);
                    defer pager.swapOut(self.pages.items[0]);
                    @memcpy(new[0..old.len], old);
                    self.small_list = null;
                }
                std.debug.assert(self.pages.items.len * self.per_page > desired_cap);
            }
        }

        pub fn swapIn(self: *const Self, sa7r: SwapAllocator, pager: Pager, idx: usize) !*T {
            if (idx >= self.len) @panic("out of bounds bich");
            return try self.swapInUnchecked(sa7r, pager, idx);
        }

        /// Does no bound checking. Use to access slots with-in capacity
        /// but not necessarily length. Use `swapIn` for safer alternative.
        fn swapInUnchecked(self: *const Self, sa7r: SwapAllocator, pager: Pager, idx: usize) !*T {
            if (self.small_list) |*small| {
                const bytes: []align(@alignOf(T)) u8 = @alignCast(try sa7r.swapIn(small.ptr));
                const page_slice = std.mem.bytesAsSlice(T, bytes[0..((bytes.len / @sizeOf(T)) * @sizeOf(T))]);
                return &page_slice[idx];
            } else {
                const page_idx = idx / self.per_page;
                const bytes = try pager.swapIn(self.pages.items[page_idx]);
                const page_slice = std.mem.bytesAsSlice(T, bytes[0..((bytes.len / @sizeOf(T)) * @sizeOf(T))]);
                return &page_slice[idx - (page_idx * self.per_page)];
            }
        }

        pub fn swapOut(self: *const Self, sa7r: SwapAllocator, pager: Pager, idx: usize) void {
            if (idx >= self.len) @panic("out of bounds");
            self.swapOutUnchecked(sa7r, pager, idx);
        }

        fn swapOutUnchecked(self: *const Self, sa7r: SwapAllocator, pager: Pager, idx: usize) void {
            if (self.small_list) |small| {
                sa7r.swapOut(small.ptr);
            } else {
                const page_idx = idx / self.per_page;
                pager.swapOut(self.pages.items[page_idx]);
            }
        }

        pub fn set(self: *const Self, sa7r: SwapAllocator, pager: Pager, idx: usize, item: T) !void {
            if (idx >= self.len) @panic("out of bounds bich");
            var ptr = try self.swapIn(sa7r, pager, idx);
            defer self.swapOut(sa7r, pager, idx);
            ptr.* = item;
        }

        pub fn pop(self: *Self, sa7r: SwapAllocator, pager: Pager) !T {
            if (self.len == 0) @panic("iss empty");
            const idx = self.len - 1;
            const item_ptr = (try self.swapIn(sa7r, pager, idx));
            // copy to the stack
            const item = item_ptr.*;
            // swap out before decreasing the length to avoid fouling up the
            // bound check
            self.swapOut(sa7r, pager, idx);
            self.len -= 1;
            return item;
        }

        pub fn append(self: *Self, ha7r: Allocator, sa7r: SwapAllocator, pager: Pager, item: T) !void {
            try self.ensureUnusedCapacity(ha7r, sa7r, pager, 1);
            const idx = self.len;
            var ptr = try self.swapInUnchecked(sa7r, pager, idx);
            defer self.swapOut(sa7r, pager, idx);
            ptr.* = item;
            self.len += 1;
        }

        const Iterator = struct {
            cur: usize,
            stop: usize,
            sa7r: SwapAllocator,
            pager: Pager,
            list: *const Self,
            in_idx: ?usize = null,

            pub fn new(list: *const Self, from: usize, to: usize, sa7r: SwapAllocator, pager: Pager) Iterator {
                return Iterator{
                    .cur = from,
                    .stop = to,
                    .list = list,
                    .pager = pager,
                    .sa7r = sa7r,
                };
            }

            pub fn close(self: *Iterator) void {
                self.swapOutResident();
            }

            fn swapOutResident(self: *Iterator) void {
                if (self.in_idx) |idx| {
                    self.list.swapOut(self.sa7r, self.pager, idx);
                    self.in_idx = null;
                }
            }

            pub fn next(self: *Iterator) !?*T {
                if (self.cur == self.stop) {
                    return null;
                }
                self.swapOutResident();
                var ptr = try self.list.swapIn(self.sa7r, self.pager, self.cur);
                self.in_idx = self.cur;
                self.cur += 1;
                return ptr;
            }
        };

        /// Might panic if the list is modified before the iterator is closed.
        /// Be sure to close the iterator after usage.
        /// This reuses the list's built in pager.
        pub fn iterator(self: *const Self, sa7r: SwapAllocator, pager: Pager) Iterator {
            // we give it the backing pager since we don't want it sharing our
            // single page cache in case of simultaneous usage
            return Iterator.new(self, 0, self.len, sa7r, pager);
        }
    };
}

test "SwapList.usage" {
    const page_size = std.mem.page_size;
    const ha7r = std.testing.allocator;
    var mmap_pager = try MmapPager.init(ha7r, "/tmp/comb.test.SwapList.usage", .{
        .page_size = page_size,
    });
    defer mmap_pager.deinit();

    var lru = try LRUSwapCache.init(ha7r, mmap_pager.pager(), 1);
    defer lru.deinit();
    var pager = lru.pager();

    var msa7r = PagingSwapAllocator(.{}).init(ha7r, pager);
    defer msa7r.deinit();
    var sa7r = msa7r.allocator();

    var list = SwapList(usize).init(pager.pageSize());
    defer list.deinit(ha7r, sa7r, pager);

    const item_per_page = page_size / @sizeOf(usize);
    const page_count = 10;
    var nums = [_]usize{0} ** (page_count * item_per_page);
    for (&nums, 0..) |*num, ii| {
        num.* = ii;
        try list.append(ha7r, sa7r, pager, ii);
    }
    {
        var it = list.iterator(sa7r, pager);
        defer it.close();
        var ii: usize = 0;
        while (try it.next()) |num| {
            try std.testing.expectEqual(ii, num.*);
            ii += 1;
        }
    }
}

// /// An array list that stores the items using a `SwapAllocator`.
// pub fn SwapListSimple(comptime T: type) type {
//     return struct { const Self = @This(); };
// }
//
// /// A SwapList that doesn't make use of the heap.
// /// Stores the list metadata using a `SwapListSimple` until it overflows a page,
// /// then uses an instance of itself to store the list metadata.
// /// This'll add one extra page `swapIn` operation per metadata storage mesa-levels
// /// used but an LRU cached pager should help here.
// pub fn SwapListUltraPetite(comptime T: type) type {
//     return struct {
//         const Self = @This();
//         pub const ListHeader = struct {
//         };
//     };
// }
// /// Swapping version of `std.MultiArrayList`.
// pub fn MultiSwapList(comptime T: type) type {
//     return struct {
//         const Self = @This();
//         const fields = std.meta.fields(T);
//
//         pub const Field = std.meta.FieldEnum(T);
//         fn FieldType(field: Field) type {
//             return std.meta.fieldInfo(T, field).field_type;
//         }
//
//         pub const Config = struct {
//             page_size: usize = std.mem.page_size,
//             /// the page count at which we'd be manually managing pages
//             /// instead of using the `SwapAllocator`
//             page_out_threshold: usize = blk: {
//                 // do fancy pants heurstic, we don't want to wait until we have
//                 // enough items for say 20 pages until we...page out
//                 break :blk fields.len;
//             },
//         };
//
//         const SmallList = struct {
//             ptr: SwapAllocator.Ptr,
//             len: usize,
//         };
//
//         config: Config,
//         len: usize = 0,
//         small_list: ?SmallList = null,
//         pages: std.ArrayListUnmanaged(PageNo) = .{},
//
//         pub fn init(config: Config) Self {
//             return Self {
//                 .config = config,
//             };
//         }
//
//         pub fn deinit(self: *Self) void {
//             _ = self;
//         }
//
//         fn perPage(self: *const Self) usize {
//             return self.config.page_size / @sizeOf(T);
//         }
//     };
// }
//
// test "MultiArrayList.usage" {}
