const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
// TODO: remove dependence on heap allocator

pub const MmapPager = struct {
    const Self = @This();
    pub const Config = struct {
        page_size: usize = std.mem.page_size,
    };

    const Page = struct {
        slice: ?[]align(std.mem.page_size) u8,
        free: bool,
    };
    pub const PageId = usize;

    // const Allocation = struct {
    //     // the first page of the allocation
    //     start: usize,
    //     len: usize,
    // };

    const FreeList = std.PriorityQueue(
        PageId, 
        void, 
        struct {
            // we wan't earlier pages to be filled in earlier
            fn cmp(ctx: void, a: PageId, b: PageId) std.math.Order {
                _ = ctx;
                return std.math.order(a, b);
            }
        }.cmp
    );

    config: Config,
    a7r: Allocator,

    backing_file: std.fs.File,
    pages: std.ArrayListUnmanaged(Page),
    // slice_page_map: std.AutoHashMapUnmanaged(usize, PageId),
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
            // .slice_page_map = std.AutoHashMapUnmanaged(usize, PageId){},
        };
        return self;
    }

    pub fn deinit(self: *Self) void {
        self.backing_file.close();
        self.pages.deinit(self.a7r);
        // self.slice_page_map.deinit(self.a7r);
        self.free_list.deinit();
    }

    pub fn alloc(self: *Self) !PageId {
        if (self.free_list.removeOrNull()) |id| {
            var page = &self.pages.items[id];
            page.free = false;
            return id;
        } else {
            // ensure capacity before trying anything
            try self.pages.ensureUnusedCapacity(self.a7r, 1);
            // ensure capacity on the free list to avoid error checking on free calls
            try self.free_list.ensureUnusedCapacity(1);
            try std.os.ftruncate(self.backing_file.handle, (self.pages.items.len + 1) * self.config.page_size);
            try self.pages.append(self.a7r, Page{
                .slice = null,
                .free = false,
            });
            return self.pages.items.len - 1;
        }
    }

    pub fn swapAndFree(self: *Self, id: PageId) !void {
        self.swapOut(id);
        try self.free(id);
    }

    /// This doesn't swap out the page.
    pub fn free(self: *Self, id: PageId) void {
        if (id >= self.pages.items.len) {
            return;
        }
        var page = &self.pages.items[id];
        page.free = true;

        if (id == self.pages.items.len - 1) {
            var ii = id;
            while (true) : ({
                ii -= 1;
            }) {
                if (ii == 0 or !(self.pages.items[ii].free)) break;
                _ = self.pages.popOrNull();
            }
        } else {
            // we've been growing the free list along the page list so it's ok
            self.free_list.add(id) catch unreachable;
        }
    }

    pub fn swapIn(self: *Self, id: PageId) ![]align(std.mem.page_size) u8 {
        var page = &self.pages.items[id];
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
                id * self.config.page_size,
            );
            errdefer std.os.munmap(slice);
            // try self.slice_page_map.put(self.a7r, @ptrToInt(slice.ptr), id);
            page.slice = slice;
            return slice;
        }
    }

    pub fn swapOut(self: *Self, page_id: usize) void {
        if (self.pages.items.len <= page_id) {
            return;
        }
        var page = &self.pages.items[page_id];
        if (page.slice) |slice| {
            std.os.munmap(slice);
            // _ = self.slice_page_map.remove(@ptrToInt(slice.ptr));
            page.slice = null;
        }
    }

    test "MmapPager.usage" {
        var a7r = std.testing.allocator;
        const page_size = std.mem.page_size;
        var pager = try MmapPager.init(a7r, "/tmp/MmmapUsage.test", .{
            .page_size = page_size,
        });
        defer pager.deinit();

        const item_per_page = page_size / @sizeOf(usize);
        const page_count = 10;
        var nums = [_]usize{0} ** (page_count * item_per_page);
        for (nums) |*num, ii| {
            num.* = ii;
        }
        var pages: [page_count]PageId = undefined;
        for (pages) |*id, pii| {
            id.* = try pager.alloc();
            var bytes = try pager.swapIn(id.*);
            defer pager.swapOut(id.*);
            for (std.mem.bytesAsSlice(usize, bytes)) |*num, ii| {
                num.* = nums[(pii * item_per_page) + ii];
            }
        }
        for (pages) |id, pii| {
            var bytes = try pager.swapIn(id);
            defer pager.swapOut(id);
            for (std.mem.bytesAsSlice(usize, bytes)) |num, ii| {
                try std.testing.expectEqual(nums[(pii * item_per_page) + ii], num);
            }
        }
        {
            const page_id = page_count / 2;
            pager.free(pages[page_id]);
            // TODO: find a way to test panics?
            // try std.testing.expectError(error.PageNotAllocated, pager.swapIn(pages[page_id]));

            var new_stuff = [_]usize{0} ** item_per_page;
            for (new_stuff) |*num, ii| {
                num.* = item_per_page - ii;
                nums[(page_id * item_per_page) + ii] = item_per_page - ii;
            }

            var id = try pager.alloc();
            var bytes = try pager.swapIn(id);
            defer pager.swapOut(id);
            for (std.mem.bytesAsSlice(usize, bytes)) |*num, ii| {
                num.* = new_stuff[ii];
            }
        }
        for (pages) |id, pii| {
            var bytes = try pager.swapIn(id);
            defer pager.swapOut(id);
            for (std.mem.bytesAsSlice(usize, bytes)) |num, ii| {
                try std.testing.expectEqual(nums[(pii * item_per_page) + ii], num);
            }
        }
    }
};

const PageCache = struct {
    const PageId = MmapPager.PageId;

    const InPage = struct {
        id: PageId,
        slice: []align(std.mem.page_size)u8,
    };

    in_page: ?InPage = null,

    fn new() PageCache {
        return PageCache{ .in_page = null };
    }

    fn swapIn(self: *PageCache, pager: *MmapPager, id: PageId) ![]align(std.mem.page_size)u8 {
        if (self.in_page) |in_p| {
            if (in_p.id == id) {
                return in_p.slice;
            } else {
                pager.swapOut(in_p.id);
                const slice = try pager.swapIn(id);
                self.in_page = InPage {
                    .id = id,
                    .slice = slice,
                };
                return slice;
            }
        } else {
            const slice = try pager.swapIn(id);
            self.in_page = InPage {
                .id = id,
                .slice = slice,
            };
            return slice;
        }
    }

    fn swapOut(self: *PageCache, pager: *MmapPager) void {
        if (self.in_page) |in_p| {
            pager.swapOut(in_p.id);
            self.in_page = null;
        }
    }
};

pub const AllocatorConfig = struct {
    page_size: usize = std.mem.page_size,
};

pub fn SwappingAllocator(config: AllocatorConfig) type {
    return struct {
        const Self = @This();

        const bucket_count = std.math.log2(config.page_size);
        const BuckIndex = std.meta.Int(.unsigned, std.math.log2(bucket_count));
        /// TODO: consider using a usize and bittweedling to improve performance
        pub const Ptr = packed struct {
            const SlotIndex = std.meta.Int(
                .unsigned, 
                @typeInfo(usize).Int.bits - @typeInfo(BuckIndex).Int.bits
            );
            buck: BuckIndex,
            slot: SlotIndex,
        };

        comptime {
            // TODO: THiS gets fucked if usize bytes are above 255
            if (@sizeOf(Ptr) != @sizeOf(usize)) {
                var buf = [_]u8{0} ** 64;
                var msg = std.fmt.bufPrint(
                    &buf, 
                    "Ptr size mismatch: {} !=  {}", 
                    .{ @sizeOf(Ptr), @sizeOf(usize) }
                ) catch @panic("wtf");
                @compileError(msg);
            }
        }
        const PageId = MmapPager.PageId;

        const Bucket = struct {
            // const Class = std.meta.Int(.unsigned, bucket_count);
            const Allocation = struct {
                // gen: usize,
                free: bool,
                len: usize, // FIXME: usize is too big for this purpose
            };
            const FreeList = std.PriorityQueue(
                Ptr.SlotIndex,
                void,
                struct {
                    fn cmp(_: void, a: Ptr.SlotIndex, b: Ptr.SlotIndex) std.math.Order {
                        return std.math.order(a, b);
                    }
                }.cmp,
            );

            // allocs: std.MultiArrayList(Allocation),
            allocs: std.ArrayListUnmanaged(Allocation),
            pages: std.ArrayListUnmanaged(PageId),
            free_list: FreeList,
            // class: Class,
            per_page: usize,

            fn init(ha7r: Allocator, per_page: usize) Bucket {
                return .{
                    // .allocs = std.MultiArrayList(Allocation){},
                    .allocs = std.ArrayListUnmanaged(Allocation){},
                    .pages = std.ArrayListUnmanaged(PageId){},
                    .free_list = FreeList.init(ha7r, .{}),
                    // .class = class,
                    .per_page = per_page,
                };
            }

            fn deinit(self: *Bucket, ma7r: *Self) void{
                self.allocs.deinit(ma7r.ha7r);
                for (self.pages.items) |id| {
                    ma7r.pager.free(id);
                }
                self.pages.deinit(ma7r.ha7r);
                self.free_list.deinit();
            }

            fn addOne(self: *Bucket, alloc_len: usize, ma7r: *Self) !Ptr.SlotIndex {
                std.debug.print("alloc_len = {}, slot_size = {}\n", .{ alloc_len, (ma7r.pager.config.page_size / self.per_page)});
                std.debug.assert(alloc_len < (ma7r.pager.config.page_size / self.per_page));
                if (self.free_list.removeOrNull()) |slot| {
                    self.allocs.items[slot].free = false;
                    return slot;
                } else {
                    const len = self.allocs.items.len;
                    if (len == std.math.maxInt(Ptr.SlotIndex)) @panic(@typeName(Bucket) ++ " is full");
                    const cap = self.pages.items.len * self.per_page;
                    if (len >= cap) {
                        // check if our heap's in good shape before talking to swap
                        try self.pages.ensureUnusedCapacity(ma7r.ha7r, 1);
                        const new_page = try ma7r.pager.alloc();
                        _ = try self.pages.append(ma7r.ha7r, new_page);
                    }
                    try self.allocs.ensureUnusedCapacity(ma7r.ha7r, 1);
                    try self.allocs.append(ma7r.ha7r, Allocation {
                        // .gen = 0,
                        .free = false,
                        .len = alloc_len,
                    });
                    return @intCast(Ptr.SlotIndex, self.allocs.items.len - 1);
                }
            }

            pub fn swapIn(self: *Bucket, slot: Ptr.SlotIndex, pager: *MmapPager, cache: *PageCache) ![]u8 {
                const meta = self.allocs.items[slot];
                if (meta.free) @panic("Ptr points to free allocation");
                const page_idx = slot / self.per_page;
                const page_slice = try cache.swapIn(pager, self.pages.items[page_idx]);
                // const page_slice = std.mem.bytesAsSlice(T, bytes[0..((bytes.len / @sizeOf(T)) * @sizeOf(T))]);
                const size = @divExact(pager.config.page_size, self.per_page);
                const page_slot = slot - (page_idx * self.per_page);
                return page_slice[page_slot * size..(page_slot * size) + meta.len];
            }
        };

        ha7r: Allocator,
        pager: *MmapPager,
        buckets: [bucket_count]?Bucket,
        cache: PageCache,
        // free_list: [bucket_count]?FreeList,

        pub fn init(ha7r: Allocator, pager: *MmapPager) Self {
            return .{
                .ha7r = ha7r,
                .pager = pager,
                .buckets = [_]?Bucket{null} ** bucket_count,
                .cache = PageCache.new(),
                // .free_list = [_]?FreeList{null} ** bucket_count,
            };
        }

        pub fn deinit(self: *Self) void {
            for (self.buckets) |*opt| {
                if (opt.*) |*buck| {
                    buck.deinit(self);
                }
            }
            self.cache.swapOut(self.pager);
            // for (self.free_list) |*opt| {
            //     if (opt.*) |*list| {
            //         list.deinit();
            //     }
            // }
        }

        pub fn swapIn(self: *Self, ptr: Ptr) ![]u8 {
            const buck_idx = ptr.buck;
            if (self.buckets[buck_idx]) |*buck|{
                return try buck.swapIn(ptr.slot, self.pager, &self.cache);
            } else {
                @panic("Ptr points to empty bucket");
            }
        }

        pub fn alloc(self: *Self, len: usize) !Ptr {
            const buck_idx = std.math.log2(std.math.ceilPowerOfTwo(usize, len) catch @panic("fuck no"));
            if (buck_idx >= self.buckets.len) {
                std.debug.todo("large allocations");
            } else {
                const buck_idx_fit = @intCast(BuckIndex, buck_idx);
                std.debug.print("buck for len {} == {}\n", .{ len, @as(usize, 1) << buck_idx_fit });
                if (self.buckets[buck_idx_fit] == null) {
                    const slot_size = @as(usize, 1) << buck_idx_fit;
                    self.buckets[buck_idx_fit] = Bucket.init(
                        self.ha7r, 
                        @divExact(config.page_size, slot_size),
                    );
                }
                var buck = &(self.buckets[buck_idx_fit].?);
                var slot = try buck.addOne(len, self);
                return Ptr{ .buck = buck_idx_fit, .slot = slot };
            }
        }
    };
}

test "SwappingAllocator.usage" {
    const page_size = std.mem.page_size;
    const ha7r = std.testing.allocator;
    var pager = try MmapPager.init(ha7r, "/tmp/comb.test.SwappingAllocator.usage", .{
        .page_size = page_size,
    });
    defer pager.deinit();
    
    var ma74 = SwappingAllocator(.{}).init(ha7r, &pager);
    defer ma74.deinit();

    std.debug.print("Size MPA: {}\n", .{@sizeOf(SwappingAllocator(.{ .page_size = 4 * 1024 }))});
    std.debug.print("Size GPA: {}\n", .{@sizeOf(std.heap.GeneralPurposeAllocator(.{}))});

    const table = [_][]const u8{
        "i wondered all night"[0..],
        "i wondered all night"[0..],
        "i wondered all night about you"[0..],
        "i've been here for years just wondering around the neighborhood"[0..],
    };
    var ptrs: [table.len]SwappingAllocator(.{}).Ptr = undefined;

    for (&table) |case, ii| {
        ptrs[ii] = try ma74.alloc(case.len);
        var string = try ma74.swapIn(ptrs[ii]);
        std.mem.copy(u8, string, case);
    }

    // std.debug.print("Allocer\n{any}\n", .{ ma74.buckets });
    for (&ptrs) |ptr, ii| {
        const string = try ma74.swapIn(ptr);
        try std.testing.expectEqualSlices(u8, table[ii], string);
    }
}

pub fn SwappingList(comptime T: type) type {
    return struct {
        const Self = @This();
        const PageId = MmapPager.PageId;

        a7r: Allocator,
        pager: *MmapPager,
        len: usize,
        capacity: usize,
        pages: std.ArrayListUnmanaged(PageId),
        cache: PageCache,
        /// items per page
        per_page: usize,

        /// Panics if index is out of bound.
        inline fn ptrTo(self: *Self, index: usize) !*T {
            const page_idx = index / self.per_page;
            const bytes = try self.cache.swapIn(self.pager, self.pages.items[page_idx]);
            const page_slice = std.mem.bytesAsSlice(T, bytes[0..((bytes.len / @sizeOf(T)) * @sizeOf(T))]);
            return &page_slice[index - (page_idx * self.per_page)];
        }

        pub fn init(a7r: Allocator, pager: *MmapPager) Self {
            return Self{
                .a7r = a7r,
                .len = 0,
                .capacity = 0,
                .pager = pager,
                .pages = std.ArrayListUnmanaged(PageId){},
                .per_page = pager.config.page_size / @sizeOf(T),
                .cache = PageCache.new(),
            };
        }

        pub fn deinit(self: *Self) void {
            self.cache.swapOut(self.pager);
            for (self.pages.items) |id| {
                self.pager.free(id);
            }
            self.pages.deinit(self.a7r);
        }

        pub fn ensureUnusedCapacity(self: *Self, n_items: usize) !void {
            if (self.capacity > self.len + n_items) {
                return;
            }
            const new_cap = if (self.capacity == 0) self.per_page else self.capacity * 2;
            var new_pages_req = (new_cap - self.capacity) / self.per_page;
            std.debug.assert(new_pages_req > 0);
            try self.pages.ensureUnusedCapacity(self.a7r, new_pages_req);
            while (new_pages_req > 0) : ({
                new_pages_req -= 1;
            }) {
                const id = try self.pager.alloc();
                try self.pages.append(self.a7r, id);
                self.capacity += self.per_page;
            }
            // std.debug.print("capacity increased to: {}\n",.{ self.capacity });
        }

        pub fn get(self: *Self, idx: usize) !*T {
            if (idx >= self.len) @panic("out of bounds bich");
            return try self.ptrTo(idx);
        }

        pub fn set(self: *Self, idx: usize, item: T) !void {
            if (idx >= self.len) @panic("out of bounds bich");
            var ptr = try self.ptrTo(idx);
            ptr.* = item;
        }

        pub fn pop(self: *Self) !T {
            if (self.len == 0) @panic("iss empty");
            var ptr = try self.ptrTo(self.len - 1);
            self.len -= 1;
            return ptr.*;
        }

        pub fn append(self: *Self, item: T) !void {
            try self.ensureUnusedCapacity(1);
            var ptr = try self.ptrTo(self.len);
            ptr.* = item;
            self.len += 1;
        }

        const Iterator = struct {
            cur: usize,
            stop: usize,
            cache: PageCache,
            list: *const Self,

            pub fn new(list: *const Self, from: usize, to: usize) Iterator {
                return Iterator{
                    .cur = from,
                    .stop = to,
                    .list = list,
                    .cache = PageCache.new(),
                };
            }

            pub fn close(self: *Iterator) void {
                self.cache.swapOut(self.list.pager);
            }

            pub fn next(self: *Iterator) !?*T {
                if (self.cur == self.stop) {
                    return null;
                }
                var ptr = try self.ptrTo(self.cur);
                self.cur += 1;
                return ptr;
            }

            /// Panics if index is out of bound.
            inline fn ptrTo(self: *Iterator, index: usize) !*T {
                const page_idx = index / self.list.per_page;
                const bytes = try self.cache.swapIn(self.list.pager, self.list.pages.items[page_idx]);
                const page_slice = std.mem.bytesAsSlice(T, bytes[0..((bytes.len / @sizeOf(T)) * @sizeOf(T))]);
                return &page_slice[index - (page_idx * self.list.per_page)];
            }
        };

        /// Might panic if the list is modified before the iterator is closed.
        /// Be sure to close the iterator after usage.
        pub fn iterator(self: *const Self) Iterator {
            return Iterator.new(self, 0, self.len);
        }
    };
}

test "SwappingList.usage" {
    const page_size = std.mem.page_size;
    const a7r = std.testing.allocator;
    var pager = try MmapPager.init(a7r, "/tmp/comb.test.SwappingList.usage", .{
        .page_size = page_size,
    });
    defer pager.deinit();
    var list = SwappingList(usize).init(a7r, &pager);
    defer list.deinit();

    const item_per_page = page_size / @sizeOf(usize);
    const page_count = 10;
    var nums = [_]usize{0} ** (page_count * item_per_page);
    for (nums) |*num, ii| {
        num.* = ii;
        try list.append(ii);
    }
    {
        var it = list.iterator();
        defer it.close();
        var ii: usize = 0;
        while (try it.next()) |num| {
            try std.testing.expectEqual(ii, num.*);
            ii += 1;
        }
    }
}
