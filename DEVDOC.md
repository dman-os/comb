# > *Comb* dev-doc

JUST PUT THE FUCKEN ALLOCATOR IN THE STRUCT YO!

## TODO

- Work stack
    - [ ] Dedicated `Db.Writer` type
    - [ ] Database modification operations
      - [ ] Created
      - [ ] Updated
      - [ ] Deleted
      - [ ] Deleted (Dir)
      - [ ] Moved
      - [ ] Moved (Dir)
    - [ ] Queries
    - [ ] FANotify <-> Database integration
    - [ ] B-Tree implementation for field indices
        - [ ] BSTree
    - [x] FANotify notifications & test suite
      - [ ] `tmpfs` alternative for testing
    - [x] `std.Thread.Condition` based `Queue`
      - A single CPU is still saturated when it's flooded with events. Investigate what's making this happen
        - Crossed fingers it won't be the syscalls (can't be!)
        - My current suspicion is that it's the db query for the parent of the target entry that's spiking the CPU
     - [ ] remove the `Thread`.`yield` calls all about. Timed wait on the channels makes them obsolete.

- Later
    - [ ] Support less than gram length (3) search strings

## design doc

### Features

- [ ] Fast
    - [ ] Acceleration Indices
        - [x] PostingList (for names)
        - [ ] B-Tree (for other values)
- [ ] Current (TM)
    - [ ] FANotify integration
- [ ] Low resource footprint
    - [x] Disk Swapping
- [ ] CLI client
- [ ] IPC interface

### Comb Query Executor

Prolly a DAG; but except nodes can be deduplicated.

- Nodes are lists of ids: *shortlist*
- Edges are *filters*
- Start node is the entire table, end the result

Concerns:
- how to identify duplicate nodes?
- how to and when to use indices?
- pagination strategy?
- how to maximize page motion?
- logical operators
- multi threading
- debugging tools: convert dag to exec trace
- good algos for set operations, graph transformations...etc

#### Node actions

- Query Plan
- Op 
  - and
    - set intersection
  - or
    - set union
  - not
    - inverse previous ops?
- Param
  - nameMatch

## devlog

### Upstream Issues

- [Zig compiler bug surrounding optional const slices](https://github.com/ziglang/zig/issues/4907)
- [Max depth for `std.fmt.formatType` isn't configurable](https://github.com/ziglang/zig/issues/2370)

### IPC less design?

That is, instead of talking to a deamon, clients use a library to interact with
a db file and query it. We'll still need a daemon if we want to keep it current
and speccing out and implementing a DB format that supports mutliple clients is 
a lot more work.

### July 05 Incident

So I spent the whole goddamn day implementing an LRU cache over the code that
does the paging in and out from db file. (More like helper file). This,
ofcourse, meant hammering out an uncessarily, extensible dynamic dispatch API
for pagers and everything. It's all compiling and almost all the tests are
passing except one. A stupid mistake. I want the LRU cache to have multiple
users (`Pagers` are implemented in the vein of `std.mem.Allocator`) but I
completely neglected to think through how that'd work. Here's my problem: if
two different users request the same page be swapped in but one of them swaps
out early, ofcourse we'll need to keep the page swapped in till everyone's
happy...

...

...

oh fuck. Writing this down revealed *the* simple solution. I thought I had to
rewrite most of today to get this working. I really ought to quit programming
at such late hours.

RUBBER DUCKING!

### Doubling Mapping

What happens if I mmap the same range of a file to differents bits of process
memory? Sherlock Holmes this.

### LRU cache completely devastates performance

My initial thinking was the `mmap` call was the bottlneck and putting a LRU
cache in front of that might improve performance. It turns out, the hash map
look ups are actually worse. This doesn't happen wieh it's an LRU that only
holds on to 1 object. It's the multi thousand long LRU that's giving me shit.
What gives?

---

Right, this was my bad. I'd implemented the LRU Cache wrong. It now improves
performance by a factor of 2x to 10x.

### MMAP HELL

The insophistication of my mmaping scheme has finally caught up with me. Let's 
redo it. First of all, here's what we want:

- Avoid fragmentation and minimaize disk size usage.
- Scheme for small lists to be able to share pages.

---

After some examining, it turns out that the current apporoach and direction was
more than sufficent. All I had to do was add a stricter constraint that any
access to swapped memory has to be explicity swappedIn/out. There's no way to
safely abstract this away from high level users.

### FAN_EVENT_MODIFY

...are emitted anytime a file is written to and as you can imagine are
supernumerous. We'll be needing a scheme to combine them for over a span of
time if not options for completely disabling them erstwhile we find ourselves
syscalling and queriying 
against the index each time we catch 


### Test harnesses

I want to automated tests for every peice of this as I imagine, testing this by
hand would be painful.  Especially as more and more features accure.  There are
a number of moving peices so unit tests are probably the best thing to do but I
wonder if a single large e2e testing suite will be workable.  If I were to write
such a thing, I want it to exercise all the interesting boundaries and I want it
to have easy knobs that'll allow me exercise these boundaries in any variation
I'd want.  That sounds like an unreasonable requirment at first blush but let's
see if we can get something working.  If it doesn't suffice, I guess it's always
good to have an e2e suite anyways.

Concerns include:
- Query parsing
- IPC
- Query execution
- Fs events

#### `tmpfs`

After a bit of tinkering, I've written an e2e suite for the fanotify pipeline.
Making modifications to the filesystem on one end and querying the database on
the other to see if the expected changes are reflected.  In order to make it
managable, the suite and all previous fanotify tests make use of `tmpfs` mounts.
Unfortunately, the fanotify implementation seems to be lacking for `tmpfs` when
using `REPORT_FID` and it doesn't give me the containing directory name making
nested events in the mount...unparsable...umm... rubber fucking duck! (FIXME:
next time, write the rubber duck relization when you have one.)

---

After spending hours trying to make sense of the contents of kernel `bugzilla`
and `lore` and finding no reported issues on `tmpfs`, I decided to go read
`fanotify` manual pages again.  It turns out, I'm reading the event buffers
wrong.  To be more specific, it's possible that multiple
`fanotify_event_info_fid` might be attached to a single fanotify event but my
code just reads one.  Could be that's where we'll find the missing
information...let's investigate.

---

There are indeed multiple fid records being sent.  Two in the problematic cases
which lines up with what the docs read.  Unfortunately, the record I was
ignoring only carries a stale file handle.  Which is not useful, not usefall at
all.

---

Hold up for a fucking minute?  The MAN pages are new!  I started to notice a
bunch of information that I know for a fact wasn't in here before and looking at
the change date, it's indeed from December of 2022.  This is good news. I wonder
if the notice about multiple records was in the previous verision.

---

OMG, I think I found the bug.  The `open_by_handle_at` command takes the file
descriptor of the mount we're accessing as the first paramter.  I've been giving
it the handle to current working directory which resolves to the mount the
current directory is on which is not the same damn mount as the `tmpfs` mounts.
Let's see if giving it the appropriate handle helps.

Edit: This was revealed through close rescrutiny of the example given in the man
page.

---

It indeed did help.  Took a while though.  `open_by_handle_at` kept throwing
`EBADF`(`BadFileDescriptor`) when I used the fd I used from a `Dir` open on the
mount using `std.os.fs.cwd().openDir`.  I had to use `std.os.open` directly with
the `O.RDONLY` permission for it to work.

---

Spent the whole fucking night trying to hunt down a seg fault...incase want to
know (still to find it). You know what, this warrants a devlog entry.


### Thread  Cleanup

Within the codebase exists many instances of such code that serve as entry
points for threads:

```ziglang
fn threadFn(self: *@This()) !void {
    while (true) {
        // wait for events on a channel. Not all such thread constructs use 
        // channels but most do these cases are relevant to demonstrate 
        // the...problem.
        var event = self.channel.get();
        // ... do something with event (hopefully cleanup)
        try self.handleEvent(event);
    }

 ```

We don't want to the thread to go on forever so we usually add a way to signal
it to finish.  In Rust, one could use the closing on the channel to signal this
but our implementation isn't so sophisticated.  This means that we'll have to
add timeouts to the `get` to make sure we don't wait in there forever and miss
the death signal.

```ziglang
fn threadFn(self: *@This()) !void {
    // wait for signal to finish. Usually makes use of `Thread.ResetEvent` to 
    // atomically coordinate signal which uses futexes within.
    while(
        !self.die_signal.isSet()
    ) {
        if (self.event_q.getTimed(500_000_000)) |event| {
            if (self.die_signal.isSet()) return;
            try self.handleEvent(event);
        }
    }
}
 ```

The channel isn't sophisticated enough to signal when there are no more possible
senders 

```ziglang
fn threadFn(self: *@This()) !void {
    while(
        !self.die_signal.isSet()
    ) {
        var event = self.channel.get();
        try self.handleEvent(event);
    }
}
 ```

```ziglang
fn threadFn(self: *@This()) !void {
    defer self.ha7r.destroy(self);
    while(
        !self.die_signal.isSet()
    ) {
        // wait for events on a channel. Not all such thread constructs use channels but most do 
        // these cases are relevant to demonstrate the...problem.
        if (self.event_q.getTimed(500_000_000)) |event| {
            if (self.die_signal.isSet()) return;
            // ... do something with event (hopefully cleanup)
            try self.handleEvent(event);
        } else |_| {
            // if we don't have an
            std.Thread.yield() catch @panic("ThreadYieldErr");
        }
    }
}
 ```
