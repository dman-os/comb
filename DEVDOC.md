# > *Comb* dev-doc

JUST PUT THE FUCKEN ALLOCATOR IN THE STRUCT YO!

## TODO

- Work stack
    - [x] Swapping PList
    - [x] FANotify notifications & test suite
      - [ ] `tmpfs` alternative for testing
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
against the index each time we catch one.

