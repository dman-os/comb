# > *Comb* dev-doc

## TODO

- Work stack
    - [ ] Swapping PList
    - [ ] B-Tree
        - [ ] BSTree

## design doc

### Features

- [ ] Low resource footprint
- [ ] Current TM
- [ ] CLI client
- [ ] IPC interface

## devlog

### IPC less design?

That is, instead of talking to a deamon, clients use a library to interact with
a db file and query it. We'll still need a daemon if we want to keep it current
and speccing out and implementing a DB format that supports mutliple clients is 
a lot more work.

### July 05 Incident

So I spent the whole goddamn day implementing an LRU cache over the code that does
the paging in and out from db file. (More like helper file). This, ofcourse, meant 
hammering out an uncessarily, extensible dynamic dispatch API for pagers and everything.
It's all compiling and almost all the tests are passing except one. A stupid mistake.
I want the LRU cache to have multiple users (`Pagers` are implemented in the vein of 
`std.mem.Allocator`) but I completely neglected to think through how that'd work.
Here's my problem: if two different users request the same page be swapped in but one of them
swaps out early, ofcourse we'll need to keep the page swapped in till everyone's happy...
...
...
oh fuck. Writing this down revealed *the* simple solution. I thought I had to rewrite
most of today to get this working. I really ought to quit programming at such late hours.

RUBBER DUCKING!

### Doubling Mapping

What happens if I mmap the same range of a file to differents bits of process memory? Sherlock Holmes this.

### LRU cache completely devastates performance

My initial thinking was the `mmap` call was the bottlneck and putting a LRU cache
in front of that might improve performance. It turns out, the hash map look ups are
actually worse. This doesn't happen wieh it's an LRU that only holds on to 1 object.
It's the multi thousand long LRU that's giving me shit. What gives?

---

Right, this was my bad. I'd implemented the LRU Cache wrong. It now improves performance
by a factor of 2x to 10x.
