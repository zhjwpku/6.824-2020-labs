### 6.824-2020-labs

This branch is my implementation of labs for MIT 6.824 Distributed Systems (Spring 2020)

The golang version I use is `go version go1.17 darwin/amd64`

When doing the labs, if you come across `cannot find module` error, you should run:

```
export GO111MODULE=off
```

On MacOS you should install `coreutils` to ensure the script works:

```
brew install coreutils
ln -s /usr/local/bin/gtimeout /usr/local/bin/timeout
```

### some useful links

- [schedule of lectures](http://nil.lcs.mit.edu/6.824/2020/schedule.html)
- [lectures videos](https://www.youtube.com/playlist?list=PLrw6a1wE39_tb2fErI4-WkMbsvGQk9_UB)