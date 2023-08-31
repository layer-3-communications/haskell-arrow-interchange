# Arrow Interchange

This library is kind of a mess right now. There is a library that builds with
`vex` and a different one that builds with `vext`. Build them each by doing
this:

    cabal build --project-file cabal.vex.project --builddir dist-vex all
    cabal build --project-file cabal.vext.project --builddir dist-vext all

And the run the tests (which use the `vex` part) with:

    ./scripts/runtests
