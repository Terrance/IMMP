from argparse import ArgumentParser

# Backwards-compatible import route for LocalFilter.
from immp import LocalFilter  # noqa
from immp.hook.runner import main

try:
    import uvloop
except ImportError:
    uvloop = None


def entrypoint():
    if uvloop:
        uvloop.install()
    parser = ArgumentParser(prog="immp", add_help=False)
    parser.add_argument("-w", "--write", action="store_true")
    parser.add_argument("file", metavar="FILE")
    args = parser.parse_args()
    main(args.file, args.write)


if __name__ == "__main__":
    entrypoint()
