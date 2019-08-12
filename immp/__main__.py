from argparse import ArgumentParser

from immp import LocalFilter  # For backwards compatibility only.
from immp.hook.runner import main


def entrypoint():
    parser = ArgumentParser(prog="immp", add_help=False)
    parser.add_argument("-w", "--write", action="store_true")
    parser.add_argument("file", metavar="FILE")
    args = parser.parse_args()
    main(args.file, args.write)


if __name__ == "__main__":
    entrypoint()
