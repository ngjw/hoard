import argparse
from functools import cached_property


class ArgumentParser(argparse.ArgumentParser):

    @cached_property
    def subparsers(self):
        subparsers = self.add_subparsers()
        subparsers.required = True
        return subparsers

    def add_subparser(self, *args, **kwargs):
        return self.subparsers.add_parser(*args, **kwargs)

    def add_main_program(self, mode, prog):

        if mode is None:
            prog.configure_parser(self)
            self.set_defaults(main=prog.main)
        else:
            p = self.add_subparser(mode)
            prog.configure_parser(p)
            p.set_defaults(main=prog.main)

    def parse_and_run(self, argv=None):
        pargs = self.parse_args(argv)
        pargs.main(pargs)


class MainProgram:

    @classmethod
    def configure_parser(cls, p):
        raise NotImplementedError

    @classmethod
    def main(cls, pargs):
        raise NotImplementedError

    @classmethod
    def run(cls, argv=None):
        p = ArgumentParser(cls.__name__)
        p.add_main_program(None, cls)
        p.parse_and_run(argv)

