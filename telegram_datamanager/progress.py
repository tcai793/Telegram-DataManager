from console import sc
import sys
import random
import time


class Progress:
    def __init__(self, lines):
        self._lines = lines
        self._print_empty_lines()

    def __del__(self):
        sys.stdout.write('\n')
        sys.stdout.flush()

    def _print_empty_lines(self):
        for i in range(self._lines):
            sys.stdout.write('\n')
        sys.stdout.flush()

    def _go_up_and_clear_line(self, line_diff):
        sys.stdout.write(sc.prev_line(line_diff))
        sys.stdout.write(sc.erase_line(0))

    def _go_down(self, line_diff):
        sys.stdout.write(sc.next_line(line_diff))

    def update_line(self, linenum, line_str):
        if linenum >= self._lines:
            raise ValueError('linenum >= self._lines')

        line_diff = self._lines-linenum
        if line_diff > 0:
            self._go_up_and_clear_line(line_diff)
        sys.stdout.write(line_str)
        sys.stdout.flush()
        if line_diff > 0:
            self._go_down(line_diff)
