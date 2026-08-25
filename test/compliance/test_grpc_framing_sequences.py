#!/usr/bin/env python3
"""Direct tests for deterministic gRPC message framing sequences."""

import argparse
import random
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_grpc_framing_sequences as target


class FramingSequenceTests(unittest.TestCase):
    def test_mutations_are_deterministic_and_cover_each_kind(self) -> None:
        first = []
        second = []
        for output, seed in ((first, 17), (second, 17)):
            rng = random.Random(seed)
            for index in range(len(target.MUTATIONS)):
                output.append(
                    target.mutate_frame(
                        target.frame(b"payload"),
                        target.frame(b"second"),
                        index,
                        rng,
                    )
                )
        self.assertEqual(first, second)
        self.assertEqual([item[0] for item in first], list(target.MUTATIONS))
        self.assertTrue(first[0][2])
        self.assertTrue(all(not item[2] for item in first[1:]))
        self.assertTrue(first[0][3])
        self.assertEqual(
            [item[3] for item in first[1:]],
            [False, False, True, True, True, True, False, False],
        )

    def test_numeric_arguments_are_bounded(self) -> None:
        self.assertEqual(target.seed_int("0xffffffff"), target.MAX_SEED)
        self.assertEqual(
            target.case_count_int(str(target.MAX_CASE_COUNT)),
            target.MAX_CASE_COUNT,
        )
        for value in ("-1", "4294967296", "seed"):
            with self.subTest(seed=value):
                with self.assertRaises(argparse.ArgumentTypeError):
                    target.seed_int(value)
        for value in ("0", "10001", "many"):
            with self.subTest(case_count=value):
                with self.assertRaises(argparse.ArgumentTypeError):
                    target.case_count_int(value)


if __name__ == "__main__":
    unittest.main()
