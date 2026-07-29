#!/usr/bin/env python3

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from generate_settings import normalized_cpp  # noqa: E402
from settings_scripts.parse_and_sort_settings_in_json import (
    sort_json_data,
)  # noqa: E402


class TestGeneratedSettingsCheck(unittest.TestCase):
    def test_cpp_token_comparison_ignores_formatting_and_literal_wrapping(self) -> None:
        generated = 'static constexpr const char *Description = "one long description";'
        formatted = "static constexpr const char * Description =\n" '    "one long "\n' '    "description";\n'
        self.assertEqual(normalized_cpp(generated), normalized_cpp(formatted))

    def test_cpp_token_comparison_preserves_content_and_token_boundaries(self) -> None:
        self.assertNotEqual(
            normalized_cpp('const char *value = "left";'),
            normalized_cpp('const char *value = "right";'),
        )
        self.assertNotEqual(normalized_cpp("int value = 1;"), normalized_cpp("intvalue = 1;"))

    def test_check_mode_sorting_does_not_rewrite_json(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "settings.json"
            original = [{"name": "z"}, {"name": "a"}]
            path.write_text(json.dumps(original), encoding="utf-8")

            sorted_data = sort_json_data(path, write_sorted=False)

            self.assertEqual(sorted_data, [{"name": "a"}, {"name": "z"}])
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), original)


if __name__ == "__main__":
    unittest.main()
