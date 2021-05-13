#
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Tests for generator_utils."""
from zetasql.parser.generator_utils import CleanIndent
from zetasql.parser.generator_utils import Trim
from absl.testing import absltest


class GeneratorUtilsTest(absltest.TestCase):

  def test_trim(self):
    untrimmed = '\n\n\nbrevity   \n\n\nis the soul \n\nof wit\n\n\n\n\n'
    expected = '\n\nbrevity\n\nis the soul\n\nof wit\n\n'
    self.assertEqual(expected, Trim(untrimmed))

  def test_clean_indent(self):
    comment = """
    First line of comment, often rather short.

    Sometimes there is a much longer, rambling, multiline continuation which
    has a lot more detail.
      """
    expected = """// First line of comment, often rather short.
//\u0020
// Sometimes there is a much longer, rambling, multiline continuation which
// has a lot more detail."""
    self.assertEqual(expected, CleanIndent(comment, prefix='// '))


if __name__ == '__main__':
  absltest.main()
