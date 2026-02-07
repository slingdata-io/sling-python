"""
Test for columns parameter type casting vs column selection issue.

This test verifies that the `columns` parameter in Sling class is used for
type casting (e.g., casting a string to timestamp) and NOT for column selection.

Issue: When using `columns={"created_at": "timestamp"}` with Arrow input,
only the `created_at` column was being returned instead of all columns with
`created_at` cast to timestamp type.

See: https://github.com/slingdata-io/sling-cli/issues/XXX
"""
import os
import tempfile
import pytest

from sling import Sling


class TestColumnsTypeCasting:
    """Test that columns parameter applies type casting, not column selection."""

    def test_columns_casts_types_not_selects_with_arrow_input(self):
        """
        Test that columns parameter casts column types instead of selecting columns.

        When using input data with Arrow format and specifying columns for type casting,
        ALL columns should be present in the output, with the specified columns cast
        to their target types.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = os.path.join(tmpdir, "output.csv")

            # Sample data with multiple columns
            sample_data = [
                {"id": 1, "name": "Alice", "created_at": "2024-01-15", "value": 100},
                {"id": 2, "name": "Bob", "created_at": "2024-02-20", "value": 200},
            ]

            # Create Sling with columns to cast 'created_at' to timestamp
            # This should cast the type, NOT select only this column
            sling = Sling(
                input=sample_data,
                tgt_object=f"file://{output_file}",
                columns={"created_at": "timestamp"},  # Should cast, not select
            )

            sling.run(print_output=False)

            # Read output and verify ALL columns are present
            import csv
            with open(output_file, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"

            # Get the column names from the output
            output_columns = set(rows[0].keys())

            # Verify ALL original columns are present (columns is for casting, not selection)
            assert 'id' in output_columns, \
                f"id column should exist - columns is for type casting, not selection. Got columns: {output_columns}"
            assert 'name' in output_columns, \
                f"name column should exist - columns is for type casting, not selection. Got columns: {output_columns}"
            assert 'created_at' in output_columns, \
                f"created_at column should exist. Got columns: {output_columns}"
            assert 'value' in output_columns, \
                f"value column should exist - columns is for type casting, not selection. Got columns: {output_columns}"

            # Verify data integrity
            assert rows[0]['name'] == 'Alice'
            assert rows[1]['name'] == 'Bob'

    def test_select_filters_columns(self):
        """
        Test that select parameter actually filters/selects columns.

        This is the expected behavior for column selection - use `select`, not `columns`.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = os.path.join(tmpdir, "output.csv")

            sample_data = [
                {"id": 1, "name": "Alice", "created_at": "2024-01-15", "value": 100},
                {"id": 2, "name": "Bob", "created_at": "2024-02-20", "value": 200},
            ]

            # Create Sling with select to filter columns
            sling = Sling(
                input=sample_data,
                tgt_object=f"file://{output_file}",
                select=["id", "name"],  # Should filter/select columns
            )

            sling.run(print_output=False)

            import csv
            with open(output_file, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            output_columns = set(rows[0].keys())

            # With select, we expect ONLY the selected columns
            assert output_columns == {"id", "name"}, \
                f"Expected only selected columns (id, name), got: {output_columns}"

    def test_columns_and_select_together(self):
        """
        Test using both columns (for casting) and select (for filtering) together.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = os.path.join(tmpdir, "output.csv")

            sample_data = [
                {"id": 1, "name": "Alice", "created_at": "2024-01-15", "value": 100},
                {"id": 2, "name": "Bob", "created_at": "2024-02-20", "value": 200},
            ]

            # Use select to filter AND columns to cast type
            sling = Sling(
                input=sample_data,
                tgt_object=f"file://{output_file}",
                select=["id", "name", "created_at"],  # Filter to these 3 columns
                columns={"created_at": "timestamp"},   # Cast created_at to timestamp
            )

            sling.run(print_output=False)

            import csv
            with open(output_file, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            output_columns = set(rows[0].keys())

            # Should have the 3 selected columns
            assert output_columns == {"id", "name", "created_at"}, \
                f"Expected selected columns (id, name, created_at), got: {output_columns}"

            # value should NOT be present (filtered out by select)
            assert 'value' not in output_columns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
