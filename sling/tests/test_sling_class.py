import os
import pytest
import tempfile
import json
import csv
from pathlib import Path
from sling.bin import SLING_BIN
from sling import Sling, SlingError


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir


@pytest.fixture
def sample_data():
    """Sample data for testing"""
    return [
        {"id": 1, "name": "John Doe", "age": 30, "city": "New York", "salary": 50000.0},
        {"id": 2, "name": "Jane Smith", "age": 25, "city": "Los Angeles", "salary": 60000.0},
        {"id": 3, "name": "Bob Johnson", "age": 35, "city": "Chicago", "salary": 55000.0},
        {"id": 4, "name": "Alice Brown", "age": 28, "city": "Houston", "salary": 65000.0},
        {"id": 5, "name": "Charlie Wilson", "age": 32, "city": "Phoenix", "salary": 58000.0},
    ]


@pytest.fixture
def sample_data_with_nulls():
    """Sample data with missing/null values for testing edge cases"""
    return [
        {"id": 1, "name": "John", "age": 30, "city": "New York"},
        {"id": 2, "name": "Jane", "city": "LA"},  # missing age
        {"id": 3, "age": 35, "city": "Chicago"},  # missing name
        {"id": 4, "name": "", "age": None, "city": "Houston"},  # empty/null values
    ]


class TestSlingClassBasic:
    """Basic functionality tests for the Sling class"""
    
    def test_sling_class_exists(self):
        """Test that Sling class can be imported and instantiated"""
        sling = Sling()
        assert sling is not None
        assert hasattr(sling, 'run')
    
    def test_parameter_assignment(self):
        """Test that parameters are correctly assigned"""
        sling = Sling(
            src_conn="postgres://test",
            src_stream="users", 
            tgt_conn="file:///tmp/test.csv",
            debug=True,
            limit=100
        )
        
        assert sling.src_conn == "postgres://test"
        assert sling.src_stream == "users"
        assert sling.tgt_conn == "file:///tmp/test.csv"
        assert sling.debug is True
        assert sling.limit == 100


@pytest.mark.skipif(not os.path.exists(SLING_BIN), reason="Sling binary not available")
class TestSlingInputStreaming:
    """Test input streaming functionality"""
    
    def test_input_to_csv_file(self, temp_dir, sample_data):
        """Test streaming Python data to CSV file"""
        output_file = os.path.join(temp_dir, "output.csv")
        
        sling = Sling(
            input=sample_data,
            tgt_object=f"file://{output_file}",
            debug=True
        )
        
        sling.run()
        
        # Verify the file was created and has correct content
        assert os.path.exists(output_file)
        
        with open(output_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        assert len(rows) == len(sample_data)
        assert rows[0]['name'] == 'John Doe'
        assert rows[0]['age'] == '30'
        assert rows[1]['name'] == 'Jane Smith'
    
    def test_input_to_json_file(self, temp_dir, sample_data):
        """Test streaming Python data to JSON file"""
        output_file = os.path.join(temp_dir, "output.json")
        
        sling = Sling(
            input=sample_data,
            tgt_object=f"file://{output_file}",
            tgt_options={"format": "json"},
            debug=True
        )
        
        sling.run()
        
        # Verify the file was created and has correct content
        assert os.path.exists(output_file)
        
        with open(output_file, 'r') as f:
            content = f.read()
        
        # JSON format creates a JSON array
        data = json.loads(content)
        assert isinstance(data, list)
        assert len(data) == len(sample_data)
        assert data[0]['name'] == 'John Doe'
        assert data[0]['age'] == 30
        
    def test_input_to_jsonlines_file(self, temp_dir, sample_data):
        """Test streaming Python data to JSON Lines file"""
        output_file = os.path.join(temp_dir, "output.jsonl")
        
        sling = Sling(
            input=sample_data,
            tgt_object=f"file://{output_file}",
            tgt_options={"format": "jsonlines"},
            debug=True
        )
        
        sling.run()
        
        # Verify the file was created and has correct content
        assert os.path.exists(output_file)
        
        with open(output_file, 'r') as f:
            lines = f.readlines()
        
        # Should be JSON Lines format
        assert len(lines) == len(sample_data)
        first_record = json.loads(lines[0])
        assert first_record['name'] == 'John Doe'
        assert first_record['age'] == 30
    
    def test_input_with_transforms(self, temp_dir, sample_data):
        """Test input streaming with transforms"""
        output_file = os.path.join(temp_dir, "transformed.csv")
        
        sling = Sling(
            input=sample_data,
            select=["id", "name", "age"],  # Select only some columns
            tgt_object=f"file://{output_file}",
            debug=True
        )
        
        sling.run()
        
        # Verify only selected columns are present
        with open(output_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        assert len(rows) == len(sample_data)
        # Should only have the selected columns
        assert set(rows[0].keys()) == {"id", "name", "age"}
        assert "city" not in rows[0]
        assert "salary" not in rows[0]
    
    def test_input_with_missing_fields(self, temp_dir, sample_data_with_nulls):
        """Test input streaming with records that have missing fields"""
        output_file = os.path.join(temp_dir, "nulls.csv")
        
        sling = Sling(
            input=sample_data_with_nulls,
            tgt_object=f"file://{output_file}",
            debug=True
        )
        
        sling.run()
        
        # Verify the file was created and handles nulls properly
        assert os.path.exists(output_file)
        
        with open(output_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        assert len(rows) == len(sample_data_with_nulls)
        # Fields should be empty string where missing
        assert rows[1]['age'] == ''  # Jane missing age
        assert rows[2]['name'] == ''  # Bob missing name
    
    def test_input_large_dataset(self, temp_dir):
        """Test streaming a larger dataset"""
        # Generate larger dataset
        large_data = [
            {"id": i, "value": f"item_{i}", "number": i * 1.5}
            for i in range(1000)
        ]
        
        output_file = os.path.join(temp_dir, "large.csv")
        
        sling = Sling(
            input=large_data,
            tgt_object=f"file://{output_file}",
            debug=False  # Reduce noise for large dataset
        )
        
        sling.run()
        
        # Verify the file was created and has correct row count
        assert os.path.exists(output_file)
        
        with open(output_file, 'r') as f:
            line_count = sum(1 for line in f) - 1  # Subtract header
        
        assert line_count == 1000
    
    def test_input_generator_function(self, temp_dir):
        """Test streaming from a generator function"""
        def data_generator():
            for i in range(100):
                yield {"id": i, "data": f"generated_{i}"}
        
        output_file = os.path.join(temp_dir, "generated.csv")
        
        sling = Sling(
            input=data_generator(),
            tgt_object=f"file://{output_file}",
            debug=True
        )
        
        sling.run()
        
        # Verify the file was created
        assert os.path.exists(output_file)
        
        with open(output_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        assert len(rows) == 100
        assert rows[0]['data'] == 'generated_0'
        assert rows[99]['data'] == 'generated_99'


@pytest.mark.skipif(not os.path.exists(SLING_BIN), reason="Sling binary not available")
class TestSlingOutputStreaming:
    """Test output streaming functionality"""
    
    def test_csv_output_streaming(self, temp_dir, sample_data):
        """Test streaming CSV file output to Python iterator"""
        # First create a CSV file to read from
        input_file = os.path.join(temp_dir, "input.csv")
        with open(input_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=sample_data[0].keys())
            writer.writeheader()
            writer.writerows(sample_data)
        
        # Now read it back using output streaming
        sling = Sling(
            src_conn=f"file://{input_file}",
            debug=True
        )
        
        records = list(sling.stream())
        
        assert len(records) == len(sample_data)
        assert records[0]['name'] == 'John Doe'
        # With Arrow format, types are preserved; with CSV, everything is strings
        assert records[0]['age'] in (30, '30')  # Accept both int and string
        assert records[1]['name'] == 'Jane Smith'
    
    def test_json_output_streaming(self, temp_dir, sample_data):
        """Test streaming JSON file output to Python iterator"""
        # First create a JSON Lines file to read from
        input_file = os.path.join(temp_dir, "input.json")
        with open(input_file, 'w') as f:
            for record in sample_data:
                f.write(json.dumps(record) + '\n')
        
        # Now read it back using output streaming
        # When reading JSON files, sling outputs each JSON line as a string in a 'data' column
        sling = Sling(
            src_conn=f"file://{input_file}",
            debug=True
        )
        
        records = list(sling.stream())
        
        assert len(records) == len(sample_data)
        # JSON lines are returned as strings in the 'data' column
        assert 'data' in records[0]
        # Parse the JSON string
        first_record = json.loads(records[0]['data'])
        assert first_record['name'] == 'John Doe'
        assert first_record['age'] == 30
    
    def test_output_streaming_with_limit(self, temp_dir, sample_data):
        """Test output streaming with limit parameter"""
        # Create input file
        input_file = os.path.join(temp_dir, "input.csv")
        with open(input_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=sample_data[0].keys())
            writer.writeheader()
            writer.writerows(sample_data)
        
        # Read with limit
        sling = Sling(
            src_conn=f"file://{input_file}",
            limit=3,
            debug=True
        )
        
        records = list(sling.stream())
        
        assert len(records) == 3  # Should be limited to 3 records
        assert records[0]['name'] == 'John Doe'
    
    def test_output_streaming_with_select(self, temp_dir, sample_data):
        """Test output streaming with column selection"""
        # Create input file
        input_file = os.path.join(temp_dir, "input.csv")
        with open(input_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=sample_data[0].keys())
            writer.writeheader()
            writer.writerows(sample_data)
        
        # Read with column selection
        sling = Sling(
            src_conn=f"file://{input_file}",
            select=["id", "name"],
            debug=True
        )
        
        records = list(sling.stream())
        
        assert len(records) == len(sample_data)
        # Should only have selected columns
        assert set(records[0].keys()) == {"id", "name"}
        assert records[0]['name'] == 'John Doe'
    
    def test_output_streaming_early_break(self, temp_dir, sample_data):
        """Test that output streaming can be broken early"""
        # Create input file
        input_file = os.path.join(temp_dir, "input.csv")
        with open(input_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=sample_data[0].keys())
            writer.writeheader()
            writer.writerows(sample_data)
        
        # Read and break early
        sling = Sling(
            src_conn=f"file://{input_file}",
            debug=True
        )
        
        records = []
        for i, record in enumerate(sling.stream()):
            records.append(record)
            if i >= 1:  # Break after 2 records
                break
        
        assert len(records) == 2
        assert records[0]['name'] == 'John Doe'
        assert records[1]['name'] == 'Jane Smith'


@pytest.mark.skipif(not os.path.exists(SLING_BIN), reason="Sling binary not available")
class TestSlingRoundTrip:
    """Test complete roundtrip: Python data → file → Python data"""
    
    def test_roundtrip_csv(self, temp_dir, sample_data):
        """Test complete roundtrip through CSV"""
        temp_file = os.path.join(temp_dir, "roundtrip.csv")
        
        # Step 1: Python data → CSV file
        sling_write = Sling(
            input=sample_data,
            tgt_object=f"file://{temp_file}",
            debug=True
        )
        sling_write.run()
        
        # Step 2: CSV file → Python data
        sling_read = Sling(
            src_conn=f"file://{temp_file}",
            debug=True
        )
        
        result_data = list(sling_read.stream())
        
        # Verify data integrity
        # With Arrow format, types are preserved; with CSV, everything is strings
        assert len(result_data) == len(sample_data)
        assert result_data[0]['name'] == sample_data[0]['name']
        # Accept both typed and string values
        assert result_data[0]['id'] in (sample_data[0]['id'], str(sample_data[0]['id']))
        assert result_data[0]['age'] in (sample_data[0]['age'], str(sample_data[0]['age']))
    
    def test_roundtrip_json(self, temp_dir, sample_data):
        """Test complete roundtrip through JSON"""
        temp_file = os.path.join(temp_dir, "roundtrip.json")
        
        # Step 1: Python data → JSON file (using jsonlines for proper roundtrip)
        sling_write = Sling(
            input=sample_data,
            tgt_object=f"file://{temp_file}",
            tgt_options={"format": "jsonlines"},
            debug=True
        )
        sling_write.run()
        
        # Step 2: JSON file → Python data (via CSV output)
        sling_read = Sling(
            src_conn=f"file://{temp_file}",
            debug=True
        )
        
        result_data = list(sling_read.stream())
        
        # Verify data integrity
        # JSON lines are returned as strings in the 'data' column
        assert len(result_data) == len(sample_data)
        assert 'data' in result_data[0]
        # Parse the JSON string
        first_record = json.loads(result_data[0]['data'])
        assert first_record['name'] == sample_data[0]['name']
    
    def test_roundtrip_with_transforms(self, temp_dir, sample_data):
        """Test roundtrip with transformations"""
        temp_file = os.path.join(temp_dir, "transformed_roundtrip.csv")
        
        # Step 1: Python data → CSV file (with selection)
        sling_write = Sling(
            input=sample_data,
            select=["id", "name", "age"],
            tgt_object=f"file://{temp_file}",
            debug=True
        )
        sling_write.run()
        
        # Step 2: CSV file → Python data (with limit)
        sling_read = Sling(
            src_conn=f"file://{temp_file}",
            limit=3,
            debug=True
        )
        
        result_data = list(sling_read.stream())
        
        # Verify transformations were applied
        assert len(result_data) == 3  # Limited to 3
        assert set(result_data[0].keys()) == {"id", "name", "age"}  # Only selected columns
        assert "city" not in result_data[0]
        assert "salary" not in result_data[0]


@pytest.mark.skipif(not os.path.exists(SLING_BIN), reason="Sling binary not available")
class TestSlingErrorHandling:
    """Test error handling and edge cases"""
    
    def test_invalid_target_connection(self, sample_data):
        """Test error handling for invalid target connection"""
        sling = Sling(
            input=sample_data,
            tgt_conn="invalid://connection/path",
            debug=True
        )
        
        with pytest.raises(SlingError):
            sling.run()
    
    def test_invalid_source_connection(self):
        """Test error handling for invalid source connection"""
        sling = Sling(
            src_conn="invalid://connection/path",
            debug=True
        )
        
        with pytest.raises(SlingError):
            list(sling.stream())
    
    def test_empty_input_data(self, temp_dir):
        """Test handling of empty input data"""
        output_file = os.path.join(temp_dir, "empty.csv")
        
        sling = Sling(
            input=[],  # Empty list
            tgt_object=f"file://{output_file}",
            debug=True
        )
        
        sling.run()
        
        # Should create an empty file or file with just headers
        assert os.path.exists(output_file)
    
    def test_nonexistent_source_file(self):
        """Test error handling for nonexistent source file"""
        sling = Sling(
            src_conn="file:///nonexistent/path/file.csv",
            debug=True
        )
        
        with pytest.raises(SlingError):
            list(sling.stream())




if __name__ == "__main__":
    # Run tests if executed directly
    pytest.main([__file__, "-v"]) 