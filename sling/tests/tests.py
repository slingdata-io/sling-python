import os
import pytest
from sling import Replication, ReplicationStream, Pipeline

def test_replication_stream():
    # Test basic initialization
    stream = ReplicationStream(
        id="test_stream",
        mode="full-refresh",
        object="schema.table",
        primary_key=["id"],
        source_options={"header": True},
        target_options={"batch_limit": 1000}
    )
    
    assert stream.id == "test_stream"
    assert stream.mode == "full-refresh"
    assert stream.object == "schema.table"
    assert stream.primary_key == ["id"]
    assert stream.source_options.header == True
    assert stream.target_options.batch_limit == 1000

    # Test enable/disable
    assert stream.disabled == None
    stream.disable()
    assert stream.disabled == True
    stream.enable()
    assert stream.disabled == False

def test_replication():
    # Test basic initialization
    replication = Replication(
        source="postgres",
        target="snowflake",
        defaults=ReplicationStream(mode="full-refresh"),
        streams={
            "stream1": ReplicationStream(
                object="schema.table1",
                primary_key=["id"]
            ),
            "stream2": ReplicationStream(
                object="schema.table2",
                primary_key=["id"],
                disabled=True
            )
        },
        env={"MY_VAR": "value"},
        debug=True
    )

    assert replication.source == "postgres"
    assert replication.target == "snowflake"
    assert replication.defaults.mode == "full-refresh"
    assert len(replication.streams) == 2
    assert replication.env["MY_VAR"] == "value"
    assert replication.debug == True
    assert replication.streams["stream2"].disabled == True

    # Test stream management
    replication.add_streams({
        "stream3": ReplicationStream(
            object="schema.table3",
            primary_key=["id"]
        )
    })
    assert len(replication.streams) == 3

    replication.disable_streams(["stream1", "stream2"])
    assert replication.streams["stream1"].disabled == True
    assert replication.streams["stream2"].disabled == True
    assert replication.streams["stream3"].disabled == None

    replication.enable_streams(["stream1"])
    assert replication.streams["stream1"].disabled == False
    assert replication.streams["stream2"].disabled == True

    # Test mode setting
    replication.set_default_mode("incremental")
    assert replication.defaults.mode == "incremental"

    # Test command preparation
    cmd = replication._prep_cmd()
    assert "run -d -r" in cmd
    assert replication.temp_file.endswith(".json")

def test_pipeline():
    # Test basic initialization
    pipeline = Pipeline(
        steps=[
            {"type": "log", "message": "Step 1"},
            {"type": "command", "command": ["echo", "hello"]},
            {
                "type": "replication",
                "source": "postgres",
                "target": "snowflake",
                "streams": {
                    "stream1": {
                        "object": "schema.table1",
                        "mode": "full-refresh"
                    }
                }
            }
        ],
        env={"PIPELINE_VAR": "value"}
    )

    assert len(pipeline.steps) == 3
    assert pipeline.env["PIPELINE_VAR"] == "value"

    # Test command preparation
    cmd = pipeline._prep_cmd()
    assert "run -p" in cmd
    assert pipeline.temp_file.endswith(".yaml")

@pytest.fixture
def cleanup_temp_files():
    yield
    # Cleanup any temporary files after tests
    temp_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for file in os.listdir(temp_dir):
        if file.startswith('sling-') and (file.endswith('.json') or file.endswith('.yaml')):
            try:
                os.remove(os.path.join(temp_dir, file))
            except:
                pass

@pytest.mark.usefixtures("cleanup_temp_files")
def test_run_methods(monkeypatch):
    # Test Replication run
    replication = Replication(
        source="postgres",
        target="mysql",
        streams={"public.test1k_mariadb_pg": {
          "object": "mysql.test1k_mariadb_pg",
          "mode": "full-refresh",
        }}
    )
    output = replication.run(return_output=True)
    assert "execution succeeded" in output

    # Test Pipeline run
    pipeline = Pipeline(
        steps=[{"type": "log", "message": "testing now"}]
    )
    output = pipeline.run(return_output=True)
    assert "testing now" in output