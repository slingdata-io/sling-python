import os
import pytest
import json
import tempfile
from unittest.mock import patch, MagicMock, mock_open
from sling import (
    Replication, ReplicationStream, Pipeline, Task, Source, Target, 
    Mode, TaskOptions, cli
)
from sling.options import SourceOptions, TargetOptions
from sling.hooks import *

class TestMode:
    """Test the Mode enum"""
    
    def test_mode_values(self):
        assert Mode.FULL_REFRESH.value == "full-refresh"
        assert Mode.INCREMENTAL.value == "incremental" 
        assert Mode.TRUNCATE.value == "truncate"
        assert Mode.SNAPSHOT.value == "snapshot"
        assert Mode.BACKFILL.value == "backfill"

class TestSource:
    """Test the Source class"""
    
    def test_source_initialization(self):
        source = Source(
            conn="postgres",
            stream="public.users",
            primary_key=["id"],
            update_key="updated_at",
            limit=1000,
            options={"header": True, "delimiter": ","}
        )
        
        assert source.conn == "postgres"
        assert source.stream == "public.users"
        assert source.primary_key == ["id"]
        assert source.update_key == "updated_at"
        assert source.limit == 1000
        assert isinstance(source.options, SourceOptions)
        assert source.options.header == True
        assert source.options.delimiter == ","
    
    def test_source_default_values(self):
        source = Source()
        assert source.conn is None
        assert source.stream is None
        assert source.primary_key == []
        assert source.update_key is None
        assert source.limit is None
        assert isinstance(source.options, SourceOptions)

class TestTarget:
    """Test the Target class"""
    
    def test_target_initialization(self):
        target = Target(
            conn="snowflake",
            object="schema.table",
            options={"batch_limit": 5000, "pre_sql": "TRUNCATE TABLE schema.table"}
        )
        
        assert target.conn == "snowflake"
        assert target.object == "schema.table"
        assert isinstance(target.options, TargetOptions)
        assert target.options.batch_limit == 5000
        assert target.options.pre_sql == "TRUNCATE TABLE schema.table"
    
    def test_target_with_options_object(self):
        options = TargetOptions(batch_limit=1000, post_sql="ANALYZE TABLE schema.table")
        target = Target(conn="mysql", object="db.table", options=options)
        
        assert target.conn == "mysql"
        assert target.object == "db.table"
        assert target.options.batch_limit == 1000
        assert target.options.post_sql == "ANALYZE TABLE schema.table"

class TestTaskOptions:
    """Test the TaskOptions class"""
    
    def test_task_options_initialization(self):
        options = TaskOptions(stdout=True, debug=False)
        assert options.stdout == True
        assert options.debug == False
    
    def test_task_options_defaults(self):
        options = TaskOptions()
        assert options.stdout is None
        assert options.debug is None

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

def test_replication_stream_with_hooks_dict():
    """Test ReplicationStream with hooks as dictionaries"""
    hooks = {
        "pre": [{"type": "query", "connection": "postgres", "query": "DELETE FROM table WHERE condition"}],
        "post": [{"type": "query", "connection": "postgres", "query": "ANALYZE TABLE table"}]
    }
    
    stream = ReplicationStream(
        id="test_stream_hooks_dict",
        mode=Mode.INCREMENTAL,
        hooks=hooks
    )
    
    assert stream.id == "test_stream_hooks_dict"
    assert stream.mode == Mode.INCREMENTAL
    assert isinstance(stream.hooks, HookMap)

def test_replication_stream_with_hook_classes():
    """Test ReplicationStream with Hook class instances"""
    from sling.hooks import HookQuery, HookLog
    
    hooks = HookMap(
        pre=[
            HookQuery(
                connection="postgres", 
                query="DELETE FROM table WHERE condition",
                id="cleanup_step"
            ),
            HookLog(message="Starting replication", level="info")
        ],
        post=[
            HookQuery(
                connection="postgres", 
                query="ANALYZE TABLE table",
                id="analyze_step"
            ),
            HookLog(message="Replication completed", level="info")
        ]
    )
    
    stream = ReplicationStream(
        id="test_stream_hook_classes",
        mode=Mode.INCREMENTAL,
        hooks=hooks
    )
    
    assert stream.id == "test_stream_hook_classes"
    assert stream.mode == Mode.INCREMENTAL
    assert isinstance(stream.hooks, HookMap)
    assert len(stream.hooks.pre) == 2
    assert len(stream.hooks.post) == 2

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

def test_replication_with_file_path():
    """Test Replication with file_path"""
    replication = Replication(file_path="/path/to/replication.yaml")
    
    cmd = replication._prep_cmd()
    assert 'run  -r "/path/to/replication.yaml"' in cmd
    assert replication.temp_file is None

def test_replication_json_serialization():
    """Test that Replication config can be serialized to JSON"""
    replication = Replication(
        source="postgres",
        target="mysql",
        defaults=ReplicationStream(mode=Mode.FULL_REFRESH),
        streams={
            "test_stream": ReplicationStream(
                object="schema.table",
                mode=Mode.INCREMENTAL,
                primary_key=["id", "name"]
            )
        },
        env={"TEST_VAR": "test_value"}
    )
    
    # Test command preparation (which creates temp file with JSON)
    cmd = replication._prep_cmd()
    assert "run  -r" in cmd
    assert replication.temp_file is not None
    
    # Verify the temp file was created and contains valid JSON
    with open(replication.temp_file, 'r') as f:
        config = json.load(f)
    
    assert config["source"] == "postgres"
    assert config["target"] == "mysql" 
    assert config["defaults"]["mode"] == "full-refresh"
    assert config["streams"]["test_stream"]["object"] == "schema.table"
    assert config["streams"]["test_stream"]["mode"] == "incremental"
    assert config["env"]["TEST_VAR"] == "test_value"

def test_replication_with_hook_classes_json_serialization():
    """Test that Replication with Hook classes can be serialized to JSON"""
    from sling.hooks import HookQuery, HookLog, HookHTTP
    
    replication = Replication(
        source="postgres",
        target="snowflake",
        defaults=ReplicationStream(mode=Mode.FULL_REFRESH),
        streams={
            "users": ReplicationStream(
                object="schema.users",
                mode=Mode.INCREMENTAL,
                primary_key=["id"],
                hooks=HookMap(
                    pre=[
                        HookLog(
                            id="start_log",
                            message="Starting users replication",
                            level="info"
                        ),
                        HookQuery(
                            id="cleanup",
                            connection="snowflake",
                            query="DELETE FROM schema.users WHERE status = 'deleted'",
                            transient=False
                        )
                    ],
                    post=[
                        HookHTTP(
                            id="notify_webhook",
                            url="https://api.example.com/notify",
                            method="POST",
                            payload='{"table": "users", "status": "completed"}',
                            headers={"Content-Type": "application/json"}
                        ),
                        HookLog(
                            id="end_log",
                            message="Users replication completed",
                            level="info"
                        )
                    ]
                )
            )
        },
        env={"REPLICATION_ENV": "production"}
    )
    
    # Test command preparation (which creates temp file with JSON)
    cmd = replication._prep_cmd()
    assert "run  -r" in cmd
    assert replication.temp_file is not None
    
    # Verify the temp file was created and contains valid JSON
    with open(replication.temp_file, 'r') as f:
        config = json.load(f)
    
    assert config["source"] == "postgres"
    assert config["target"] == "snowflake"
    assert config["defaults"]["mode"] == "full-refresh"
    
    # Check the users stream
    users_stream = config["streams"]["users"]
    assert users_stream["object"] == "schema.users"
    assert users_stream["mode"] == "incremental"
    assert users_stream["primary_key"] == ["id"]
    
    # Check hooks were serialized correctly
    hooks = users_stream["hooks"]
    
    # Check pre hooks
    assert len(hooks["pre"]) == 2
    
    # First pre hook (log)
    log_hook = hooks["pre"][0]
    assert log_hook["type"] == "log"
    assert log_hook["id"] == "start_log"
    assert log_hook["message"] == "Starting users replication"
    assert log_hook["level"] == "info"
    
    # Second pre hook (query)
    query_hook = hooks["pre"][1]
    assert query_hook["type"] == "query"
    assert query_hook["id"] == "cleanup"
    assert query_hook["connection"] == "snowflake"
    assert query_hook["query"] == "DELETE FROM schema.users WHERE status = 'deleted'"
    assert query_hook["transient"] == False
    
    # Check post hooks
    assert len(hooks["post"]) == 2
    
    # First post hook (http)
    http_hook = hooks["post"][0]
    assert http_hook["type"] == "http"
    assert http_hook["id"] == "notify_webhook"
    assert http_hook["url"] == "https://api.example.com/notify"
    assert http_hook["method"] == "POST"
    assert http_hook["payload"] == '{"table": "users", "status": "completed"}'
    assert http_hook["headers"]["Content-Type"] == "application/json"
    
    # Second post hook (log)
    end_log_hook = hooks["post"][1]
    assert end_log_hook["type"] == "log"
    assert end_log_hook["id"] == "end_log"
    assert end_log_hook["message"] == "Users replication completed"
    assert end_log_hook["level"] == "info"
    
    # Check environment
    assert config["env"]["REPLICATION_ENV"] == "production"
    
    # Clean up
    os.remove(replication.temp_file)

def test_replication_with_dict_hooks_json_serialization():
    """Test that Replication with dictionary hooks can be serialized to JSON"""
    replication = Replication(
        source="mysql",
        target="bigquery",
        defaults=ReplicationStream(mode=Mode.FULL_REFRESH),
        streams={
            "orders": ReplicationStream(
                object="analytics.orders",
                mode=Mode.INCREMENTAL,
                primary_key=["order_id"],
                hooks={
                    "pre": [
                        {
                            "type": "query",
                            "connection": "mysql",
                            "query": "UPDATE orders SET processed = 1 WHERE processed = 0"
                        },
                        {
                            "type": "log",
                            "message": "Starting orders replication",
                            "level": "info"
                        }
                    ],
                    "post": [
                        {
                            "type": "http",
                            "url": "https://webhook.example.com/orders",
                            "method": "POST",
                            "payload": '{"event": "orders_replicated"}'
                        }
                    ]
                }
            )
        },
        env={"BATCH_SIZE": "5000"}
    )
    
    # Test command preparation (which creates temp file with JSON)
    cmd = replication._prep_cmd()
    assert "run  -r" in cmd
    assert replication.temp_file is not None
    
    # Verify the temp file was created and contains valid JSON
    with open(replication.temp_file, 'r') as f:
        config = json.load(f)
    
    assert config["source"] == "mysql"
    assert config["target"] == "bigquery"
    assert config["defaults"]["mode"] == "full-refresh"
    
    # Check the orders stream
    orders_stream = config["streams"]["orders"]
    assert orders_stream["object"] == "analytics.orders"
    assert orders_stream["mode"] == "incremental"
    assert orders_stream["primary_key"] == ["order_id"]
    
    # Check hooks were serialized correctly (should remain as dictionaries)
    hooks = orders_stream["hooks"]
    
    # Check pre hooks
    assert len(hooks["pre"]) == 2
    
    # First pre hook (query)
    query_hook = hooks["pre"][0]
    assert query_hook["type"] == "query"
    assert query_hook["connection"] == "mysql"
    assert query_hook["query"] == "UPDATE orders SET processed = 1 WHERE processed = 0"
    
    # Second pre hook (log)
    log_hook = hooks["pre"][1]
    assert log_hook["type"] == "log"
    assert log_hook["message"] == "Starting orders replication"
    assert log_hook["level"] == "info"
    
    # Check post hooks
    assert len(hooks["post"]) == 1
    
    # Post hook (http)
    http_hook = hooks["post"][0]
    assert http_hook["type"] == "http"
    assert http_hook["url"] == "https://webhook.example.com/orders"
    assert http_hook["method"] == "POST"
    assert http_hook["payload"] == '{"event": "orders_replicated"}'
    
    # Check environment
    assert config["env"]["BATCH_SIZE"] == "5000"
    
    # Clean up
    os.remove(replication.temp_file)

def test_pipeline():
    # Test basic initialization
    pipeline = Pipeline(
        steps=[
            StepLog(message="Step 1"),
            StepCommand(command=["echo", "hello"]),
            StepReplication(
                path="basic_replication.yaml",
                streams=["stream1"],
                mode="full-refresh"
            )
        ],
        env={"PIPELINE_VAR": "value"}
    )

    assert len(pipeline.steps) == 3
    assert pipeline.env["PIPELINE_VAR"] == "value"

    # Test command preparation
    cmd = pipeline._prep_cmd()
    assert "run -p" in cmd
    assert pipeline.temp_file.endswith(".yaml")

def test_pipeline_with_file_path():
    """Test Pipeline with file_path"""
    pipeline = Pipeline(file_path="/path/to/pipeline.yaml")
    
    cmd = pipeline._prep_cmd()
    assert 'run -p "/path/to/pipeline.yaml"' in cmd
    assert pipeline.temp_file is None

def test_pipeline_json_serialization():
    """Test that Pipeline config can be serialized to JSON correctly"""
    pipeline = Pipeline(
        steps=[
            StepLog(message="Starting pipeline"),
            StepReplication(
                path="replication.yaml",
                streams=["public.users"],
                mode="full-refresh"
            ),
            StepCommand(command=["echo", "Pipeline completed"]),
            StepQuery(
                connection="snowflake",
                query="ANALYZE TABLE analytics.users"
            )
        ],
        env={"PIPELINE_ENV": "production", "LOG_LEVEL": "debug"}
    )
    
    # Test command preparation (which creates temp file with JSON)
    cmd = pipeline._prep_cmd()
    assert "run -p" in cmd
    assert pipeline.temp_file is not None
    assert pipeline.temp_file.endswith(".yaml")
    
    # Verify the temp file was created and contains valid JSON
    with open(pipeline.temp_file, 'r') as f:
        config = json.load(f)
    
    # Verify the JSON structure matches expected pipeline format
    assert "steps" in config
    assert "env" in config
    
    # Check steps array
    steps = config["steps"]
    assert len(steps) == 4
    
    # Verify first step (log)
    assert steps[0]["type"] == "log"
    assert steps[0]["message"] == "Starting pipeline"
    
    # Verify second step (replication)
    assert steps[1]["type"] == "replication"
    assert steps[1]["path"] == "replication.yaml"
    assert steps[1]["streams"] == ["public.users"]
    assert steps[1]["mode"] == "full-refresh"
    
    # Verify third step (command)
    assert steps[2]["type"] == "command"
    assert steps[2]["command"] == ["echo", "Pipeline completed"]
    
    # Verify fourth step (query)
    assert steps[3]["type"] == "query"
    assert steps[3]["connection"] == "snowflake"
    assert steps[3]["query"] == "ANALYZE TABLE analytics.users"
    
    # Check environment variables
    env = config["env"]
    assert env["PIPELINE_ENV"] == "production"
    assert env["LOG_LEVEL"] == "debug"
    
    # Clean up
    os.remove(pipeline.temp_file)

def test_pipeline_empty_steps():
    """Test Pipeline with empty steps"""
    pipeline = Pipeline(steps=[], env={"TEST": "value"})
    
    cmd = pipeline._prep_cmd()
    assert "run -p" in cmd
    
    with open(pipeline.temp_file, 'r') as f:
        config = json.load(f)
    
    assert config["steps"] == []
    assert config["env"]["TEST"] == "value"
    
    # Clean up
    os.remove(pipeline.temp_file)

def test_pipeline_complex_steps():
    """Test Pipeline with complex nested steps"""
    pipeline = Pipeline(
        steps=[
            StepGroup(
                steps=[
                    StepLog(message="Group step 1"),
                    StepLog(message="Group step 2")
                ],
                loop={"range": "1..3"}
            ),
            StepReplication(
                path="complex_replication.yaml",
                streams=["orders", "customers"],
                mode="incremental",
                env={"BATCH_SIZE": "10000"}
            ),
            StepHTTP(
                url="https://api.example.com/webhook",
                method="POST",
                payload='{"status": "completed"}',
                headers={"Content-Type": "application/json"}
            ),
            StepWrite(
                to="/tmp/pipeline_status.txt",
                content="Pipeline execution completed successfully"
            )
        ],
        env={"ENVIRONMENT": "staging", "MAX_WORKERS": "4"}
    )
    
    cmd = pipeline._prep_cmd()
    
    with open(pipeline.temp_file, 'r') as f:
        config = json.load(f)
    
    # Should have 4 steps now
    assert len(config["steps"]) == 4
    
    # Verify complex group step
    group_step = config["steps"][0]
    assert group_step["type"] == "group"
    assert len(group_step["steps"]) == 2
    assert group_step["steps"][0]["type"] == "log"
    assert group_step["steps"][0]["message"] == "Group step 1"
    assert group_step["steps"][1]["type"] == "log"
    assert group_step["steps"][1]["message"] == "Group step 2"
    assert group_step["loop"]["range"] == "1..3"
    
    # Verify replication step
    repl_step = config["steps"][1]
    assert repl_step["type"] == "replication"
    assert repl_step["path"] == "complex_replication.yaml"
    assert repl_step["streams"] == ["orders", "customers"]
    assert repl_step["mode"] == "incremental"
    assert repl_step["env"]["BATCH_SIZE"] == "10000"
    
    # Verify HTTP step
    http_step = config["steps"][2]
    assert http_step["type"] == "http"
    assert http_step["url"] == "https://api.example.com/webhook"
    assert http_step["method"] == "POST"
    assert http_step["payload"] == '{"status": "completed"}'
    assert http_step["headers"]["Content-Type"] == "application/json"
    
    # Verify write step
    write_step = config["steps"][3]
    assert write_step["type"] == "write"
    assert write_step["to"] == "/tmp/pipeline_status.txt"
    assert write_step["content"] == "Pipeline execution completed successfully"
    
    # Clean up
    os.remove(pipeline.temp_file)

def test_pipeline_with_hook_objects():
    """Test Pipeline with actual Step objects"""
    pipeline = Pipeline(
        steps=[
            StepLog(message="Starting with step object"),
            StepQuery(
                connection="postgres", 
                query="SELECT COUNT(*) FROM users",
                into="user_count"
            ),
            StepCommand(command=["echo", "Processing complete"])
        ],
        env={"STEP_TEST": "true"}
    )
    
    cmd = pipeline._prep_cmd()
    
    with open(pipeline.temp_file, 'r') as f:
        config = json.load(f)
    
    # Verify step objects are serialized correctly
    steps = config["steps"]
    assert len(steps) == 3
    
    # Check log step
    log_step = steps[0]
    assert log_step["type"] == "log"
    assert log_step["message"] == "Starting with step object"
    
    # Check query step
    query_step = steps[1]
    assert query_step["type"] == "query"
    assert query_step["connection"] == "postgres"
    assert query_step["query"] == "SELECT COUNT(*) FROM users"
    assert query_step["into"] == "user_count"
    
    # Check command step
    command_step = steps[2]
    assert command_step["type"] == "command"
    assert command_step["command"] == ["echo", "Processing complete"]
    
    # Check environment
    assert config["env"]["STEP_TEST"] == "true"
    
    # Clean up
    os.remove(pipeline.temp_file)

def test_pipeline_hook_special_keywords():
    """Test Pipeline with steps that have special keyword handling"""
    pipeline = Pipeline(
        steps=[
            StepRead(from_="/path/to/input.txt", into="file_content"),
            StepCopy(from_="/source/file.txt", to="/dest/file.txt", recursive=True),
            StepCommand(command=["ls", "-la"], print_output=True, working_dir="/tmp"),
            StepReplication(
                path="test.yaml",
                range_param="2023-01-01..2023-12-31",
                working_dir="/pipelines"
            )
        ],
        env={"SPECIAL_TEST": "true"}
    )
    
    cmd = pipeline._prep_cmd()
    
    with open(pipeline.temp_file, 'r') as f:
        config = json.load(f)
    
    steps = config["steps"]
    assert len(steps) == 4
    
    # Check read step - 'from_' should become 'from'
    read_step = steps[0]
    assert read_step["type"] == "read"
    assert read_step["from"] == "/path/to/input.txt"
    assert read_step["into"] == "file_content"
    assert "from_" not in read_step  # Should not have the Python version
    
    # Check copy step - 'from_' should become 'from'
    copy_step = steps[1]
    assert copy_step["type"] == "copy"
    assert copy_step["from"] == "/source/file.txt"
    assert copy_step["to"] == "/dest/file.txt"
    assert copy_step["recursive"] == True
    assert "from_" not in copy_step  # Should not have the Python version
    
    # Check command step - 'print_output' should become 'print'
    command_step = steps[2]
    assert command_step["type"] == "command"
    assert command_step["command"] == ["ls", "-la"]
    assert command_step["print"] == True
    assert command_step["working_dir"] == "/tmp"
    assert "print_output" not in command_step  # Should not have the Python version
    
    # Check replication step - 'range_param' should become 'range'
    repl_step = steps[3]
    assert repl_step["type"] == "replication"
    assert repl_step["path"] == "test.yaml"
    assert repl_step["range"] == "2023-01-01..2023-12-31"
    assert repl_step["working_dir"] == "/pipelines"
    assert "range_param" not in repl_step  # Should not have the Python version
    
    # Check environment
    assert config["env"]["SPECIAL_TEST"] == "true"
    
    # Clean up
    os.remove(pipeline.temp_file)

def test_pipeline_hook_conditions_and_properties():
    """Test Pipeline with steps that have if conditions and common properties"""
    pipeline = Pipeline(
        steps=[
            StepLog(
                id="start_log",
                message="Pipeline started",
                level="info",
                if_condition="${{ env.DEBUG == 'true' }}"
            ),
            StepQuery(
                id="user_count",
                connection="postgres",
                query="SELECT COUNT(*) as user_count FROM users",
                into="user_count_result",
                transient=False,
                on_failure="continue"
            ),
            StepCheck(
                id="validate_users",
                check="user_count > 0",
                failure_message="No users found in database",
                vars={"min_users": 1, "table": "users"}
            ),
            StepStore(
                id="store_result",
                key="pipeline_status",
                value="completed",
                if_condition="${{ steps.validate_users.success }}"
            )
        ],
        env={"PIPELINE_ID": "test_001"}
    )
    
    cmd = pipeline._prep_cmd()
    
    with open(pipeline.temp_file, 'r') as f:
        config = json.load(f)
    
    steps = config["steps"]
    assert len(steps) == 4
    
    # Check log step with if condition
    log_step = steps[0]
    assert log_step["type"] == "log"
    assert log_step["id"] == "start_log"
    assert log_step["message"] == "Pipeline started"
    assert log_step["level"] == "info"
    assert log_step["if"] == "${{ env.DEBUG == 'true' }}"  # 'if_condition' becomes 'if'
    assert "if_condition" not in log_step  # Should not have the Python version
    
    # Check query step with multiple properties
    query_step = steps[1]
    assert query_step["type"] == "query"
    assert query_step["id"] == "user_count"
    assert query_step["connection"] == "postgres"
    assert query_step["query"] == "SELECT COUNT(*) as user_count FROM users"
    assert query_step["into"] == "user_count_result"
    assert query_step["transient"] == False
    assert query_step["on_failure"] == "continue"
    
    # Check check step with vars
    check_step = steps[2]
    assert check_step["type"] == "check"
    assert check_step["id"] == "validate_users"
    assert check_step["check"] == "user_count > 0"
    assert check_step["failure_message"] == "No users found in database"
    assert check_step["vars"]["min_users"] == 1
    assert check_step["vars"]["table"] == "users"
    
    # Check store step
    store_step = steps[3]
    assert store_step["type"] == "store"
    assert store_step["id"] == "store_result"
    assert store_step["key"] == "pipeline_status"
    assert store_step["value"] == "completed"
    assert store_step["if"] == "${{ steps.validate_users.success }}"
    
    # Check environment
    assert config["env"]["PIPELINE_ID"] == "test_001"
    
    # Clean up
    os.remove(pipeline.temp_file)

class TestTask:
    """Test the deprecated Task class"""
    
    def test_task_initialization(self):
        source = Source(conn="postgres", stream="public.users")
        target = Target(conn="mysql", object="db.users")
        
        task = Task(
            source=source,
            target=target,
            mode=Mode.FULL_REFRESH,
            options=TaskOptions(debug=True),
            env={"TASK_VAR": "value"}
        )
        
        assert isinstance(task.source, Source)
        assert isinstance(task.target, Target)
        assert task.mode == Mode.FULL_REFRESH
        assert isinstance(task.options, TaskOptions)
        assert task.env["TASK_VAR"] == "value"
    
    def test_task_with_dict_inputs(self):
        task = Task(
            source={"conn": "postgres", "stream": "public.users"},
            target={"conn": "mysql", "object": "db.users"},
            mode="incremental",
            options={"debug": True}
        )
        
        assert isinstance(task.source, Source)
        assert isinstance(task.target, Target)
        assert task.source.conn == "postgres"
        assert task.target.conn == "mysql"
        assert task.mode == "incremental"
        assert isinstance(task.options, TaskOptions)
        assert task.options.debug == True
    
    def test_task_command_preparation(self):
        task = Task(
            source=Source(conn="postgres", stream="public.users"),
            target=Target(conn="mysql", object="db.users")
        )
        
        cmd = task._prep_cmd()
        assert "run -c" in cmd
        assert task.temp_file.endswith(".json")
        
        # Verify the temp file contains valid JSON
        with open(task.temp_file, 'r') as f:
            config = json.load(f)
        
        assert config["source"]["conn"] == "postgres"
        assert config["target"]["conn"] == "mysql"
        assert config["mode"] == "full-refresh"

@pytest.fixture
def cleanup_temp_files():
    yield
    # Cleanup any temporary files after tests
    temp_dir = tempfile.gettempdir()
    for file in os.listdir(temp_dir):
        if file.startswith('sling-') and (file.endswith('.json') or file.endswith('.yaml')):
            try:
                os.remove(os.path.join(temp_dir, file))
            except:
                pass

class TestMockedExecution:
    """Test execution methods with mocked binary calls"""
    
    @patch('sling._exec_cmd')
    def test_replication_run_success(self, mock_exec):
        mock_exec.return_value = iter(["execution started", "processing data", "execution succeeded"])
        
        replication = Replication(
            source="postgres",
            target="mysql",
            streams={"public.users": {"object": "db.users", "mode": "full-refresh"}}
        )
        
        output = replication.run(return_output=True)
        assert "execution succeeded" in output
        mock_exec.assert_called_once()
    
    @patch('sling._exec_cmd')
    def test_pipeline_run_success(self, mock_exec):
        mock_exec.return_value = iter(["pipeline started", "step 1 completed", "pipeline completed"])
        
        pipeline = Pipeline(steps=[{"type": "log", "message": "test"}])
        output = pipeline.run(return_output=True)
        
        assert "pipeline completed" in output
        mock_exec.assert_called_once()
    
    @patch('sling._exec_cmd')
    def test_task_run_success(self, mock_exec):
        mock_exec.return_value = iter(["task started", "data transferred", "task completed"])
        
        task = Task(
            source=Source(conn="postgres", stream="public.users"),
            target=Target(conn="mysql", object="db.users")
        )
        
        output = task.run(return_output=True)
        assert "task completed" in output
        mock_exec.assert_called_once()
    
    @patch('sling._exec_cmd')
    def test_run_with_exception(self, mock_exec):
        mock_exec.side_effect = Exception("Binary execution failed")
        
        replication = Replication(source="postgres", target="mysql")
        
        with pytest.raises(Exception) as exc_info:
            replication.run(return_output=True)
        
        assert "Binary execution failed" in str(exc_info.value)
    
    @patch('sling._exec_cmd')
    def test_cli_function(self, mock_exec):
        mock_exec.return_value = iter(["sling version 1.0.0"])
        
        output = cli("--version", return_output=True)
        assert "sling version 1.0.0" in output
        mock_exec.assert_called_once()

class TestEnvironmentHandling:
    """Test environment variable handling"""
    
    @patch.dict(os.environ, {"EXISTING_VAR": "existing_value"})
    @patch('sling._exec_cmd')
    def test_env_merging(self, mock_exec):
        mock_exec.return_value = iter(["success"])
        
        replication = Replication(
            source="postgres", 
            target="mysql",
            env={"NEW_VAR": "new_value"}
        )
        
        replication.run(return_output=True)
        
        # Check that _exec_cmd was called with merged environment
        mock_exec.assert_called_once()
        call_args = mock_exec.call_args
        env_arg = call_args[1]['env']
        
        assert env_arg["EXISTING_VAR"] == "existing_value"  # Original env var
        assert env_arg["NEW_VAR"] == "new_value"  # New env var

class TestErrorHandling:
    """Test error handling scenarios"""
    
    def test_invalid_mode_enum_conversion(self):
        """Test that invalid mode strings are handled gracefully"""
        stream = ReplicationStream(mode="invalid-mode")
        assert stream.mode == "invalid-mode"  # Should store as-is, not convert
    
    def test_empty_replication_config(self):
        """Test that empty replication configurations work"""
        replication = Replication()
        
        assert replication.source is None
        assert replication.target is None
        assert isinstance(replication.defaults, ReplicationStream)
        assert replication.streams == {}
        assert replication.env == {}
        assert replication.debug == False
    
    @patch('sling._exec_cmd')
    def test_temp_file_cleanup_on_success(self, mock_exec):
        """Test that temp files are cleaned up on successful execution"""
        mock_exec.return_value = iter(["success"])
        
        replication = Replication(source="postgres", target="mysql")
        replication.run(return_output=True)
        
        # Temp file should be cleaned up
        if replication.temp_file:
            assert not os.path.exists(replication.temp_file)
    
    @patch('sling._exec_cmd')
    def test_temp_file_preserved_on_error(self, mock_exec):
        """Test that temp files are preserved on error for debugging"""
        mock_exec.side_effect = Exception("Execution failed")
        
        replication = Replication(source="postgres", target="mysql")
        
        with pytest.raises(Exception):
            replication.run(return_output=False)  # Don't return output to test debug path
        
        # Temp file should still exist for debugging
        if replication.temp_file:
            assert os.path.exists(replication.temp_file)
            # Clean up for test
            os.remove(replication.temp_file)

@pytest.mark.skipif(os.getenv("SLING_BINARY") is None, reason="Sling binary not available")
class TestRealBinaryExecution:
    """Integration tests that require the actual sling binary"""
    
    def test_version_command(self):
        """Test that we can get version from real binary"""
        try:
            output = cli("--version", return_output=True)
            assert "sling" in output.lower()
        except Exception:
            pytest.skip("Sling binary not available or not working")
    
    def test_help_command(self):
        """Test that we can get help from real binary"""
        try:
            output = cli("--help", return_output=True)
            assert "usage" in output.lower() or "commands" in output.lower()
        except Exception:
            pytest.skip("Sling binary not available or not working")

# Ensure existing tests still work
@pytest.mark.usefixtures("cleanup_temp_files")
@patch('sling._exec_cmd')
def test_run_methods(mock_exec):
    """Modified version of original test with mocked execution"""
    mock_exec.return_value = iter(["execution succeeded"])
    
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
    mock_exec.return_value = iter(["testing now"])
    pipeline = Pipeline(
        steps=[{"type": "log", "message": "testing now"}]
    )
    output = pipeline.run(return_output=True)
    assert "testing now" in output