import os
import json
import tempfile
import pytest
from pathlib import Path

from sling.api_spec import (
    ApiSpec, Endpoint, Request, Pagination, Response, Records,
    Processor, Rule, Iterate, Call, DynamicEndpoint,
    AuthType, OAuthFlow, HTTPMethod, RuleAction, AggregationType,
    BackoffType, ResponseFormat,
)

SAMPLE_YAML = Path(__file__).parent / "sample_api_spec.yaml"


# ──────────────────────────────────────────────────────────────
# Enum tests
# ──────────────────────────────────────────────────────────────

class TestEnums:
    def test_auth_type_values(self):
        assert AuthType.NONE.value == ""
        assert AuthType.BASIC.value == "basic"
        assert AuthType.OAUTH2.value == "oauth2"
        assert AuthType.AWS_SIGV4.value == "aws-sigv4"
        assert AuthType.HMAC.value == "hmac"
        assert AuthType.MTLS.value == "mtls"
        assert AuthType.STATIC.value == "static"
        assert AuthType.SEQUENCE.value == "sequence"

    def test_oauth_flow_values(self):
        assert OAuthFlow.CLIENT_CREDENTIALS.value == "client_credentials"
        assert OAuthFlow.AUTHORIZATION_CODE.value == "authorization_code"
        assert OAuthFlow.DEVICE_CODE.value == "device_code"

    def test_http_method_values(self):
        for m in ("GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "CONNECT", "OPTIONS", "TRACE"):
            assert HTTPMethod[m].value == m

    def test_aggregation_type_values(self):
        assert AggregationType.NONE.value == ""
        assert AggregationType.MAXIMUM.value == "maximum"
        assert AggregationType.MINIMUM.value == "minimum"
        assert AggregationType.COLLECT.value == "collect"
        assert AggregationType.FIRST.value == "first"
        assert AggregationType.LAST.value == "last"

    def test_rule_action_values(self):
        assert RuleAction.RETRY.value == "retry"
        assert RuleAction.CONTINUE.value == "continue"
        assert RuleAction.STOP.value == "stop"
        assert RuleAction.BREAK.value == "break"
        assert RuleAction.SKIP.value == "skip"
        assert RuleAction.FAIL.value == "fail"

    def test_backoff_type_values(self):
        assert BackoffType.NONE.value == ""
        assert BackoffType.CONSTANT.value == "constant"
        assert BackoffType.LINEAR.value == "linear"
        assert BackoffType.EXPONENTIAL.value == "exponential"
        assert BackoffType.JITTER.value == "jitter"

    def test_response_format_values(self):
        assert ResponseFormat.CSV.value == "csv"
        assert ResponseFormat.JSON.value == "json"
        assert ResponseFormat.JSONLINES.value == "jsonlines"
        assert ResponseFormat.PARQUET.value == "parquet"
        assert ResponseFormat.AVRO.value == "avro"
        assert ResponseFormat.XML.value == "xml"
        assert ResponseFormat.EXCEL.value == "excel"
        assert ResponseFormat.SAS.value == "sas"
        assert ResponseFormat.TEXT.value == "text"


# ──────────────────────────────────────────────────────────────
# Request
# ──────────────────────────────────────────────────────────────

class TestRequest:
    def test_all_fields(self):
        r = Request(
            url="http://example.com",
            timeout=30,
            method=HTTPMethod.POST,
            headers={"Accept": "application/json"},
            parameters={"limit": "100"},
            payload={"query": "active"},
            rate=10,
            concurrency=3,
        )
        d = r.to_dict()
        assert d["url"] == "http://example.com"
        assert d["timeout"] == 30
        assert d["method"] == "POST"
        assert d["headers"] == {"Accept": "application/json"}
        assert d["parameters"] == {"limit": "100"}
        assert d["payload"] == {"query": "active"}
        assert d["rate"] == 10
        assert d["concurrency"] == 3

    def test_none_fields_omitted(self):
        r = Request(url="http://example.com")
        d = r.to_dict()
        assert d == {"url": "http://example.com"}

    def test_method_as_string(self):
        r = Request(method="GET")
        assert r.to_dict()["method"] == "GET"

    def test_method_as_enum(self):
        r = Request(method=HTTPMethod.PUT)
        assert r.to_dict()["method"] == "PUT"

    def test_empty_parameters_preserved(self):
        r = Request(parameters={})
        assert r.to_dict() == {"parameters": {}}


# ──────────────────────────────────────────────────────────────
# Pagination
# ──────────────────────────────────────────────────────────────

class TestPagination:
    def test_all_fields(self):
        p = Pagination(
            next_state={"offset": "{state.offset + 100}"},
            stop_condition="length(response.records) == 0",
        )
        d = p.to_dict()
        assert d["next_state"] == {"offset": "{state.offset + 100}"}
        assert d["stop_condition"] == "length(response.records) == 0"

    def test_empty_pagination_overrides(self):
        p = Pagination(next_state={}, stop_condition=None)
        d = p.to_dict()
        assert d == {"next_state": {}}


# ──────────────────────────────────────────────────────────────
# Records
# ──────────────────────────────────────────────────────────────

class TestRecords:
    def test_all_fields_jmespath(self):
        r = Records(
            jmespath="data[]",
            primary_key=["id", "type"],
            update_key="updated_at",
            limit=5000,
            casing="snake",
            select=["*", "full_name as name"],
            duplicate_tolerance="100000,0.001",
        )
        d = r.to_dict()
        assert d["jmespath"] == "data[]"
        assert d["primary_key"] == ["id", "type"]
        assert d["update_key"] == "updated_at"
        assert d["limit"] == 5000
        assert d["casing"] == "snake"
        assert d["select"] == ["*", "full_name as name"]
        assert d["duplicate_tolerance"] == "100000,0.001"
        assert "jq" not in d

    def test_all_fields_jq(self):
        r = Records(jq=".data[]", primary_key=["id"])
        d = r.to_dict()
        assert d["jq"] == ".data[]"
        assert "jmespath" not in d

    def test_string_primary_key(self):
        r = Records(primary_key="id")
        assert r.to_dict()["primary_key"] == "id"


# ──────────────────────────────────────────────────────────────
# Processor
# ──────────────────────────────────────────────────────────────

class TestProcessor:
    def test_all_fields(self):
        p = Processor(
            expression="record.updated_at",
            output="state.last_updated",
            aggregation=AggregationType.MAXIMUM,
            if_condition="!is_null(record.updated_at)",
        )
        d = p.to_dict()
        assert d["expression"] == "record.updated_at"
        assert d["output"] == "state.last_updated"
        assert d["aggregation"] == "maximum"
        assert d["if"] == "!is_null(record.updated_at)"
        assert "if_condition" not in d

    def test_if_from_kwargs(self):
        """Processor constructed from dict unpacking with 'if' key."""
        raw = {"expression": "record.id", "output": "queue.ids", "if": 'record.active == true'}
        p = Processor(**raw)
        assert p.if_condition == 'record.active == true'
        assert p.to_dict()["if"] == 'record.active == true'

    def test_aggregation_as_string(self):
        p = Processor(expression="x", aggregation="collect")
        assert p.to_dict()["aggregation"] == "collect"

    def test_all_aggregation_types(self):
        for agg in AggregationType:
            if agg == AggregationType.NONE:
                continue
            p = Processor(expression="x", aggregation=agg)
            assert p.to_dict()["aggregation"] == agg.value

    def test_minimal(self):
        p = Processor(expression="log(record)")
        assert p.to_dict() == {"expression": "log(record)"}


# ──────────────────────────────────────────────────────────────
# Rule
# ──────────────────────────────────────────────────────────────

class TestRule:
    def test_all_fields(self):
        r = Rule(
            action=RuleAction.RETRY,
            condition="response.status == 429",
            max_attempts=5,
            backoff=BackoffType.EXPONENTIAL,
            backoff_base=2,
            message="Rate limited",
        )
        d = r.to_dict()
        assert d["action"] == "retry"
        assert d["condition"] == "response.status == 429"
        assert d["max_attempts"] == 5
        assert d["backoff"] == "exponential"
        assert d["backoff_base"] == 2
        assert d["message"] == "Rate limited"

    def test_all_actions(self):
        for action in RuleAction:
            r = Rule(action=action, condition="true")
            assert r.to_dict()["action"] == action.value

    def test_all_backoff_types(self):
        for bt in BackoffType:
            if bt == BackoffType.NONE:
                continue
            r = Rule(action="retry", backoff=bt, backoff_base=1)
            assert r.to_dict()["backoff"] == bt.value

    def test_action_as_string(self):
        r = Rule(action="stop", condition="true")
        assert r.to_dict()["action"] == "stop"


# ──────────────────────────────────────────────────────────────
# Iterate
# ──────────────────────────────────────────────────────────────

class TestIterate:
    def test_all_fields(self):
        i = Iterate(over="queue.user_ids", into="state.user_id", concurrency=5)
        d = i.to_dict()
        assert d["over"] == "queue.user_ids"
        assert d["into"] == "state.user_id"
        assert d["concurrency"] == 5

    def test_over_as_list(self):
        i = Iterate(over=["a", "b", "c"], into="state.item")
        assert i.to_dict()["over"] == ["a", "b", "c"]


# ──────────────────────────────────────────────────────────────
# Response
# ──────────────────────────────────────────────────────────────

class TestResponse:
    def test_all_fields(self):
        resp = Response(
            format=ResponseFormat.JSONLINES,
            records=Records(jmespath="data[]", primary_key=["id"]),
            processors=[Processor(expression="log(record)")],
            rules=[Rule(action=RuleAction.RETRY, condition="response.status == 429")],
            prepend_rules=[Rule(action=RuleAction.SKIP, condition="true")],
            append_rules=[Rule(action=RuleAction.STOP, condition="false")],
            prepend_processors=[Processor(expression="x", output="y")],
            append_processors=[Processor(expression="a", output="b")],
        )
        d = resp.to_dict()
        assert d["format"] == "jsonlines"
        assert d["records"]["jmespath"] == "data[]"
        assert len(d["processors"]) == 1
        assert len(d["rules"]) == 1
        assert len(d["+rules"]) == 1
        assert len(d["rules+"]) == 1
        assert len(d["+processors"]) == 1
        assert len(d["processors+"]) == 1

    def test_records_from_dict(self):
        resp = Response(records={"jmespath": "items[]", "primary_key": ["id"]})
        assert isinstance(resp.records, Records)
        assert resp.records.jmespath == "items[]"

    def test_processors_from_dicts(self):
        resp = Response(
            processors=[
                {"expression": "record.id", "output": "queue.ids"},
                {"expression": "x", "if": "y"},
            ]
        )
        assert isinstance(resp.processors[0], Processor)
        assert isinstance(resp.processors[1], Processor)
        assert resp.processors[1].if_condition == "y"

    def test_rules_from_dicts(self):
        resp = Response(rules=[{"action": "retry", "condition": "true", "max_attempts": 3}])
        assert isinstance(resp.rules[0], Rule)
        assert resp.rules[0].max_attempts == 3

    def test_format_as_string(self):
        resp = Response(format="csv")
        assert resp.to_dict()["format"] == "csv"

    def test_modifier_keys_roundtrip(self):
        """Modifier keys (+rules, rules+) survive serialization and parsing."""
        resp = Response(
            append_rules=[Rule(action="skip", condition="response.status == 404")],
        )
        d = resp.to_dict()
        assert "rules+" in d
        assert "+rules" not in d


# ──────────────────────────────────────────────────────────────
# Call
# ──────────────────────────────────────────────────────────────

class TestCall:
    def test_all_fields(self):
        c = Call(
            if_condition="state.should_run",
            request=Request(url="http://x"),
            pagination=Pagination(next_state={}, stop_condition="true"),
            response=Response(records=Records(jmespath="data")),
            authentication={"type": "static", "headers": {"X": "Y"}},
            iterate="state.items",
            into="state.item",
        )
        d = c.to_dict()
        assert d["if"] == "state.should_run"
        assert d["request"]["url"] == "http://x"
        assert d["pagination"]["stop_condition"] == "true"
        assert d["response"]["records"]["jmespath"] == "data"
        assert d["authentication"]["type"] == "static"
        assert d["iterate"] == "state.items"
        assert d["into"] == "state.item"

    def test_if_from_kwargs(self):
        raw = {"if": "state.ok", "request": {"url": "http://x"}}
        c = Call(**raw)
        assert c.if_condition == "state.ok"
        assert c.to_dict()["if"] == "state.ok"

    def test_request_from_dict(self):
        c = Call(request={"url": "http://x", "method": "POST"})
        assert isinstance(c.request, Request)
        assert c.request.method == "POST"

    def test_pagination_from_dict(self):
        c = Call(pagination={"next_state": {"p": "1"}, "stop_condition": "true"})
        assert isinstance(c.pagination, Pagination)

    def test_response_from_dict_with_modifiers(self):
        c = Call(response={
            "records": {"jmespath": "d"},
            "+rules": [{"action": "skip", "condition": "true"}],
        })
        assert isinstance(c.response, Response)
        assert len(c.response.prepend_rules) == 1


# ──────────────────────────────────────────────────────────────
# Endpoint
# ──────────────────────────────────────────────────────────────

class TestEndpoint:
    def test_all_fields(self):
        ep = Endpoint(
            name="my_endpoint",
            description="Test endpoint",
            docs="https://docs.example.com",
            disabled=False,
            state={"offset": 0},
            sync=["last_id"],
            request=Request(url="http://x"),
            pagination=Pagination(next_state={"p": "1"}),
            response=Response(records=Records(primary_key=["id"])),
            iterate=Iterate(over="queue.ids", into="state.id", concurrency=3),
            setup=[Call(request=Request(url="http://x/pre"))],
            teardown=[Call(request=Request(url="http://x/post"))],
            depends_on=["other"],
            overrides={"mode": "full-refresh"},
            authentication={"type": "basic"},
            prepend_setup=[Call(request=Request(url="http://x/pre2"))],
            append_setup=[Call(request=Request(url="http://x/pre3"))],
            prepend_teardown=[Call(request=Request(url="http://x/post2"))],
            append_teardown=[Call(request=Request(url="http://x/post3"))],
        )
        d = ep.to_dict()
        assert d["name"] == "my_endpoint"
        assert d["description"] == "Test endpoint"
        assert d["docs"] == "https://docs.example.com"
        assert d["disabled"] is False
        assert d["state"] == {"offset": 0}
        assert d["sync"] == ["last_id"]
        assert d["request"]["url"] == "http://x"
        assert d["pagination"]["next_state"] == {"p": "1"}
        assert d["response"]["records"]["primary_key"] == ["id"]
        assert d["iterate"]["over"] == "queue.ids"
        assert len(d["setup"]) == 1
        assert len(d["teardown"]) == 1
        assert d["depends_on"] == ["other"]
        assert d["overrides"] == {"mode": "full-refresh"}
        assert d["authentication"] == {"type": "basic"}
        assert len(d["+setup"]) == 1
        assert len(d["setup+"]) == 1
        assert len(d["+teardown"]) == 1
        assert len(d["teardown+"]) == 1

    def test_from_dicts(self):
        ep = Endpoint(
            request={"url": "http://x", "method": "POST"},
            pagination={"next_state": {"p": "1"}},
            response={"records": {"jmespath": "d"}, "+rules": [{"action": "skip", "condition": "true"}]},
            iterate={"over": "queue.ids", "into": "state.id"},
            setup=[{"request": {"url": "http://x/setup"}}],
            teardown=[{"request": {"url": "http://x/teardown"}}],
        )
        assert isinstance(ep.request, Request)
        assert isinstance(ep.pagination, Pagination)
        assert isinstance(ep.response, Response)
        assert isinstance(ep.iterate, Iterate)
        assert isinstance(ep.setup[0], Call)
        assert isinstance(ep.teardown[0], Call)
        assert len(ep.response.prepend_rules) == 1

    def test_iterate_as_string_preserved(self):
        """Iterate can be a string shorthand (used in Call objects)."""
        ep = Endpoint(iterate="queue.ids")
        assert ep.iterate == "queue.ids"
        assert ep.to_dict()["iterate"] == "queue.ids"

    def test_empty_pagination_override(self):
        ep = Endpoint(pagination=Pagination(next_state={}))
        d = ep.to_dict()
        assert d["pagination"] == {"next_state": {}}

    def test_modifier_keys_from_dict(self):
        raw = {
            "request": {"url": "http://x"},
            "+setup": [{"request": {"url": "http://pre"}}],
            "setup+": [{"request": {"url": "http://post"}}],
            "+teardown": [{"request": {"url": "http://td-pre"}}],
            "teardown+": [{"request": {"url": "http://td-post"}}],
        }
        ep = Endpoint(**raw)
        assert len(ep.prepend_setup) == 1
        assert len(ep.append_setup) == 1
        assert len(ep.prepend_teardown) == 1
        assert len(ep.append_teardown) == 1
        d = ep.to_dict()
        assert "+setup" in d
        assert "setup+" in d
        assert "+teardown" in d
        assert "teardown+" in d


# ──────────────────────────────────────────────────────────────
# DynamicEndpoint
# ──────────────────────────────────────────────────────────────

class TestDynamicEndpoint:
    def test_all_fields(self):
        de = DynamicEndpoint(
            setup=[
                Call(
                    request=Request(url="http://x/meta"),
                    response=Response(
                        records=Records(jmespath="tables"),
                        processors=[Processor(expression="record.name", output="state.tables", aggregation=AggregationType.COLLECT)],
                    ),
                ),
            ],
            iterate="state.tables",
            into="state.table",
            endpoint=Endpoint(
                name="{state.table}",
                request=Request(url="http://x/data/{state.table}"),
                response=Response(records=Records(primary_key=["id"])),
            ),
        )
        d = de.to_dict()
        assert len(d["setup"]) == 1
        assert d["iterate"] == "state.tables"
        assert d["into"] == "state.table"
        assert d["endpoint"]["name"] == "{state.table}"

    def test_from_dicts(self):
        raw = {
            "setup": [{"request": {"url": "http://x/meta"}}],
            "iterate": "state.items",
            "into": "state.item",
            "endpoint": {"request": {"url": "http://x/{state.item}"}},
        }
        de = DynamicEndpoint(**raw)
        assert isinstance(de.setup[0], Call)
        assert isinstance(de.endpoint, Endpoint)


# ──────────────────────────────────────────────────────────────
# ApiSpec - programmatic building
# ──────────────────────────────────────────────────────────────

class TestApiSpecBuild:
    def test_all_top_level_fields(self):
        spec = ApiSpec(
            name="Test API",
            description="A test API",
            queues=["q1", "q2"],
            authentication={"type": "oauth2", "flow": "client_credentials"},
            defaults=Endpoint(
                request=Request(url="http://x", rate=5),
                response=Response(records=Records(jmespath="data[]")),
            ),
            endpoints={
                "ep1": Endpoint(request=Request(url="http://x/ep1")),
            },
            dynamic_endpoints=[
                DynamicEndpoint(
                    iterate="state.items",
                    into="state.item",
                    endpoint=Endpoint(request=Request(url="http://x/{state.item}")),
                ),
            ],
        )
        d = spec.to_dict()
        assert d["name"] == "Test API"
        assert d["description"] == "A test API"
        assert d["queues"] == ["q1", "q2"]
        assert d["authentication"]["type"] == "oauth2"
        assert d["defaults"]["request"]["rate"] == 5
        assert "ep1" in d["endpoints"]
        assert len(d["dynamic_endpoints"]) == 1

    def test_endpoints_from_dicts(self):
        spec = ApiSpec(
            name="test",
            endpoints={
                "ep": {"request": {"url": "http://x"}, "response": {"records": {"jmespath": "d"}}},
            },
        )
        assert isinstance(spec.endpoints["ep"], Endpoint)
        assert isinstance(spec.endpoints["ep"].request, Request)

    def test_dynamic_endpoints_from_dicts(self):
        spec = ApiSpec(
            name="test",
            dynamic_endpoints=[
                {"iterate": "state.x", "into": "state.y", "endpoint": {"request": {"url": "http://x"}}},
            ],
        )
        assert isinstance(spec.dynamic_endpoints[0], DynamicEndpoint)
        assert isinstance(spec.dynamic_endpoints[0].endpoint, Endpoint)

    def test_defaults_from_dict(self):
        spec = ApiSpec(
            name="test",
            defaults={"request": {"url": "http://x", "rate": 5}},
            endpoints={"ep": Endpoint(request=Request(url="http://x/ep"))},
        )
        assert isinstance(spec.defaults, Endpoint)
        assert spec.defaults.request.rate == 5

    def test_none_fields_omitted_in_to_dict(self):
        spec = ApiSpec(name="test", endpoints={"ep": Endpoint(request=Request(url="http://x"))})
        d = spec.to_dict()
        assert "queues" not in d
        assert "authentication" not in d
        assert "defaults" not in d
        assert "dynamic_endpoints" not in d
        assert "description" not in d


# ──────────────────────────────────────────────────────────────
# ApiSpec - serialization
# ──────────────────────────────────────────────────────────────

class TestApiSpecSerialization:
    def _make_spec(self):
        return ApiSpec(
            name="Serialize Test",
            queues=["ids"],
            defaults=Endpoint(
                request=Request(headers={"Auth": "Bearer tok"}, rate=5),
                response=Response(records=Records(jmespath="data[]", primary_key=["id"])),
            ),
            endpoints={
                "items": Endpoint(
                    request=Request(url="http://x/items"),
                    response=Response(
                        processors=[Processor(expression="record.id", output="queue.ids")],
                    ),
                ),
            },
        )

    def test_to_yaml(self):
        spec = self._make_spec()
        yaml_str = spec.to_yaml()
        assert "name: Serialize Test" in yaml_str
        assert "jmespath: data[]" in yaml_str
        assert "queue.ids" in yaml_str

    def test_to_json(self):
        spec = self._make_spec()
        json_str = spec.to_json()
        parsed = json.loads(json_str)
        assert parsed["name"] == "Serialize Test"
        assert parsed["queues"] == ["ids"]
        assert "items" in parsed["endpoints"]

    def test_to_yaml_file(self):
        spec = self._make_spec()
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False, mode="w") as f:
            path = f.name
        try:
            spec.to_yaml_file(path)
            with open(path) as f:
                content = f.read()
            assert "name: Serialize Test" in content
        finally:
            os.remove(path)

    def test_roundtrip_programmatic(self):
        spec = self._make_spec()
        yaml_str = spec.to_yaml()
        spec2 = ApiSpec.parse(yaml_str)
        assert spec.to_dict() == spec2.to_dict()


# ──────────────────────────────────────────────────────────────
# ApiSpec - parsing sample YAML
# ──────────────────────────────────────────────────────────────

@pytest.mark.skipif(not SAMPLE_YAML.exists(), reason="sample_api_spec.yaml not found")
class TestParseSampleYaml:
    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = ApiSpec.parse_file(str(SAMPLE_YAML))

    # -- top-level fields --

    def test_name(self):
        assert self.spec.name == "Sample API"

    def test_description(self):
        assert self.spec.description == "A comprehensive sample API spec covering all supported fields"

    def test_queues(self):
        assert self.spec.queues == ["user_ids", "order_ids", "event_ids"]

    def test_authentication(self):
        auth = self.spec.authentication
        assert auth["type"] == "oauth2"
        assert auth["flow"] == "client_credentials"
        assert "client_id" in auth
        assert "client_secret" in auth
        assert "authentication_url" in auth

    # -- defaults --

    def test_defaults_state(self):
        assert self.spec.defaults.state["base_url"] == "https://api.example.com/v1"
        assert self.spec.defaults.state["limit"] == 50

    def test_defaults_request_all_fields(self):
        req = self.spec.defaults.request
        assert req.url == "{state.base_url}/default"
        assert req.timeout == 30
        assert req.method == "GET"
        assert req.headers["Accept"] == "application/json"
        assert req.parameters["limit"] == "{state.limit}"
        assert req.payload == {"format": "full"}
        assert req.rate == 10
        assert req.concurrency == 3

    def test_defaults_response_records_all_fields(self):
        rec = self.spec.defaults.response.records
        assert rec.jmespath == "data[]"
        assert rec.primary_key == ["id"]
        assert rec.limit == 10000
        assert rec.casing == "snake"
        assert rec.select == ["*", "full_name as name"]
        assert rec.duplicate_tolerance == "100000,0.001"

    def test_defaults_response_rules_all_actions(self):
        rules = self.spec.defaults.response.rules
        actions = [r.action for r in rules]
        assert "retry" in actions
        assert "stop" in actions
        assert "continue" in actions
        assert "break" in actions
        assert "fail" in actions

    def test_defaults_response_rules_all_fields(self):
        retry_rule = self.spec.defaults.response.rules[0]
        assert retry_rule.action == "retry"
        assert retry_rule.condition == "response.status == 429"
        assert retry_rule.max_attempts == 5
        assert retry_rule.backoff == "exponential"
        assert retry_rule.backoff_base == 2
        assert retry_rule.message == "Rate limited, retrying"

    def test_defaults_response_processors(self):
        procs = self.spec.defaults.response.processors
        assert len(procs) == 1
        assert procs[0].expression == "log(record)"

    def test_defaults_pagination(self):
        pag = self.spec.defaults.pagination
        assert pag.next_state["offset"] == "{state.offset + state.limit}"
        assert "length(response.records)" in pag.stop_condition

    def test_defaults_setup_all_call_fields(self):
        setup = self.spec.defaults.setup
        assert len(setup) == 1
        call = setup[0]
        assert isinstance(call, Call)
        assert call.if_condition == "state.run_health_check"
        assert call.request.url == "{state.base_url}/health"
        assert call.request.timeout == 5
        assert call.request.method == "GET"
        assert call.request.headers == {"Accept": "application/json"}
        assert call.request.parameters == {}
        assert call.request.rate == 1
        assert call.request.concurrency == 1
        assert call.pagination.next_state == {}
        assert call.pagination.stop_condition == "true"
        assert call.response.records.jmespath == "status"
        assert len(call.response.processors) == 1
        assert call.authentication["type"] == "static"
        assert call.iterate == "state.health_targets"
        assert call.into == "state.health_target"

    def test_defaults_teardown(self):
        td = self.spec.defaults.teardown
        assert len(td) == 1
        assert td[0].request.method == "POST"

    def test_defaults_iterate(self):
        it = self.spec.defaults.iterate
        assert isinstance(it, Iterate)
        assert "inputs.regions" in it.over
        assert it.into == "state.region"
        assert it.concurrency == 2

    def test_defaults_depends_on(self):
        assert self.spec.defaults.depends_on == ["prerequisite"]

    def test_defaults_overrides(self):
        assert self.spec.defaults.overrides["mode"] == "incremental"
        assert self.spec.defaults.overrides["label"] == "default-label"

    def test_defaults_authentication(self):
        assert self.spec.defaults.authentication["headers"]["X-Custom-Auth"] == "custom-token"

    # -- endpoint: account (minimal, empty overrides) --

    def test_account_endpoint(self):
        ep = self.spec.endpoints["account"]
        assert ep.description == "Retrieve account information"
        assert ep.docs == "https://api.example.com/docs/account"
        assert ep.request.parameters == {}
        assert ep.pagination.to_dict() == {}
        assert ep.overrides["mode"] == "full-refresh"

    # -- endpoint: users (all Request fields, name field) --

    def test_users_endpoint(self):
        ep = self.spec.endpoints["users"]
        assert ep.name == "users_endpoint"
        assert ep.docs == "https://api.example.com/docs/users"
        req = ep.request
        assert req.url == "{state.base_url}/users"
        assert req.timeout == 60
        assert req.method == "POST"
        assert req.headers == {"X-Custom": "value"}
        assert req.parameters == {"include": "details"}
        assert req.payload == {"query": "active users"}
        assert req.rate == 20
        assert req.concurrency == 5

    # -- endpoint: user_orders (iterate, depends_on, jq, all Records fields) --

    def test_user_orders_endpoint(self):
        ep = self.spec.endpoints["user_orders"]
        assert ep.depends_on == ["users"]
        it = ep.iterate
        assert isinstance(it, Iterate)
        assert it.over == "queue.user_ids"
        assert it.into == "state.user_id"
        assert it.concurrency == 5
        rec = ep.response.records
        assert rec.jq == ".orders[]"
        assert rec.jmespath is None
        assert rec.primary_key == ["id"]
        assert rec.update_key == "updated_at"
        assert rec.limit == 5000
        assert rec.casing == "snake"
        assert rec.select == ["id", "total", "order_date as date"]
        assert rec.duplicate_tolerance == "50000,0.01"

    # -- endpoint: orders_incremental (sync, all aggregation types, if condition) --

    def test_orders_incremental_sync(self):
        ep = self.spec.endpoints["orders_incremental"]
        assert ep.sync == ["last_updated"]
        assert ep.response.records.update_key == "updated_at"

    def test_orders_incremental_all_aggregations(self):
        procs = self.spec.endpoints["orders_incremental"].response.processors
        aggs = [p.aggregation for p in procs if p.aggregation]
        assert "maximum" in aggs
        assert "minimum" in aggs
        assert "first" in aggs
        assert "last" in aggs
        assert "collect" in aggs

    def test_orders_incremental_if_condition(self):
        procs = self.spec.endpoints["orders_incremental"].response.processors
        proc_with_if = [p for p in procs if p.if_condition]
        assert len(proc_with_if) == 1
        assert proc_with_if[0].if_condition == "!is_null(record.updated_at)"

    # -- endpoint: fragile_endpoint (+rules / append_rules) --

    def test_fragile_append_rules(self):
        ep = self.spec.endpoints["fragile_endpoint"]
        assert ep.response.prepend_rules is not None
        assert len(ep.response.prepend_rules) == 1
        assert ep.response.prepend_rules[0].action == "skip"
        assert ep.response.prepend_rules[0].message == "Resource not found, skipping"

    # -- endpoint: enriched_endpoint (processors+ / append_processors) --

    def test_enriched_append_processors(self):
        ep = self.spec.endpoints["enriched_endpoint"]
        assert ep.response.append_processors is not None
        assert len(ep.response.append_processors) == 1
        assert ep.response.append_processors[0].expression == "upper(record.name)"

    # -- endpoint: csv_endpoint (response format) --

    def test_csv_response_format(self):
        ep = self.spec.endpoints["csv_endpoint"]
        assert ep.response.format == "csv"

    # -- endpoint: disabled_endpoint --

    def test_disabled_endpoint(self):
        ep = self.spec.endpoints["disabled_endpoint"]
        assert ep.disabled is True

    # -- endpoint: conditional_endpoint (processor if from YAML) --

    def test_conditional_processors(self):
        ep = self.spec.endpoints["conditional_endpoint"]
        procs = ep.response.processors
        assert procs[0].if_condition == 'record.type == "important"'
        assert procs[1].if_condition == "!is_null(record.metadata)"

    # -- endpoint: with_setup (setup/teardown on endpoint) --

    def test_endpoint_setup_teardown(self):
        ep = self.spec.endpoints["with_setup"]
        assert len(ep.setup) == 1
        assert ep.setup[0].request.url == "{state.base_url}/pre-check"
        assert len(ep.teardown) == 1
        assert ep.teardown[0].request.method == "POST"

    # -- endpoint: custom_auth_endpoint (endpoint-level authentication) --

    def test_endpoint_authentication(self):
        ep = self.spec.endpoints["custom_auth_endpoint"]
        assert ep.authentication["type"] == "basic"

    # -- endpoint: modified_lifecycle (+setup, setup+, +teardown, teardown+) --

    def test_modifier_setup_teardown(self):
        ep = self.spec.endpoints["modified_lifecycle"]
        assert len(ep.prepend_setup) == 1
        assert ep.prepend_setup[0].request.url == "{state.base_url}/extra-pre"
        assert len(ep.append_setup) == 1
        assert ep.append_setup[0].request.url == "{state.base_url}/extra-post-setup"
        assert len(ep.prepend_teardown) == 1
        assert ep.prepend_teardown[0].request.url == "{state.base_url}/extra-pre-teardown"
        assert len(ep.append_teardown) == 1
        assert ep.append_teardown[0].request.url == "{state.base_url}/extra-post-teardown"

    # -- endpoint: simple_iterate (depends_on) --

    def test_simple_iterate_depends_on(self):
        ep = self.spec.endpoints["simple_iterate"]
        assert ep.depends_on == ["conditional_endpoint"]

    # -- endpoint: all_rules (all backoff types) --

    def test_all_backoff_types_in_rules(self):
        ep = self.spec.endpoints["all_rules"]
        rules = ep.response.rules
        backoffs = [r.backoff for r in rules]
        assert "constant" in backoffs
        assert "linear" in backoffs
        assert "jitter" in backoffs
        # verify all fields on each
        for rule in rules:
            assert rule.action == "retry"
            assert rule.condition is not None
            assert rule.max_attempts is not None
            assert rule.backoff is not None
            assert rule.backoff_base is not None
            assert rule.message is not None

    # -- dynamic_endpoints --

    def test_dynamic_endpoints(self):
        assert len(self.spec.dynamic_endpoints) == 1
        de = self.spec.dynamic_endpoints[0]
        assert len(de.setup) == 2
        assert de.iterate == "state.table_names"
        assert de.into == "state.table_name"
        assert de.endpoint.name == "{state.table_name}"
        assert de.endpoint.request.url == "{state.base_url}/data/{state.table_name}"

    def test_dynamic_endpoint_setup_calls(self):
        de = self.spec.dynamic_endpoints[0]
        call1 = de.setup[0]
        assert call1.request.url == "{state.base_url}/metadata/tables"
        assert call1.response.records.jmespath == "tables[]"
        assert call1.response.processors[0].aggregation == "collect"

        call2 = de.setup[1]
        assert call2.iterate == "state.table_names"
        assert call2.into == "state.table_name"

    # -- full roundtrip --

    def test_roundtrip(self):
        d1 = self.spec.to_dict()
        yaml_str = self.spec.to_yaml()
        spec2 = ApiSpec.parse(yaml_str)
        d2 = spec2.to_dict()
        assert d1 == d2


# ──────────────────────────────────────────────────────────────
# ApiSpec - validation
# ──────────────────────────────────────────────────────────────

class TestValidation:
    def test_valid_spec_passes(self):
        spec = ApiSpec(
            name="valid",
            endpoints={"ep": Endpoint(request=Request(url="http://x"))},
        )
        assert spec.validate() == []

    def test_valid_with_dynamic_endpoints_only(self):
        spec = ApiSpec(
            name="valid",
            dynamic_endpoints=[
                DynamicEndpoint(
                    iterate="state.x",
                    into="state.y",
                    endpoint=Endpoint(request=Request(url="http://x")),
                ),
            ],
        )
        assert spec.validate() == []

    def test_name_required(self):
        spec = ApiSpec(endpoints={"ep": Endpoint()})
        errors = spec.validate()
        assert any("name" in e for e in errors)

    def test_endpoints_or_dynamic_required(self):
        spec = ApiSpec(name="test")
        errors = spec.validate()
        assert any("endpoints" in e or "dynamic_endpoints" in e for e in errors)

    def test_empty_endpoints_dict_fails(self):
        spec = ApiSpec(name="test", endpoints={})
        errors = spec.validate()
        assert any("endpoints" in e for e in errors)

    def test_jmespath_jq_mutual_exclusion(self):
        spec = ApiSpec(
            name="test",
            endpoints={
                "ep": Endpoint(
                    response=Response(records=Records(jmespath="a", jq=".a")),
                ),
            },
        )
        errors = spec.validate()
        assert any("mutually exclusive" in e for e in errors)

    def test_jmespath_jq_in_defaults(self):
        spec = ApiSpec(
            name="test",
            defaults=Endpoint(
                response=Response(records=Records(jmespath="a", jq=".a")),
            ),
            endpoints={"ep": Endpoint()},
        )
        errors = spec.validate()
        assert any("mutually exclusive" in e for e in errors)

    def test_queue_reference_valid(self):
        spec = ApiSpec(
            name="test",
            queues=["ids"],
            endpoints={
                "ep": Endpoint(
                    response=Response(
                        processors=[Processor(expression="record.id", output="queue.ids")],
                    ),
                ),
            },
        )
        assert spec.validate() == []

    def test_queue_reference_invalid(self):
        spec = ApiSpec(
            name="test",
            queues=["ids"],
            endpoints={
                "ep": Endpoint(
                    response=Response(
                        processors=[Processor(expression="record.id", output="queue.unknown")],
                    ),
                ),
            },
        )
        errors = spec.validate()
        assert any("unknown" in e for e in errors)

    def test_queue_reference_in_iterate(self):
        spec = ApiSpec(
            name="test",
            queues=["ids"],
            endpoints={
                "ep": Endpoint(
                    iterate=Iterate(over="queue.missing", into="state.id"),
                    request=Request(url="http://x"),
                ),
            },
        )
        errors = spec.validate()
        assert any("missing" in e for e in errors)

    def test_queue_reference_in_append_processors(self):
        spec = ApiSpec(
            name="test",
            queues=["valid"],
            endpoints={
                "ep": Endpoint(
                    response=Response(
                        append_processors=[Processor(expression="record.id", output="queue.invalid")],
                    ),
                ),
            },
        )
        errors = spec.validate()
        assert any("invalid" in e for e in errors)

    def test_sync_key_valid(self):
        spec = ApiSpec(
            name="test",
            endpoints={
                "ep": Endpoint(
                    sync=["last_updated"],
                    response=Response(
                        processors=[
                            Processor(expression="record.ts", output="state.last_updated", aggregation="maximum"),
                        ],
                    ),
                ),
            },
        )
        assert spec.validate() == []

    def test_sync_key_missing_processor(self):
        spec = ApiSpec(
            name="test",
            endpoints={
                "ep": Endpoint(
                    sync=["last_updated"],
                    response=Response(records=Records(primary_key=["id"])),
                ),
            },
        )
        errors = spec.validate()
        assert any("last_updated" in e for e in errors)

    def test_sync_key_in_append_processors(self):
        spec = ApiSpec(
            name="test",
            endpoints={
                "ep": Endpoint(
                    sync=["last_id"],
                    response=Response(
                        append_processors=[
                            Processor(expression="record.id", output="state.last_id", aggregation="last"),
                        ],
                    ),
                ),
            },
        )
        assert spec.validate() == []

    def test_no_queues_declared_skips_check(self):
        """When queues list is empty/None, queue refs are not validated."""
        spec = ApiSpec(
            name="test",
            endpoints={
                "ep": Endpoint(
                    response=Response(
                        processors=[Processor(expression="x", output="queue.anything")],
                    ),
                ),
            },
        )
        assert spec.validate() == []

    def test_sample_yaml_validates(self):
        if not SAMPLE_YAML.exists():
            pytest.skip("sample_api_spec.yaml not found")
        spec = ApiSpec.parse_file(str(SAMPLE_YAML))
        errors = spec.validate()
        assert errors == [], f"Sample YAML validation errors: {errors}"


# ──────────────────────────────────────────────────────────────
# JsonEncoder compatibility
# ──────────────────────────────────────────────────────────────

class TestJsonEncoder:
    def test_api_spec_enums(self):
        from sling import JsonEncoder
        enc = JsonEncoder()
        assert enc.default(HTTPMethod.GET) == "GET"
        assert enc.default(RuleAction.RETRY) == "retry"
        assert enc.default(AggregationType.MAXIMUM) == "maximum"
        assert enc.default(BackoffType.EXPONENTIAL) == "exponential"
        assert enc.default(ResponseFormat.CSV) == "csv"
        assert enc.default(AuthType.OAUTH2) == "oauth2"
        assert enc.default(OAuthFlow.CLIENT_CREDENTIALS) == "client_credentials"

    def test_existing_enums_still_work(self):
        from sling import JsonEncoder, Mode
        from sling.enum import Format, Compression, MergeStrategy
        enc = JsonEncoder()
        assert enc.default(Mode.FULL_REFRESH) == "full-refresh"
        assert enc.default(Format.PARQUET) == "parquet"
        assert enc.default(Compression.GZIP) == "gzip"
        assert enc.default(MergeStrategy.DELETE_INSERT) == "delete_insert"


# ──────────────────────────────────────────────────────────────
# parse_file error handling
# ──────────────────────────────────────────────────────────────

class TestParseErrors:
    def test_parse_non_mapping(self):
        with pytest.raises(ValueError, match="mapping"):
            ApiSpec.parse("- item1\n- item2\n")

    def test_parse_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            ApiSpec.parse_file("/nonexistent/path.yaml")
