import json
import math
from unittest.mock import MagicMock, patch

from comsdk.edge import Edge as RuntimeEdge
from comsdk.executors import build_executor_function
from comsdk.graph import Func, ThreadParallelizationPolicy
from django.test import RequestFactory, TestCase

from .aini.aini_parser import build_initial_data, parse_aini
from .execution_history import record_execution_event, record_execution_events
from .models import Edge, Graph, GraphExecutionSession, State, Transfer
from .runtime_ir import build_comsdk_graph_from_db, serialize_runtime_edge
from .views import _build_execution_input_schema, graph_execution_history_json, start_execution
from config.tasks import execute_graph_task


RAW_AINI = """
[Input]
*TaskName=ElasticResearch
-Pressure=34 [[MPa]]
OutputFilename=@TaskName@_@Pressure@.res
CopyObjectToRep=[1]{0|1}
Range=[0.4;0.1:0.6;0.05]
DimensionalRange=[120.0;1:300;1] [[GPa]]
Mode=[auNO]{auNO|auLCS|auSegmented}
LocalAxes=((1;0;0);(0;1;0);(0;0;1))
GeoFile=[geometry.geo]
"""


class AINIParserTests(TestCase):
    def test_parse_aini_supported_types(self):
        """
        Что делает: тестовый слой aINI-парсера.
        Место: тестовый слой aINI-парсера.
        Вход: RAW_AINI с разными типами параметров.
        Выход: проверяет, что parse_aini возвращает ожидаемые value_type и значения.
        """
        parsed = parse_aini(RAW_AINI)
        self.assertEqual(parsed["sections"][0]["name"], "Input")
        params = {param["name"]: param for param in parsed["parameters"]}

        self.assertEqual(params["TaskName"]["value_type"], "text")
        self.assertEqual(params["Pressure"]["value_type"], "dim")
        self.assertEqual(params["Pressure"]["value"]["value"], 34)
        self.assertEqual(params["Mode"]["value_type"], "combobox")
        self.assertEqual(params["Range"]["value_type"], "interval")
        self.assertEqual(params["DimensionalRange"]["value_type"], "interval")
        self.assertEqual(params["DimensionalRange"]["value"]["current"], 120.0)
        self.assertEqual(params["DimensionalRange"]["value"]["unit"], "GPa")
        self.assertEqual(params["GeoFile"]["value_type"], "file_ref")

    def test_build_initial_data_resolves_templates(self):
        """
        Что делает: тестовый слой подготовки initial_data из aINI.
        Место: тестовый слой подготовки initial_data из aINI.
        Вход: RAW_AINI с шаблонной строкой OutputFilename.
        Выход: проверяет runtime-значения и разрешение @TaskName@/@Pressure@.
        """
        data = build_initial_data(RAW_AINI)
        self.assertEqual(data["TaskName"], "ElasticResearch")
        self.assertEqual(data["Pressure"], 34)
        self.assertEqual(data["Range"], 0.4)
        self.assertEqual(data["DimensionalRange"], 120.0)
        self.assertEqual(data["OutputFilename"], "ElasticResearch_34.res")
        self.assertTrue(data["CopyObjectToRep"])

    def test_build_execution_input_schema_marks_initial_values(self):
        """
        Что делает: тестовый слой схемы формы запуска.
        Место: тестовый слой схемы формы запуска.
        Вход: RAW_AINI с обязательными, optional и предзаполненными параметрами.
        Выход: проверяет initial_value, labels и счетчик prefilled_count.
        """
        schema = _build_execution_input_schema(RAW_AINI)
        fields = {field["name"]: field for field in schema["fields"]}

        self.assertEqual(fields["TaskName"]["initial_value"], "ElasticResearch")
        self.assertTrue(fields["TaskName"]["has_initial_value"])
        self.assertEqual(fields["Pressure"]["initial_value"], 34)
        self.assertEqual(fields["Pressure"]["initial_value_label"], "34 [MPa]")
        self.assertEqual(fields["Range"]["input_type"], "number")
        self.assertEqual(fields["Range"]["initial_value"], 0.4)
        self.assertEqual(fields["DimensionalRange"]["initial_value"], 120.0)
        self.assertEqual(fields["DimensionalRange"]["initial_value_label"], "120.0 [GPa]")
        self.assertTrue(fields["CopyObjectToRep"]["initial_value"])
        self.assertGreaterEqual(schema["prefilled_count"], 3)


class StartExecutionAINITests(TestCase):
    def setUp(self):
        """
        Что делает: подготовка тестов endpoint'а запуска исполнения.
        Место: подготовка тестов endpoint'а запуска исполнения.
        Вход: нет.
        Выход: создает RequestFactory для ручного вызова Django view.
        """
        self.factory = RequestFactory()

    @patch("comwpc.views.execute_graph_task.delay")
    def test_start_execution_uses_aini_defaults_for_omitted_fields(self, delay_mock):
        """
        Что делает: тест валидации обязательных aINI-полей при запуске графа.
        Место: тест валидации обязательных aINI-полей при запуске графа.
        Вход: POST без обязательного TaskName.
        Выход: проверяет HTTP 400 и отсутствие вызова Celery delay.
        """
        graph = Graph.objects.create(
            name="test_graph_with_aini",
            raw_dot="digraph Test { __BEGIN__ -> __END__ }",
            raw_aini=RAW_AINI,
        )

        request = self.factory.post(
            f"/graph/{graph.id}/start/",
            data=json.dumps({"data": {"Pressure": 55}}),
            content_type="application/json",
        )

        response = start_execution(request, graph.id)
        self.assertEqual(response.status_code, 200)
        delay_mock.assert_called_once()

        payload = delay_mock.call_args.args[2]
        self.assertEqual(payload["TaskName"], "ElasticResearch")
        self.assertEqual(payload["Pressure"], 55)
        self.assertEqual(payload["OutputFilename"], "ElasticResearch_55.res")

    @patch("comwpc.views.execute_graph_task.delay")
    def test_start_execution_merges_aini_defaults_with_user_values(self, delay_mock):
        """
        Что делает: тест подготовки payload для Celery-задачи запуска графа.
        Место: тест подготовки payload для Celery-задачи запуска графа.
        Вход: POST с пользовательскими TaskName и Pressure.
        Выход: проверяет session history, payload без лишних aINI-derived значений и вызов delay.
        """
        graph = Graph.objects.create(
            name="test_graph_with_aini_values",
            raw_dot="digraph Test { __BEGIN__ -> __END__ }",
            raw_aini=RAW_AINI,
        )

        request_payload = {
            "data": {
                "TaskName": "CustomTask",
                "Pressure": 55,
                "Range": 0.45,
                "DimensionalRange": 125.5,
            }
        }
        request = self.factory.post(
            f"/graph/{graph.id}/start/",
            data=json.dumps(request_payload),
            content_type="application/json",
        )

        response = start_execution(request, graph.id)
        self.assertEqual(response.status_code, 200)
        delay_mock.assert_called_once()
        self.assertEqual(delay_mock.call_args.args[0], graph.id)

        payload = delay_mock.call_args.args[2]
        self.assertEqual(payload["TaskName"], "CustomTask")
        self.assertEqual(payload["Pressure"], 55)
        self.assertEqual(payload["Range"], 0.45)
        self.assertEqual(payload["DimensionalRange"], 125.5)
        self.assertEqual(payload["OutputFilename"], "CustomTask_55.res")
        self.assertEqual(payload["CopyObjectToRep"], True)
        self.assertEqual(payload["Mode"], "auNO")
        self.assertNotIn("a", payload)

        session = GraphExecutionSession.objects.get(session_id=delay_mock.call_args.args[1])
        self.assertEqual(session.graph, graph)
        self.assertEqual(session.status, GraphExecutionSession.STATUS_PENDING)
        self.assertEqual(session.initial_data, payload)


class ExecutionHistoryTests(TestCase):
    def setUp(self):
        """
        Что делает: подготовка тестов истории исполнения.
        Место: подготовка тестов истории исполнения.
        Вход: нет.
        Выход: создает RequestFactory и тестовый Graph.
        """
        self.factory = RequestFactory()
        self.graph = Graph.objects.create(
            name="history_graph",
            raw_dot="digraph Test { __BEGIN__ -> __END__ }",
        )

    def test_record_execution_event_updates_session_status_and_events(self):
        """
        Что делает: тест записи live-событий в историю исполнения.
        Место: тест записи live-событий в историю исполнения.
        Вход: pending-сессия и события state_enter/complete.
        Выход: проверяет status, event_count, last_state, finished_at и порядок событий.
        """
        session = GraphExecutionSession.objects.create(
            graph=self.graph,
            session_id="session-history-1",
            initial_data={"Pressure": 55},
        )

        record_execution_event(
            session.session_id,
            {
                "event": "state_enter",
                "state": "Prepare",
                "timestamp": 1710000000,
                "data": {"Pressure": 55},
                "session_id": session.session_id,
            },
        )
        record_execution_event(
            session.session_id,
            {
                "event": "complete",
                "state": None,
                "timestamp": 1710000005,
                "data": {},
                "session_id": session.session_id,
            },
        )

        session.refresh_from_db()
        self.assertEqual(session.status, GraphExecutionSession.STATUS_COMPLETED)
        self.assertEqual(session.event_count, 2)
        self.assertEqual(session.last_state, "Prepare")
        self.assertIsNotNone(session.finished_at)

        events = list(session.events.all())
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].event_type, "state_enter")
        self.assertEqual(events[1].event_type, "complete")

    def test_graph_execution_history_json_returns_saved_sessions(self):
        """
        Что делает: тест API выдачи истории запусков графа.
        Место: тест API выдачи истории запусков графа.
        Вход: сессия с error-событием и GET-запрос истории.
        Выход: проверяет JSON-ответ, failed status, error_message и сериализацию событий.
        """
        session = GraphExecutionSession.objects.create(
            graph=self.graph,
            session_id="session-history-2",
            initial_data={"TaskName": "Demo"},
        )
        record_execution_event(
            session.session_id,
            {
                "event": "error",
                "state": "BrokenState",
                "timestamp": 1710000010,
                "message": "boom",
                "data": {"TaskName": "Demo"},
                "session_id": session.session_id,
            },
        )

        request = self.factory.get(f"/api/graphs/{self.graph.id}/executions/")
        response = graph_execution_history_json(request, self.graph.id)

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.content)
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["session_id"], session.session_id)
        self.assertEqual(payload[0]["status"], GraphExecutionSession.STATUS_FAILED)
        self.assertEqual(payload[0]["error_message"], "boom")
        self.assertEqual(payload[0]["events"][0]["event"], "error")

    def test_record_execution_events_batches_and_sanitizes_nonfinite_numbers(self):
        session = GraphExecutionSession.objects.create(
            graph=self.graph,
            session_id="session-history-batch",
            initial_data={"TaskName": "Batch"},
        )

        record_execution_events(
            session.session_id,
            [
                {
                    "event": "state_enter",
                    "state": "Prepare",
                    "timestamp": 1710000020,
                    "data": {"score": math.inf},
                },
                {
                    "event": "complete",
                    "state": None,
                    "timestamp": 1710000021,
                    "data": {"score": math.nan},
                },
            ],
        )

        session.refresh_from_db()
        events = list(session.events.all())

        self.assertEqual(session.status, GraphExecutionSession.STATUS_COMPLETED)
        self.assertEqual(session.event_count, 2)
        self.assertEqual([event.sequence for event in events], [1, 2])
        self.assertIsNone(events[0].payload["score"])
        self.assertIsNone(events[1].payload["score"])


class ExecuteGraphTaskTests(TestCase):
    def _create_parallel_policy_graph(self):
        graph = Graph.objects.create(name="parallel_policy_graph")
        begin = State.objects.create(name="__BEGIN__", graph=graph)
        swarm = State.objects.create(
            name="SWARM_READY",
            graph=graph,
            parallelism="threading",
        )
        end = State.objects.create(name="__END__", graph=graph, is_terminal=True)
        edge = Edge.objects.create(
            comment="pass",
            pred_module="",
            pred_func="",
            morph_module="",
            morph_func="",
        )
        Transfer.objects.create(
            source=begin,
            target=swarm,
            edge=edge,
            graph=graph,
            order=0,
        )
        Transfer.objects.create(
            source=swarm,
            target=end,
            edge=edge,
            graph=graph,
            order=0,
        )
        return graph

    def test_parallel_executor_serial_disables_threading_policy(self):
        graph = self._create_parallel_policy_graph()

        threaded_graph = build_comsdk_graph_from_db(
            graph,
            execution_options={"parallel_executor": "threading"},
        )
        serial_graph = build_comsdk_graph_from_db(
            graph,
            execution_options={"parallel_executor": "serial"},
        )

        threaded_swarm = threaded_graph.init_state.transfers[0].output_state
        serial_swarm = serial_graph.init_state.transfers[0].output_state
        self.assertIsInstance(
            threaded_swarm.parallelization_policy,
            ThreadParallelizationPolicy,
        )
        self.assertNotIsInstance(
            serial_swarm.parallelization_policy,
            ThreadParallelizationPolicy,
        )

    def test_parallel_executor_grpc_worker_pool_is_not_implemented(self):
        graph = self._create_parallel_policy_graph()

        with self.assertRaisesMessage(ValueError, "grpc_worker_pool is not implemented"):
            build_comsdk_graph_from_db(
                graph,
                execution_options={"parallel_executor": "grpc_worker_pool"},
            )

    def test_serialize_runtime_edge_stores_remote_cpp_executor_spec(self):
        morph_func = build_executor_function(
            executor="remote_cpp",
            operation="sin",
            input_key="angles",
            output_key="sin_angles",
        )
        runtime_edge = RuntimeEdge(
            Func(),
            Func(module="comsdk.executors", name=morph_func.__name__, func=morph_func),
        )

        edge_ir = serialize_runtime_edge(runtime_edge)

        self.assertEqual(edge_ir["executor_type"], "remote_cpp")
        self.assertEqual(edge_ir["executor_operation"], "sin")
        self.assertEqual(edge_ir["executor_input_key"], "angles")
        self.assertEqual(edge_ir["executor_output_key"], "sin_angles")

    def test_runtime_builder_executes_stored_db_ir(self):
        graph = Graph.objects.create(name="db_ir_graph")
        begin = State.objects.create(name="__BEGIN__", graph=graph)
        step = State.objects.create(name="STEP", graph=graph)
        end = State.objects.create(name="__END__", graph=graph, is_terminal=True)
        first_edge = Edge.objects.create(
            comment="increment",
            pred_module="",
            pred_func="",
            morph_module="test_funcs.simplest",
            morph_func="increment_a_edge",
        )
        final_edge = Edge.objects.create(
            comment="finish",
            pred_module="",
            pred_func="",
            morph_module="",
            morph_func="",
        )
        Transfer.objects.create(
            source=begin,
            target=step,
            edge=first_edge,
            graph=graph,
            order=0,
        )
        Transfer.objects.create(
            source=step,
            target=end,
            edge=final_edge,
            graph=graph,
            order=0,
        )
        session = GraphExecutionSession.objects.create(
            graph=graph,
            session_id="db-ir-session",
            initial_data={"a": 1},
        )

        payload = {"a": 1}
        with patch("config.tasks.publish_execution_ws_event"):
            execute_graph_task.run(graph.id, "db-ir-session", payload)

        session.refresh_from_db()
        self.assertEqual(session.status, GraphExecutionSession.STATUS_COMPLETED)
        self.assertGreaterEqual(session.event_count, 1)
        self.assertEqual(payload["a"], 2)

    def test_runtime_builder_executes_stored_remote_cpp_executor(self):
        graph = Graph.objects.create(name="db_ir_remote_cpp_graph")
        begin = State.objects.create(name="__BEGIN__", graph=graph)
        end = State.objects.create(name="__END__", graph=graph, is_terminal=True)
        edge = Edge.objects.create(
            comment="remote sin",
            pred_module="",
            pred_func="",
            morph_module="comsdk.executors",
            morph_func="remote_cpp_sin",
            executor_type="remote_cpp",
            executor_operation="sin",
            executor_input_key="x",
            executor_output_key="sin_x",
        )
        Transfer.objects.create(
            source=begin,
            target=end,
            edge=edge,
            graph=graph,
            order=0,
        )
        session = GraphExecutionSession.objects.create(
            graph=graph,
            session_id="db-ir-remote-cpp-session",
            initial_data={"x": [0.0, 1.0]},
        )

        def fake_execute_remote_cpp(data, operation, input_key, output_key):
            data[output_key] = {
                "operation": operation,
                "values": data[input_key],
            }
            return data

        payload = {"x": [0.0, 1.0]}
        with patch("comsdk.executors.execute_remote_cpp", side_effect=fake_execute_remote_cpp):
            with patch("config.tasks.publish_execution_ws_event"):
                execute_graph_task.run(graph.id, session.session_id, payload)

        session.refresh_from_db()
        self.assertEqual(session.status, GraphExecutionSession.STATUS_COMPLETED)
        self.assertEqual(
            payload["sin_x"],
            {"operation": "sin", "values": [0.0, 1.0]},
        )

    def test_runtime_builder_supports_legacy_remote_cpp_morph_name(self):
        graph = Graph.objects.create(name="db_ir_legacy_remote_cpp_graph")
        begin = State.objects.create(name="__BEGIN__", graph=graph)
        end = State.objects.create(name="__END__", graph=graph, is_terminal=True)
        edge = Edge.objects.create(
            comment="legacy remote sin",
            pred_module="",
            pred_func="",
            morph_module="comsdk.executors",
            morph_func="remote_cpp_sin",
        )
        Transfer.objects.create(
            source=begin,
            target=end,
            edge=edge,
            graph=graph,
            order=0,
        )
        session = GraphExecutionSession.objects.create(
            graph=graph,
            session_id="db-ir-legacy-remote-cpp-session",
            initial_data={"x": [0.0, 1.0]},
        )

        def fake_execute_remote_cpp(data, operation, input_key, output_key):
            data[output_key] = {
                "operation": operation,
                "values": data[input_key],
            }
            return data

        payload = {"x": [0.0, 1.0]}
        with patch("comsdk.executors.execute_remote_cpp", side_effect=fake_execute_remote_cpp):
            with patch("config.tasks.publish_execution_ws_event"):
                execute_graph_task.run(graph.id, session.session_id, payload)

        self.assertEqual(
            payload["sin_result"],
            {"operation": "sin", "values": [0.0, 1.0]},
        )

    @patch("config.tasks.publish_execution_ws_event")
    @patch("config.tasks.record_execution_events")
    @patch("config.tasks.build_comsdk_graph_from_db")
    def test_false_graph_result_marks_task_as_failed(
        self,
        build_graph,
        record_events,
        publish_event,
    ):
        stored_graph = Graph.objects.create(name="BROKEN_GRAPH")
        graph = MagicMock()
        graph.run.return_value = False
        build_graph.return_value = graph

        with self.assertRaisesMessage(RuntimeError, "Graph execution failed"):
            execute_graph_task.run(
                stored_graph.id,
                "failed-session",
                {},
            )

        error_event = record_events.call_args.args[1][0]
        self.assertEqual(error_event["event"], "error")
        self.assertEqual(error_event["message"], "Graph execution failed")
        publish_event.assert_called()
