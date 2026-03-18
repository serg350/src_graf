import json
from unittest.mock import patch

from django.test import RequestFactory, TestCase

from .aini.aini_parser import build_initial_data, parse_aini
from .models import Graph
from .views import _build_execution_input_schema, start_execution


RAW_AINI = """
[Input]
*TaskName=ElasticResearch
-Pressure=34 [[MPa]]
OutputFilename=@TaskName@_@Pressure@.res
CopyObjectToRep=[1]{0|1}
Range=[0.4;0.1:0.6;0.05]
Mode=[auNO]{auNO|auLCS|auSegmented}
LocalAxes=((1;0;0);(0;1;0);(0;0;1))
GeoFile=[geometry.geo]
"""


class AINIParserTests(TestCase):
    def test_parse_aini_supported_types(self):
        parsed = parse_aini(RAW_AINI)
        self.assertEqual(parsed["sections"][0]["name"], "Input")
        params = {param["name"]: param for param in parsed["parameters"]}

        self.assertEqual(params["TaskName"]["value_type"], "text")
        self.assertEqual(params["Pressure"]["value_type"], "dim")
        self.assertEqual(params["Pressure"]["value"]["value"], 34)
        self.assertEqual(params["Mode"]["value_type"], "combobox")
        self.assertEqual(params["Range"]["value_type"], "interval")
        self.assertEqual(params["GeoFile"]["value_type"], "file_ref")

    def test_build_initial_data_resolves_templates(self):
        data = build_initial_data(RAW_AINI)
        self.assertEqual(data["TaskName"], "ElasticResearch")
        self.assertEqual(data["Pressure"], 34)
        self.assertEqual(data["OutputFilename"], "ElasticResearch_34.res")
        self.assertTrue(data["CopyObjectToRep"])

    def test_build_execution_input_schema_marks_initial_values(self):
        schema = _build_execution_input_schema(RAW_AINI)
        fields = {field["name"]: field for field in schema["fields"]}

        self.assertEqual(fields["TaskName"]["initial_value"], "ElasticResearch")
        self.assertTrue(fields["TaskName"]["has_initial_value"])
        self.assertEqual(fields["Pressure"]["initial_value"], 34)
        self.assertEqual(fields["Pressure"]["initial_value_label"], "34 [MPa]")
        self.assertTrue(fields["CopyObjectToRep"]["initial_value"])
        self.assertGreaterEqual(schema["prefilled_count"], 3)


class StartExecutionAINITests(TestCase):
    def setUp(self):
        self.factory = RequestFactory()

    @patch("comwpc.views.execute_graph_task.delay")
    def test_start_execution_requires_required_aini_fields(self, delay_mock):
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
        self.assertEqual(response.status_code, 400)
        delay_mock.assert_not_called()

    @patch("comwpc.views.execute_graph_task.delay")
    def test_start_execution_uses_only_user_provided_aini_values(self, delay_mock):
        graph = Graph.objects.create(
            name="test_graph_with_aini_values",
            raw_dot="digraph Test { __BEGIN__ -> __END__ }",
            raw_aini=RAW_AINI,
        )

        request_payload = {
            "data": {
                "TaskName": "CustomTask",
                "Pressure": 55,
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

        payload = delay_mock.call_args.args[2]
        self.assertEqual(payload["TaskName"], "CustomTask")
        self.assertEqual(payload["Pressure"], 55)
        self.assertNotIn("OutputFilename", payload)
        self.assertNotIn("a", payload)
