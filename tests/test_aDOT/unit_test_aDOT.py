import unittest
import os
from comsdk.parser import Parser
from comsdk.graph import Graph


class TestADOTParser(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_files_dir = os.path.join(os.path.dirname(__file__), "test_adot_files")

    def setUp(self):
        self.parser = Parser()

    def test_sequential_graph(self):
        """Тест последовательного графа"""
        graph = self.parser.parse_file(os.path.join(self.test_files_dir, "sequential.adot"))
        self.assertIsInstance(graph, Graph)

        # Проверка структуры
        init_state = graph.init_state
        self.assertEqual(len(init_state.transfers), 1)

        # Проверка выполнения
        data = {"a": 1}
        result = graph.run(data)
        self.assertTrue(result)
        self.assertEqual(data["a"], 4)  # 1 -> (inc) 2 -> (double) 4

    def test_cycled_graph(self):
        """Тест циклического графа с уменьшением a до 0"""
        graph = self.parser.parse_file(os.path.join(self.test_files_dir, "cycled.adot"))
        self.assertIsInstance(graph, Graph)

        # Проверка выполнения
        data = {"a": 5}  # Начинаем с 5
        result = graph.run(data)
        self.assertTrue(result)
        self.assertEqual(data["a"], 0,
                         "Граф должен уменьшать a до 0")

        # Дополнительная проверка с другим начальным значением
        data = {"a": 10}
        result = graph.run(data)
        self.assertTrue(result)
        self.assertEqual(data["a"], 0)

    def test_branching_graph(self):
        """Тест графа с ветвлениями"""
        graph = self.parser.parse_file(os.path.join(self.test_files_dir, "branching.adot"))
        self.assertIsInstance(graph, Graph)

        data = {"a": 5, "b": 5}
        result = graph.run(data)
        self.assertTrue(result)
        self.assertEqual(data["a"], 10)  # 5 * 2 = 10
        self.assertEqual(data["b"], 15)

        data = {"a": 1, "b": 3}
        result = graph.run(data)
        self.assertTrue(result)
        self.assertEqual(data["b"], 13)
        self.assertEqual(data["a"], 2)

    def test_subgraph_integration(self):
        """Тест интеграции подграфов"""
        graph = self.parser.parse_file(os.path.join(self.test_files_dir, "main_graph.adot"))
        self.assertIsInstance(graph, Graph)

        data = {'input': 5}
        result = graph.run(data)
        print(data)
        self.assertTrue(result)
        self.assertEqual(data['value'], 10)  # 5 * 2 = 10
        self.assertTrue(data['initialized'])
        self.assertTrue(data['valid'])
        self.assertTrue(data['processed'])
        self.assertTrue(data['saved'])
        self.assertTrue(data['clean'])
        self.assertEqual(data['result'], 10)


    def test_edge_types(self):
        """Тест разных типов ребер (-> и =>)"""
        graph = self.parser.parse_file(os.path.join(self.test_files_dir, "edge_types.adot"))
        self.assertIsInstance(graph, Graph)

        # Проверка структуры графа
        start_node = graph.init_state.transfers[0].output_state
        self.assertEqual(len(start_node.transfers), 2, "Должно быть 2 исходящих ребра")

        # Проверка типов ребер
        edge_simple = start_node.transfers[0].edge
        edge_parallel = start_node.transfers[1].edge

        self.assertEqual(edge_simple.order, 0, "Обычное ребро должно иметь order=0")
        self.assertEqual(edge_parallel.order, 1, "Параллельное ребро должно иметь order=1")

    def test_attributes_parsing(self):
        """Проверка парсинга атрибутов узлов и ребер"""
        graph = self.parser.parse_file(os.path.join(self.test_files_dir, "attributes.adot"))

        # Проверка атрибутов состояния NODE1
        node1 = graph.init_state.transfers[0].output_state
        self.assertEqual(node1.selector.name, "branch_selector")
        #self.assertEqual(node1.parallelization_policy.__class__.__name__, "ThreadParallelizationPolicy")
        #self.assertEqual(node1.comment, "Первый узел с атрибутами")

        # Проверка атрибутов состояния NODE2
        node2 = node1.transfers[0].output_state
        #self.assertEqual(node2.comment, "Второй узел с подграфом")
        self.assertTrue(hasattr(node2, '_proxy_state'), "Должен содержать подграф")

        # Проверка атрибутов ребра EDGE1
        edge1 = graph.init_state.transfers[0].edge
        self.assertEqual(edge1.pred_f.name, "nonzero_predicate")
        self.assertEqual(edge1.morph_f.name, "increment_a_edge")
        self.assertEqual(edge1.comment, "Первое ребро с полным набором атрибутов")
        #self.assertEqual(edge1.order, 1)  # Проверка edge_index

        # Проверка атрибутов ребра EDGE2
        edge2 = node1.transfers[0].edge
        #self.assertEqual(len(edge2.morph_fs), 2)  # Проверка morphisms
        #self.assertEqual(edge2.morph_fs[0].name, "increment_a_edge")
        #self.assertEqual(edge2.morph_fs[1].name, "increment_b_edge")

        # Проверка загрузки подграфа
        subgraph = node2._proxy_state
        #self.assertIsInstance(subgraph, Graph)
        #self.assertEqual(subgraph.init_state.name, "__BEGIN__")


class TestGraphExecution(unittest.TestCase):
    def test_parallel_execution(self):
        """Тест параллельного выполнения"""
        # Здесь будут тесты реального параллельного выполнения
        # с использованием threading/multiprocessing
        pass


if __name__ == '__main__':
    unittest.main()