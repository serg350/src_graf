def dummy_edge(data):
    """Пустое ребро, не изменяет данные"""
    pass

def increment_a_edge(data):
    data['a'] += 1

def increment_a_double(data):
    data['a'] *= 2

def increment_a_array_edge(data):
    for i in range(len(data['a'])):
        data['a'][i] += 1

def increment_b_edge(data):
    data['b'] += 1

def decrement_a_edge(data):
    data['a'] -= 1

def nonzero_predicate(data):
    """Предикат: возвращает True если 'a' не равно 0"""
    return data['a'] != 0

def positiveness_predicate(data):
    return data['a'] > 0 

def nonpositiveness_predicate(data):
    return data['a'] <= 0 

def copy_to_c(data):
    data['c'] = data['a']

def selector_a_nonpositive(data):
    res = data['a'] <= 0
    return [res, not res]

def selector_a_positive(data):
    res = data['a'] > 0
    print(f"Selector check: a={data['a']}, continue={res}")
    return [res, not res]

def true_predicate(data):
    return True

def process_a(data):
    """Обработка ветки A: умножает a на 2"""
    if 'a' not in data:
        data['a'] = 0  # Инициализация по умолчанию
    data['a'] *= 2
    data['processed_by'] = 'A'

def process_b(data):
    """Обработка ветки B: устанавливает b в 10 (вместо добавления)"""
    data['b'] += 10
    data['processed_by'] = 'B'

def check_condition(data):
    """Предикат для выбора ветки (True - ветка A, False - ветка B)"""
    return data.get('value', 0) % 2 == 0  # Безопасное получение value

def branch_selector(data):
    """Селектор ветвления на основе check_condition"""
    condition = check_condition(data)
    return [condition, not condition]

def init_data(data):
    data['initialized'] = True
    data['processed'] = False
    data['value'] = data.get('input', 0)

def validate_data(data):
    if 'value' not in data:
        raise ValueError("Data not initialized")
    data['valid'] = True

def process_data(data):
    if not data.get('valid', False):
        raise ValueError("Invalid data")
    data['processed'] = True
    data['value'] *= 2

def save_result(data):
    data['saved'] = True
    data['result'] = data['value']

def cleanup(data):
    data['clean'] = True

def branch_selector(data):
    """Селектор ветвления для теста атрибутов"""
    return [True, False]

class ThreadParallelizationPolicy:
    """Тестовая политика параллелизма"""
    pass


