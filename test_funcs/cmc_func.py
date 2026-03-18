def request_target_property(data):
    """Проверка входных параметров, переданных из UI/aINI."""
    required_fields = ("E_target", "max_iterations", "tolerance", "E_fiber", "E_matrix")
    missing_fields = [name for name in required_fields if name not in data]
    if missing_fields:
        raise ValueError(f"Missing required input fields: {', '.join(missing_fields)}")


def make_initial_guess(data):
    """Сделать начальное предположение для объемной доли"""
    if 'iteration' not in data:
        data['iteration'] = 0
        data['Vf_min'] = 0.0
        data['Vf_max'] = 1.0
        data['Vf'] = 0.5  # Начальное предположение
    else:
        data['iteration'] += 1


def calculate_property(data):
    """Расчет текущего свойства композита"""
    Vf = data['Vf']
    E_fiber = data['E_fiber']
    E_matrix = data['E_matrix']

    # Правило смесей для модуля Юнга
    data['E_current'] = Vf * E_fiber + (1 - Vf) * E_matrix


def check_convergence(data):
    """Проверка сходимости расчета"""
    error = abs(data['E_current'] - data['E_target'])
    data['converged'] = (error <= data['tolerance'])


def is_converged(data):
    """Предикат: достигнута ли сходимость"""
    return data.get('converged', False)


def is_not_converged(data):
    """Предикат: не достигнута ли сходимость"""
    return not data.get('converged', False)


def adjust_parameter(data):
    """Корректировка параметра (объемной доли)"""
    # Метод половинного деления для подбора Vf
    if data['E_current'] < data['E_target']:
        data['Vf_min'] = data['Vf']  # Текущее значение слишком мало
    else:
        data['Vf_max'] = data['Vf']  # Текущее значение слишком велико

    # Новое предположение - середина текущего интервала
    data['Vf'] = (data['Vf_min'] + data['Vf_max']) / 2.0


def has_more_iterations(data):
    """Предикат: есть ли еще итерации"""
    return data['iteration'] < data['max_iterations']


def no_more_iterations(data):
    """Предикат: больше нет итераций"""
    return data['iteration'] >= data['max_iterations']


def save_optimal_result(data):
    """Сохранение оптимального результата"""
    data['optimal_Vf'] = data['Vf']
    data['optimal_E'] = data['E_current']


def show_optimal_result(data):
    """Отображение оптимального результата"""
    print(f"Найдено оптимальное решение за {data['iteration']} итераций:")
    print(f"Объемная доля волокна: {data['optimal_Vf']:.4f}")
    print(f"Модуль Юнга композита: {data['optimal_E']:.2f} ГПа")
    print(f"Целевое значение: {data['E_target']} ГПа")


def handle_no_convergence(data):
    """Обработка ситуации, когда сходимость не достигнута"""
    print(f"Не удалось достичь сходимости за {data['iteration']} итераций")
    print(f"Лучшее достигнутое значение: {data['E_current']:.2f} ГПа")
    print(f"При объемной доле: {data['Vf']:.4f}")

def true_predicate(data):
    """Всегда возвращает True (используется для безусловных переходов)"""
    return True
