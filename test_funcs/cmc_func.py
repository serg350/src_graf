import json
import math
import os
import random


PSO_UNEVALUATED_SCORE = 1.0e300


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


def _pso_float(data, name, default):
    value = data.get(name, default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _pso_int(data, name, default):
    value = data.get(name, default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _pso_bounds(data):
    vf_min = _pso_float(data, "vf_min", 0.05)
    vf_max = _pso_float(data, "vf_max", 0.75)
    theta_min = _pso_float(data, "theta_min", 0.0)
    theta_max = _pso_float(data, "theta_max", 90.0)
    return [(vf_min, vf_max), (theta_min, theta_max)]


def _pso_clamp(value, lower, upper):
    return max(lower, min(upper, value))


def _pso_composite_properties(position, data):
    vf = position[0]
    theta_deg = position[1]
    e_fiber = _pso_float(data, "E_fiber", 230.0)
    e_matrix = _pso_float(data, "E_matrix", 3.5)
    rho_fiber = _pso_float(data, "rho_fiber", 1.8)
    rho_matrix = _pso_float(data, "rho_matrix", 1.2)

    axial_modulus = vf * e_fiber + (1.0 - vf) * e_matrix
    transverse_modulus = 1.0 / (vf / e_fiber + (1.0 - vf) / e_matrix)
    orientation_factor = math.cos(math.radians(theta_deg)) ** 4
    effective_modulus = (
        orientation_factor * axial_modulus
        + (1.0 - orientation_factor) * transverse_modulus
    )
    density = vf * rho_fiber + (1.0 - vf) * rho_matrix

    return {
        "Vf": vf,
        "theta_deg": theta_deg,
        "E_effective": effective_modulus,
        "density": density,
        "axial_modulus": axial_modulus,
        "transverse_modulus": transverse_modulus,
        "orientation_factor": orientation_factor,
    }


def _pso_score(properties, data):
    e_target = _pso_float(data, "E_target", 120.0)
    modulus_error = abs(properties["E_effective"] - e_target)
    density_target = data.get("density_target")
    density_weight = _pso_float(data, "density_weight", 0.0)

    density_penalty = 0.0
    if density_target not in (None, ""):
        density_penalty = density_weight * abs(
            properties["density"] - float(density_target)
        )

    return modulus_error + density_penalty


def _pso_initial_particle(rng, bounds):
    return [rng.uniform(lower, upper) for lower, upper in bounds]


def _pso_initial_velocity(rng, bounds):
    velocity = []
    for lower, upper in bounds:
        span = upper - lower
        velocity.append(rng.uniform(-0.1 * span, 0.1 * span))
    return velocity


def _pso_reset_shards(data):
    for shard_index in range(4):
        data.pop(f"pso_shard_{shard_index}", None)


def pso_initialize(data):
    required_fields = ("E_target", "E_fiber", "E_matrix")
    missing_fields = [name for name in required_fields if name not in data]
    if missing_fields:
        raise ValueError(f"Missing required input fields: {', '.join(missing_fields)}")

    swarm_size = max(4, _pso_int(data, "swarm_size", 32))
    data["swarm_size"] = swarm_size
    data["max_iterations"] = max(1, _pso_int(data, "max_iterations", 30))
    data["tolerance"] = _pso_float(data, "tolerance", 0.5)
    data["iteration"] = 0
    data["pso_converged"] = False

    rng = random.Random(_pso_int(data, "random_seed", 7))
    bounds = _pso_bounds(data)
    positions = [_pso_initial_particle(rng, bounds) for _ in range(swarm_size)]
    velocities = [_pso_initial_velocity(rng, bounds) for _ in range(swarm_size)]

    data["pso_positions"] = positions
    data["pso_velocities"] = velocities
    data["pso_personal_best_positions"] = [position[:] for position in positions]
    data["pso_personal_best_scores"] = [
        PSO_UNEVALUATED_SCORE for _ in range(swarm_size)
    ]
    data["pso_global_best_position"] = positions[0][:]
    data["pso_global_best_score"] = PSO_UNEVALUATED_SCORE
    data["pso_history"] = []
    _pso_reset_shards(data)

def pso_prepare_shard_requests(data):
    for shard_index in range(4):
        particles = []
        for particle_index in range(shard_index, len(data["pso_positions"]), 4):
            particles.append({
                "particle_index": particle_index,
                "position": data["pso_positions"][particle_index],
            })

        data[f"pso_shard_request_{shard_index}"] = {
            "shard_index": shard_index,
            "shard_count": 4,
            "particles": particles,
            "params": {
                "E_target": data["E_target"],
                "E_fiber": data["E_fiber"],
                "E_matrix": data["E_matrix"],
                "rho_fiber": data.get("rho_fiber", 1.8),
                "rho_matrix": data.get("rho_matrix", 1.2),
                "density_target": data.get("density_target"),
                "density_weight": data.get("density_weight", 0.0),
                "matrix_size": data.get("matrix_size", 256),
                "matrix_work_repeats": data.get("matrix_work_repeats", 1),
                "matrix_correction_weight": data.get("matrix_correction_weight", 0.0),
            },
        }


def _pso_evaluate_shard(data, shard_index, shard_count=4):
    positions = data["pso_positions"]
    shard_results = []

    for particle_index in range(shard_index, len(positions), shard_count):
        properties = _pso_composite_properties(positions[particle_index], data)
        score = _pso_score(properties, data)
        shard_results.append(
            {
                "particle_index": particle_index,
                "score": score,
                "properties": properties,
            }
        )

    data[f"pso_shard_{shard_index}"] = shard_results


def pso_evaluate_shard_0(data):
    _pso_evaluate_shard(data, 0)


def pso_evaluate_shard_1(data):
    _pso_evaluate_shard(data, 1)


def pso_evaluate_shard_2(data):
    _pso_evaluate_shard(data, 2)


def pso_evaluate_shard_3(data):
    _pso_evaluate_shard(data, 3)

def _pso_iter_shard_results(data, shard_count=4):
    data["pso_shard_metrics"] = {}

    for shard_index in range(shard_count):
        shard_payload = data.get(f"pso_shard_{shard_index}", [])

        if isinstance(shard_payload, dict):
            if "error" in shard_payload:
                raise RuntimeError(
                    f"PSO shard {shard_index} failed: {shard_payload['error']}"
                )

            data["pso_shard_metrics"][str(shard_index)] = {
                "worker_id": shard_payload.get("worker_id"),
                "operation": shard_payload.get("operation"),
                "elapsed_ms": shard_payload.get("elapsed_ms"),
                "shard_index": shard_payload.get("shard_index", shard_index),
                "result_count": len(shard_payload.get("results", [])),
            }

            results = shard_payload.get("results", [])
        else:
            results = shard_payload or []

        for result in results:
            yield result


def pso_update_best(data):
    positions = data["pso_positions"]
    personal_best_positions = data["pso_personal_best_positions"]
    personal_best_scores = data["pso_personal_best_scores"]
    global_best_position = data["pso_global_best_position"]
    global_best_score = data["pso_global_best_score"]
    global_best_properties = data.get("pso_global_best_properties")

    for result in _pso_iter_shard_results(data, shard_count=4):
        particle_index = int(result["particle_index"])
        score = float(result["score"])
        properties = result.get("properties")

        if score < personal_best_scores[particle_index]:
            personal_best_scores[particle_index] = score
            personal_best_positions[particle_index] = positions[particle_index][:]

        if score < global_best_score:
            global_best_score = score
            global_best_position = positions[particle_index][:]
            global_best_properties = properties

    if global_best_properties is None:
        global_best_properties = _pso_composite_properties(global_best_position, data)

    e_target = _pso_float(data, "E_target", 120.0)
    modulus_error = abs(global_best_properties["E_effective"] - e_target)

    data["pso_global_best_position"] = global_best_position
    data["pso_global_best_score"] = global_best_score
    data["pso_global_best_properties"] = global_best_properties
    data["best_Vf"] = global_best_properties["Vf"]
    data["best_theta_deg"] = global_best_properties["theta_deg"]
    data["best_E_effective"] = global_best_properties["E_effective"]
    data["best_density"] = global_best_properties["density"]
    data["best_modulus_error"] = modulus_error
    data["pso_converged"] = modulus_error <= data["tolerance"]
    data["pso_history"].append(
        {
            "iteration": data["iteration"],
            "score": global_best_score,
            "modulus_error": modulus_error,
            "Vf": global_best_properties["Vf"],
            "theta_deg": global_best_properties["theta_deg"],
            "E_effective": global_best_properties["E_effective"],
            "density": global_best_properties["density"],
            "matrix_benchmark": global_best_properties.get("matrix_benchmark"),
        }
    )


def pso_stop_selector(data):
    iteration = data.get("iteration", 0)
    max_iterations = data.get("max_iterations", 1)
    should_stop = data.get("pso_converged", False) or iteration >= max_iterations - 1
    return [not should_stop, should_stop]


def pso_advance_swarm(data):
    iteration = data.get("iteration", 0) + 1
    data["iteration"] = iteration

    bounds = _pso_bounds(data)
    inertia = _pso_float(data, "inertia", 0.65)
    cognitive = _pso_float(data, "cognitive", 1.4)
    social = _pso_float(data, "social", 1.4)
    seed = _pso_int(data, "random_seed", 7)
    global_best = data["pso_global_best_position"]

    for particle_index, position in enumerate(data["pso_positions"]):
        velocity = data["pso_velocities"][particle_index]
        personal_best = data["pso_personal_best_positions"][particle_index]

        for dim, (lower, upper) in enumerate(bounds):
            rng = random.Random(seed + iteration * 100003 + particle_index * 97 + dim)
            span = upper - lower
            velocity_cap = 0.25 * span
            new_velocity = (
                inertia * velocity[dim]
                + cognitive * rng.random() * (personal_best[dim] - position[dim])
                + social * rng.random() * (global_best[dim] - position[dim])
            )
            velocity[dim] = _pso_clamp(new_velocity, -velocity_cap, velocity_cap)
            position[dim] = _pso_clamp(position[dim] + velocity[dim], lower, upper)

    _pso_reset_shards(data)


def pso_save_result(data):
    result = {
        "TaskName": data.get("TaskName", "composite_pso_demo"),
        "iterations": data.get("iteration", 0) + 1,
        "converged": data.get("pso_converged", False),
        "target_E": _pso_float(data, "E_target", 120.0),
        "best_Vf": data.get("best_Vf"),
        "best_theta_deg": data.get("best_theta_deg"),
        "best_E_effective": data.get("best_E_effective"),
        "best_density": data.get("best_density"),
        "best_modulus_error": data.get("best_modulus_error"),
        "best_score": data.get("pso_global_best_score"),
        "best_matrix_benchmark": (
            data.get("pso_global_best_properties") or {}
        ).get("matrix_benchmark"),
        "shard_metrics": data.get("pso_shard_metrics", {}),
        "history": data.get("pso_history", []),
    }

    output_filename = data.get("output_filename")
    if output_filename:
        output_path = os.path.join(
            data.get("__WORKING_DIR__", os.getcwd()),
            str(output_filename),
        )
        with open(output_path, "w", encoding="utf-8") as result_file:
            json.dump(result, result_file, ensure_ascii=False, indent=2)
        result["output_path"] = output_path

    data["pso_result"] = result
    print("COMPOSITE_PSO result:", result)
