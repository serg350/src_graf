from django.db import models


class Graph(models.Model):
    name = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)
    parent_graph = models.ForeignKey('self', null=True, blank=True, on_delete=models.SET_NULL, related_name='subgraphs')
    is_subgraph = models.BooleanField(default=False)
    raw_dot = models.TextField(blank=True, null=True)
    raw_aini = models.TextField(blank=True, null=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['name', 'is_subgraph'],
                name='unique_graph_name_per_type'
            )
        ]

    def __str__(self):
        return self.name

class State(models.Model):
    name = models.CharField(max_length=255)
    is_terminal = models.BooleanField(default=False)
    graph = models.ForeignKey(Graph, on_delete=models.CASCADE)
    subgraph = models.ForeignKey(Graph, null=True, blank=True, on_delete=models.SET_NULL, related_name='parent_states')
    comment = models.TextField(blank=True)
    array_keys_mapping = models.JSONField(blank=True, null=True)
    is_subgraph_node = models.BooleanField(default=False)

    def __str__(self):
        return f"{self.name} ({self.graph})"

class Edge(models.Model):
    comment = models.CharField(max_length=255)
    pred_module = models.CharField(max_length=255)
    pred_func = models.CharField(max_length=255)
    morph_module = models.CharField(max_length=255)
    morph_func = models.CharField(max_length=255)

    def __str__(self):
        return self.comment


class Transfer(models.Model):
    source = models.ForeignKey(State, on_delete=models.CASCADE, related_name='outgoing')
    edge = models.ForeignKey(Edge, on_delete=models.CASCADE)
    target = models.ForeignKey(State, on_delete=models.CASCADE, related_name='incoming')
    order = models.IntegerField(default=0)
    graph = models.ForeignKey(Graph, on_delete=models.CASCADE)  # Добавляем прямой ForeignKey

    class Meta:
        ordering = ['order']


class GraphExecutionSession(models.Model):
    STATUS_PENDING = "pending"
    STATUS_RUNNING = "running"
    STATUS_COMPLETED = "completed"
    STATUS_FAILED = "failed"
    STATUS_CHOICES = [
        (STATUS_PENDING, "Pending"),
        (STATUS_RUNNING, "Running"),
        (STATUS_COMPLETED, "Completed"),
        (STATUS_FAILED, "Failed"),
    ]

    graph = models.ForeignKey(
        Graph,
        on_delete=models.CASCADE,
        related_name="execution_sessions",
    )
    session_id = models.CharField(max_length=64, unique=True)
    status = models.CharField(
        max_length=16,
        choices=STATUS_CHOICES,
        default=STATUS_PENDING,
    )
    initial_data = models.JSONField(blank=True, default=dict)
    last_state = models.CharField(max_length=255, blank=True, default="")
    error_message = models.TextField(blank=True, default="")
    event_count = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.graph.name} [{self.session_id}]"


class GraphExecutionEvent(models.Model):
    session = models.ForeignKey(
        GraphExecutionSession,
        on_delete=models.CASCADE,
        related_name="events",
    )
    sequence = models.PositiveIntegerField()
    event_type = models.CharField(max_length=64)
    state = models.CharField(max_length=255, blank=True, default="")
    message = models.TextField(blank=True, default="")
    payload = models.JSONField(blank=True, default=dict)
    raw_event = models.JSONField(blank=True, default=dict)
    occurred_at = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["sequence"]
        constraints = [
            models.UniqueConstraint(
                fields=["session", "sequence"],
                name="unique_execution_event_sequence",
            )
        ]

    def __str__(self):
        return f"{self.session.session_id}:{self.sequence}:{self.event_type}"
