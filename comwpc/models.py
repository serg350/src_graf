from django.db import models

class Graph(models.Model):
    name = models.CharField(max_length=255, unique=True)
    created_at = models.DateTimeField(auto_now_add=True)
    parent_graph = models.ForeignKey('self', null=True, blank=True, on_delete=models.SET_NULL, related_name='subgraphs')
    is_subgraph = models.BooleanField(default=False)
    raw_dot = models.TextField(blank=True, null=True)

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