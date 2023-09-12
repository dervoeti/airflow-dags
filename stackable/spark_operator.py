from typing import TYPE_CHECKING, Optional, Sequence
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.models import BaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context

class SparkKubernetesOperator(BaseOperator):
    """
    Creates a SparkApplication resource in kubernetes:
    :param application_file: Defines a 'SparkApplication' custom resource as either a
        path to a '.yaml' file, '.json' file, YAML string or JSON string.
    :param namespace: kubernetes namespace for the SparkApplication
    :param kubernetes_conn_id: The :ref:`kubernetes connection id <howto/connection:kubernetes>`
        for the Kubernetes cluster.
    :param api_group: SparkApplication API group
    :param api_version: SparkApplication API version
    """

    template_fields: Sequence[str] = ('application_file', 'namespace')
    template_ext: Sequence[str] = ('.yaml', '.yml', '.json')
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        application_file: str,
        namespace: Optional[str] = None,
        kubernetes_conn_id: str = 'kubernetes_in_cluster',
        api_group: str = 'spark.stackable.tech',
        api_version: str = 'v1alpha1',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.application_file = application_file
        self.namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.api_group = api_group
        self.api_version = api_version
        self.plural = "sparkapplications"

    def execute(self, context: 'Context'):
        hook = KubernetesHook(conn_id=self.kubernetes_conn_id)
        self.log.info("Creating SparkApplication...")
        response = hook.create_custom_object(
            group=self.api_group,
            version=self.api_version,
            plural=self.plural,
            body=self.application_file,
            namespace=self.namespace,
        )
        return response

