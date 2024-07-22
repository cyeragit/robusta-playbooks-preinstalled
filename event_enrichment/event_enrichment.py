import logging

from jinja2 import Template
from typing import Dict, Any
from collections import defaultdict
from hikaru.model.rel_1_26.v1 import Pod
from robusta.api import action, ActionParams, EventChangeEvent, MarkdownBlock


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class PodLabelTemplate(ActionParams):
    template: str


@action
def event_pod_label_enricher(event: EventChangeEvent, params: PodLabelTemplate):
    logger.info(f"Enriching event with pod labels")

    if not event.obj.regarding.kind == "Pod" or not event.obj.regarding.kind == "CronJob":
        logger.info("Event is not regarding a pod / cronjob, skipping")
        return

    pod = Pod.readNamespacedPod(name=event.obj.regarding.name, namespace=event.obj.regarding.namespace).obj
    if not pod:
        logger.info("Pod not found, skipping")
        return

    labels: Dict[str, Any] = defaultdict(lambda: "<missing>")
    labels.update(pod.metadata.labels)
    labels.update(pod.metadata.annotations)
    labels["name"] = pod.metadata.name
    labels["namespace"] = pod.metadata.namespace
    template = Template(params.template)

    event.add_enrichment(
        [MarkdownBlock(template.safe_substitute(labels))],
    )