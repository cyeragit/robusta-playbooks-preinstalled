from robusta.api import action, ActionParams, EventChangeEvent, MarkdownBlock, PrometheusKubernetesAlert, JobChangeEvent
from hikaru.model.rel_1_26.v1 import Pod, Job, CronJob
from collections import defaultdict
from typing import Dict, Any
from string import Template
import logging


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class PodLabelTemplate(ActionParams):
    template: str


def get_cluster_name(event: EventChangeEvent):
    for sink in event.all_sinks.values():
        cluster_name = sink.registry.get_global_config().get("cluster_name")
        if cluster_name:
            return cluster_name
    return None


@action
def event_pod_label_enricher(event: EventChangeEvent, params: PodLabelTemplate):
    logger.info(f"Enriching event with pod labels -> {event.obj.regarding.kind} - {event.obj.regarding.name} - {event.obj.regarding.namespace}")

    relevant_event_obj = None

    if event.obj.regarding.kind == "Pod":
        relevant_event_obj = Pod.readNamespacedPod(name=event.obj.regarding.name, namespace=event.obj.regarding.namespace).obj
    elif event.obj.regarding.kind == "CronJob":
        relevant_event_obj = CronJob.readNamespacedCronJob(name=event.obj.regarding.name, namespace=event.obj.regarding.namespace).obj
    elif event.obj.regarding.kind == "Job":
        relevant_event_obj = Job.readNamespacedJob(name=event.obj.regarding.name, namespace=event.obj.regarding.namespace).obj

    if not relevant_event_obj:
        logger.info("Pod not found, skipping")
        return

    logger.info(f"Pod found, enriching with labels -> {relevant_event_obj.metadata.labels}")

    labels: Dict[str, Any] = defaultdict(lambda: "<missing>")
    labels.update(relevant_event_obj.metadata.labels)
    labels.update(relevant_event_obj.metadata.annotations)
    if event.obj.regarding.kind == "CronJob":
        logger.info(f"Enriching cronjob labels -> {relevant_event_obj.spec.jobTemplate.spec.template.metadata.labels}")
        labels.update(relevant_event_obj.spec.jobTemplate.spec.template.metadata.labels)
    labels["name"] = relevant_event_obj.metadata.name
    labels["namespace"] = relevant_event_obj.metadata.namespace
    template = Template(params.template)

    cluster_name = get_cluster_name(event)

    for sink in event.named_sinks:
        for finding in event.sink_findings[sink]:
            finding.subject.labels.update(labels)
            if cluster_name:
                labels["cluster"] = cluster_name

    event.add_enrichment(
        [MarkdownBlock(template.safe_substitute(labels))],
    )


@action
def alert_job_labels_enricher(event: JobChangeEvent):
    logger.info(f"Enriching JobChangeEvent event with job labels")
    logger.info(f"EVENT OBJECT -> {event.obj}")
    logger.info(f"EVENT OBJECT METADATA -> {event.obj.metadata}")
    try:
        logger.info(f"AS DICT -> {event.__dict__}")
    except Exception as e:
        logger.error(f"ERROR AS DICT -> {e}")
    event_labels = event.obj.metadata.labels.update(event.obj.metadata.labels)

    logger.info(f"Enriching alert with job labels -> {event_labels}")

    if event_labels:
        labels: Dict[str, Any] = defaultdict(lambda: "<missing>")
        labels.update(event_labels)

        for sink in event.named_sinks:
            for finding in event.sink_findings[sink]:
                finding.subject.labels.update(labels)
