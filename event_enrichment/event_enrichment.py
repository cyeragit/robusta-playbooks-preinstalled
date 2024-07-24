import logging

from string import Template
from typing import Dict, Any
from collections import defaultdict
from hikaru.model.rel_1_26.v1 import Pod, Job, CronJob
from robusta.api import action, ActionParams, EventChangeEvent, MarkdownBlock


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class PodLabelTemplate(ActionParams):
    template: str


@action
def event_pod_label_enricher(event: EventChangeEvent, params: PodLabelTemplate):
    logger.info(f"Enriching event with pod labels -> {event.obj.regarding.kind} - {event.obj.regarding.name} - {event.obj.regarding.namespace}")

    relevant_event_obj = None

    if event.obj.regarding.kind == "Pod":
        relevant_event_obj = Pod.readNamespacedPod(name=event.obj.regarding.name, namespace=event.obj.regarding.namespace).obj
    elif event.obj.regarding.kind == "CronJob":
        relevant_event_obj = CronJob.readNamespacedCronJob(name=event.obj.regarding.name, namespace=event.obj.regarding.namespace).obj
        try:
            logger.info('================================ CronJob ==================================')
            logger.info(relevant_event_obj)
            logger.info('==================================================================')
            logger.info('================================ CronJob.spec ==================================')
            logger.info(relevant_event_obj.spec)
            logger.info('==================================================================')
            logger.info('================================ CronJob.spec.jobTemplate ==================================')
            logger.info(relevant_event_obj.spec.jobTemplate)
            logger.info('==================================================================')
            logger.info('================================ CronJob.spec.jobTemplate.spec ==================================')
            logger.info(relevant_event_obj.spec.jobTemplate.spec)
            logger.info('==================================================================')
            logger.info('================================ CronJob.spec.jobTemplate.spec.template ==================================')
            logger.info(relevant_event_obj.spec.jobTemplate.spec.template)
            logger.info('==================================================================')
            logger.info('================================ CronJob.spec.jobTemplate.spec.template.metadata ===============================')
            logger.info(relevant_event_obj.spec.jobTemplate.spec.template.metadata)
            logger.info('==================================================================')
            logger.info('=========================== CronJob.spec.jobTemplate.spec.template.metadata.labels =============================')
            logger.info(relevant_event_obj.spec.jobTemplate.spec.template.metadata.labels)
            logger.info('==================================================================')
        except Exception as e:
            logger.error(e)
    elif event.obj.regarding.kind == "Job":
        relevant_event_obj = Job.readNamespacedJob(name=event.obj.regarding.name, namespace=event.obj.regarding.namespace).obj

    if not relevant_event_obj:
        logger.info("Pod not found, skipping")
        return

    logger.info(f"Pod found, enriching with labels -> {relevant_event_obj.metadata.labels}")

    labels: Dict[str, Any] = defaultdict(lambda: "<missing>")
    labels.update(relevant_event_obj.metadata.labels)
    labels.update(relevant_event_obj.metadata.annotations)
    labels["name"] = relevant_event_obj.metadata.name
    labels["namespace"] = relevant_event_obj.metadata.namespace
    template = Template(params.template)

    for sink in event.named_sinks:
        for finding in event.sink_findings[sink]:
            finding.subject.labels.update(labels)

    event.add_enrichment(
        [MarkdownBlock(template.safe_substitute(labels))],
    )