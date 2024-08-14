from robusta.api import action, ActionParams, RobustaJob, EventChangeEvent, MarkdownBlock, JobChangeEvent, JobStatus, TableBlock, RobustaPod
from hikaru.model.rel_1_26.v1 import Pod, Job, CronJob
from typing import Dict, Any, List, Tuple
from collections import defaultdict
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
    job_labels_keys_to_enrich = ["job-name", "team"]
    logger.info(f"Enriching JobChangeEvent event with job labels")

    job: RobustaJob = event.get_job()
    if not job:
        logging.error(f"Cannot run alert_job_labels_enricher on event with no job: {event}")
        return

    job_status: JobStatus = job.status
    status, message = __job_status_str(job_status)
    job_rows: List[List[str]] = [["status", status]]
    if message:
        job_rows.append(["message", message])

    try:
        job_labels = [[key, value] for key, value in job.metadata.labels.items() if key in job_labels_keys_to_enrich]
        image = job.get_single_pod().get_images()
    except Exception as e:
        logging.error(f"Error getting job labels -> {e}")
        job_labels = []
        image = None

    job_rows.append(["name", job.metadata.name])
    job_rows.append(["namespace", job.metadata.namespace])
    if image:
        job_rows.append(["image", str(image)])

    job_rows.extend(job_labels)

    table_block = TableBlock(
        job_rows,
        ["description", "value"],
        table_name="*Job information*",
    )
    event.add_enrichment([table_block])


def __job_status_str(job_status: JobStatus) -> Tuple[str, str]:
    if job_status.active:
        return "Running", ""

    for condition in job_status.conditions:
        if condition.status == "True":
            return condition.type, condition.message

    if not any([job_status.active, job_status.failed, job_status.succeeded, job_status.conditions]):
        return "Starting", ""

    return "Unknown", ""