from robusta.api import action, ActionParams, RobustaJob, EventChangeEvent, MarkdownBlock, JobChangeEvent, JobStatus, TableBlock, PodEvent, RobustaPod
from hikaru.model.rel_1_26.v1 import Pod, Job, CronJob
from typing import Dict, Any, List, Tuple, Union
from collections import defaultdict
import logging


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@action
def event_change_event_enricher(event: EventChangeEvent):
    logger.info(f"Enriching EventChangeEvent with object labels")

    logger.info(event)

    __enrich_event_with_cluster_name(event)
    if not event.obj:
        logger.info("Job related object not found, skipping")
        return

    alert_table_block_rows = __create_alert_table_block_rows(event)
    logger.info(alert_table_block_rows)
    alert_summary_table = __create_alert_table_block("Alert Summary", alert_table_block_rows)
    logger.info(alert_summary_table)

    event.add_enrichment([alert_summary_table])
    # relevant_event_obj = None
    #
    # if event.obj.regarding.kind == "Pod":
    #     relevant_event_obj = Pod.readNamespacedPod(name=event.obj.regarding.name, namespace=event.obj.regarding.namespace).obj
    # elif event.obj.regarding.kind == "CronJob":
    #     relevant_event_obj = CronJob.readNamespacedCronJob(name=event.obj.regarding.name, namespace=event.obj.regarding.namespace).obj
    # elif event.obj.regarding.kind == "Job":
    #     relevant_event_obj = Job.readNamespacedJob(name=event.obj.regarding.name, namespace=event.obj.regarding.namespace).obj
    #
    # if not relevant_event_obj:
    #     logger.info("Pod not found, skipping")
    #     return
    #
    # logger.info(f"Pod found, enriching with labels -> {relevant_event_obj.metadata.labels}")
    #
    # labels: Dict[str, Any] = defaultdict(lambda: "<missing>")
    # labels.update(relevant_event_obj.metadata.labels)
    # labels.update(relevant_event_obj.metadata.annotations)
    # if event.obj.regarding.kind == "CronJob":
    #     logger.info(f"Enriching cronjob labels -> {relevant_event_obj.spec.jobTemplate.spec.template.metadata.labels}")
    #     labels.update(relevant_event_obj.spec.jobTemplate.spec.template.metadata.labels)
    # labels["name"] = relevant_event_obj.metadata.name
    # labels["namespace"] = relevant_event_obj.metadata.namespace
    #
    # __enrich_event_with_cluster_name(event)
    #
    # for sink in event.named_sinks:
    #     for finding in event.sink_findings[sink]:
    #         finding.subject.labels.update(labels)


@action
def job_change_event_enricher(event: JobChangeEvent):
    logger.info(f"Enriching JobChangeEvent with object labels")

    logger.info(event)

    __enrich_event_with_cluster_name(event)
    if not event.obj:
        logger.info("Job related object not found, skipping")
        return

    alert_table_block_rows = __create_alert_table_block_rows(event)
    logger.info(alert_table_block_rows)
    alert_summary_table = __create_alert_table_block("Alert Summary", alert_table_block_rows)
    logger.info(alert_summary_table)

    event.add_enrichment([alert_summary_table])


@action
def pod_event_enricher(event: PodEvent):
    logger.info(f"Enriching PodEvent with object labels")

    __enrich_event_with_cluster_name(event)
    if not event.obj:
        logger.info("Pod related object not found, skipping")
        return

    alert_table_block_rows = __create_alert_table_block_rows(event)
    alert_summary_table = __create_alert_table_block("Alert Summary", alert_table_block_rows)

    event.add_enrichment([alert_summary_table])


def __get_cluster_name(event: Union[EventChangeEvent, JobChangeEvent, PodEvent]) -> Union[str, None]:
    for sink in event.all_sinks.values():
        cluster_name = sink.registry.get_global_config().get("cluster_name")
        if cluster_name:
            return cluster_name
    return None


def __enrich_event_with_cluster_name(event: Union[EventChangeEvent, JobChangeEvent, PodEvent]):
    cluster_name = __get_cluster_name(event)
    if cluster_name:
        for sink in event.named_sinks:
            for finding in event.sink_findings[sink]:
                finding.subject.labels.update({"cluster": cluster_name})


def __create_alert_table_block_rows(event: Union[EventChangeEvent, JobChangeEvent, PodEvent]) -> List[List[str]]:
    job_rows: List[List[str]] = [
        ["Name", event.obj.metadata.name],
        ["Namespace", event.obj.metadata.namespace]]

    team = event.obj.metadata.labels.get("team")
    if team:
        job_rows.append(["Team", team])

    return job_rows


def __create_alert_table_block(table_name: str, table_rows: List[List[str]]) -> TableBlock:
    return TableBlock(
        table_name=table_name,
        headers=["Description", "Value"],
        rows=table_rows,
    )
