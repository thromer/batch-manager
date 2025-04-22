import farmhash
import math

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, TypedDict, cast

import functions_framework

from cloudevents.http import CloudEvent
from google.cloud import tasks
from google.cloud.tasks import CloudTasksClient
from google.cloud.firestore import Client as FirestoreClient
from google.cloud.firestore import Transaction, transactional
from google.events.cloud.firestore import DocumentEventData
from google.protobuf import timestamp_pb2


PROJECT_ID = "thromer-example-001"
LOCATION = "us-west1"
QUEUE_NAME = "batch-processing"
PROCESSING_FUNCTION_URL = f"https://{LOCATION}-{PROJECT_ID}.cloudfunctions.net/process_batch"
BATCH_CONTROL_DOC_PATH = "system/batch_control"
DELAY=timedelta(seconds=60)  # TODO 90 would be prudent

# gcloud --project=thromer-example-001 tasks queues create batch-processing --location=us-west1 --log-sampling-ratio=1.0 --max-attempts=5 --min-backoff=1s --max-concurrent-dispatches=1 --max-dispatches-per-second=0.05

class BatchControl(TypedDict):
    update_time: float
    task_name: str

class DocHandler:
    def __init__(self, db: FirestoreClient, tasks_client: CloudTasksClient):
        self.db = db
        self.tasks_client = tasks_client
        self.parent = self.tasks_client.queue_path(PROJECT_ID, LOCATION, QUEUE_NAME)

    def document_created(self) -> None:
        """
        Cloud Function triggered when a document is created in collections C or D.
        Manages batch processing by updating the control document and scheduling/rescheduling tasks.
        """
        db = FirestoreClient()
        now = datetime.now(timezone.utc)
        now_s = now.timestamp()
        new_task_name = self.make_task_name(now)
        batch_doc_ref = db.document(BATCH_CONTROL_DOC_PATH)
    
        @transactional
        def update_batch_control(transaction: Transaction) -> tuple[bool, BatchControl]:
            """returns (newer, old_data); newer means delete the old task and schedule new one."""
            batch_doc = batch_doc_ref.get(transaction=transaction)
            old_data = cast(BatchControl,
                            (batch_doc.to_dict()
                             if batch_doc.exists
                             else {"update_time": -math.inf, "task_name": ""}))
            if now_s < old_data["update_time"]:
                return (False, old_data)
            new_data: BatchControl = {
                "update_time": now_s,
                "task_name": new_task_name
            }
            transaction.set(batch_doc_ref, cast(dict[str, Any], new_data))
            return (True, old_data)
    
        (newer, old_data) = update_batch_control(db.transaction())
        if newer:
            self.schedule_processing_task(old_data["task_name"], new_task_name, now)

    def make_task_name(self, now: datetime) -> str:
        t = now.strftime("%Y-%m-%dT%H%M%SZ")
        f = f"{farmhash.fingerprint64(t):x}"
        task_id = f"{f}-batch-processing-{t}"
        return f"{self.parent}/tasks/{task_id}"
    
    def schedule_processing_task(self, old_task_name: str, new_task_name: str, now: datetime) -> None:
        if old_task_name:
            try:
                self.tasks_client.delete_task(name=old_task_name)
                print(f"Deleted task {old_task_name}")
            except Exception as e:
                print(f"Error deleting task {old_task_name}: {e}")
        timestamp = timestamp_pb2.Timestamp()
        timestamp.FromDatetime(now+DELAY)
        task = tasks.Task(
            name=new_task_name,
            http_request=tasks.HttpRequest(
                http_method=tasks.HttpMethod.POST,
                url=PROCESSING_FUNCTION_URL,
                headers={"Content-Type": "application/json"}
            ),
            schedule_time=timestamp,
            dispatch_deadline=timedelta(seconds=1800)
        )
        response = self.tasks_client.create_task(parent=self.parent, task=task)
        print(f"Created new task: {response.name}")


@functions_framework.cloud_event
def on_document_created(event: CloudEvent) -> None:
    firestore_payload = DocumentEventData()
    firestore_payload._pb.ParseFromString(
        event.data
    )
    path_components = firestore_payload.value.name.split("/")
    if len(path_components) < 6:
        print("Skipping update, db not found in arg")
        return
    db_name = path_components[5]
    if db_name not in {"faux-events", ""}:
        print(f"Skipping update to db {db_name}")
        return
    DocHandler(FirestoreClient(project=PROJECT_ID),
               CloudTasksClient()).document_created()

    
def process_batch(_: Any) -> Dict[str, Any]:
    print("TODO process_batch")
    return {"status": "success"}


if __name__ == "__main__":
    DocHandler(FirestoreClient(project=PROJECT_ID),
               CloudTasksClient()).document_created()
