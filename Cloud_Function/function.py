import functions_framework
import json
import os
from googleapiclient.discovery import build

@functions_framework.cloud_event
def trigger_df_job(cloud_event):
    try:
        data = cloud_event.data
        bucket = data["bucket"]
        name = data["name"]
        print(f"Triggered by file: gs://{bucket}/{name}")

        service = build("dataflow", "v1b3")
        project = os.environ.get("GCP_PROJECT", "centered-sol-469812-v8")
        region = "us-central1"

        template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

        body = {
            "jobName": f"bq-load-{name.replace('.', '-')}-{timestamp}",
            "parameters": {
                "javascriptTextTransformGcsPath": "gs://new-bucket-89/udf.js",
                "JSONPath": "gs://new-bucket-89/bq.json",
                "javascriptTextTransformFunctionName": "transform",
                "outputTable": f"{project}:insight_ds.icc_odi_batsmen_ranking",
                "inputFilePattern": f"gs://{bucket}/{name}",
                "bigQueryLoadingTemporaryDirectory": "gs://new-bucket-89/temp/"
            },
            "environment": {
                "tempLocation": "gs://new-bucket-89/temp/"
            }
        }

        request = service.projects().locations().templates().launch(
            projectId=project,
            location=region,
            gcsPath=template_path,
            body=body,
        )

        response = request.execute()
        print("Dataflow launch response:", json.dumps(response, indent=2))

    except Exception as e:
        import traceback
        print("ERROR:", str(e))
        traceback.print_exc()
