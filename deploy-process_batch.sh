#!/bin/bash

gcloud --project=thromer-example-001 functions deploy process_batch --runtime=python311 --gen2 --region=us-west1 --source=scripts/faux_event --entry-point=process_batch --no-allow-unauthenticated --timeout=1800 --trigger-http
