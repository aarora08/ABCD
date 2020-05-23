
gcloud auth login
gcloud config set project $DEVSHELL_PROJECT_ID
gcloud iam service-accounts create runner
gcloud projects add-iam-policy-binding $DEVSHELL_PROJECT_ID --member "serviceAccount:runner@$DEVSHELL_PROJECT_ID.iam.gserviceaccount.com" --role "roles/dataflow.worker"
gcloud projects add-iam-policy-binding $DEVSHELL_PROJECT_ID --member "serviceAccount:runner@$DEVSHELL_PROJECT_ID.iam.gserviceaccount.com" --role "roles/pubsub.subscriber"
gcloud projects add-iam-policy-binding $DEVSHELL_PROJECT_ID --member "serviceAccount:runner@$DEVSHELL_PROJECT_ID.iam.gserviceaccount.com" --role "roles/bigquery.user"
gcloud iam service-accounts keys create cicd/gcp-key.json --iam-account runner@$DEVSHELL_PROJECT_ID.iam.gserviceaccount.com
gcloud services enable bigquery.googleapis.com dataflow.googleapis.com pubsub.googleapis.com