## Steps

1. Copy `airtravel.csv` file to any S3 bucket
2. Update `template.yaml` S3 bucket on line 28 to match yours
3. Build and deploy it: `sam build --use-container`, `sam deploy --guided`
