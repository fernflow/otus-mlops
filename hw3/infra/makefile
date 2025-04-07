init:
	terraform init -upgrade

plan:
	terraform plan

apply:
	terraform apply -auto-approve

destroy:
	terraform destroy -auto-approve

data_cp:
	s3cmd sync --acl-public s3://otus-mlops-source-data/ s3://hw3-data-bucket/

del:
	rm -r -f .terraform .terraform.lock.hcl terraform.tfstate terraform.tfstate.backup