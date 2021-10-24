start-spark:
	docker-compose up -d
run:
	docker exec spark-master sh -c "cd code/ ; sh build_dependencies.sh  ; /spark/bin/spark-submit  --master spark://spark-master:7077  --deploy-mode client --py-files packages.zip  ${path_run_with_args}"

test-all:
	docker exec spark-master sh -c "cd code/ ; sh setup.sh ; pipenv run python -m unittest discover -p '*_test.py'"

