test_k8s_platform_api:
	make test_unit
	#mv .coverage .coverage.unit
	#make test_integration
	#mv .coverage .coverage.integration
	#coverage combine
	#codecov

