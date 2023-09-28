RESOURCE_FILES = values.yaml

delete:
	kontemplate delete $(RESOURCE_FILES)

apply:
	kontemplate apply $(RESOURCE_FILES)

hard-restart: delete apply

restart:
	kubectl rollout restart deployment arak-indexer
