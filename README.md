# run-detection

## Running and Testing

To run:

- `pip install .`
- `run-detection`

To demo and test.
The easiest way to test the whole run detection currently:

- `docker run -p 61613:61613 -p 61616:61616 -p 8161:8161 rmohr/activemq`
- Login to the webgui at http://localhost:8161
- Create a new queue called "Interactive-Reduction"
- Send messages via the webgui to that queue
- Verify they are printed.

## How to container

- The containers are stored in
  the [container registry for the organisation on github](https://github.com/orgs/interactivereduction/packages).
- Have docker installed to build the container
- Construct the container by running:

```shell
docker build . -f ./container/rundetection.D -t ghcr.io/interactivereduction/rundetection -t ghcr.io/interactivereduction/rundetection:on-k8s
```

- Run the container by running:

```shell
docker run -it --rm --mount source=/archive,target=/archive --name rundetection ghcr.io/interactivereduction/rundetection
```

- To push containers you will need to setup the correct access for it, you can follow
  this [guide](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-to-the-container-registry).
- Upload the container by running (should be handled by CI, but this can be done manually if needed):

```shell
docker push ghcr.io/interactivereduction/rundetection -a
```

- To pull containers you will also need the permissions set above in
  the [guide](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-to-the-container-registry).
- Pull the container by running:

```shell
docker pull ghcr.io/interactivereduction/rundetection:latest
```

## Running tests

To run the unit tests only run: `pytest . --ignore test/test_e2e.py`

To run the e2e tests:

```shell
cd test 
docker-compose up -d
cd ..
pytest test/test_e2e.py
```

This will pull the kafka/activemq containers and build the run detection container.
Any code changes made after starting run detection will require the run detection container to be rebuilt.