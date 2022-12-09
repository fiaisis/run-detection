# run-detection

## Running and Testing

To run:

- `pip install .`
- `python main.py`

To demo and test.
The easiest way to test the whole run detection currently:

- `docker run -p 61613:61613 -p 61616:61616 -p 8161:8161 rmohr/activemq`
- Login to the webgui at http://localhost:8161
- Create a new queue called "Interactive-Reduction"
- Send messages via the webgui to that queue
- Verify they are printed.

## How to container

- The containers are stored in the [container registry for the organisation on github](https://github.com/orgs/interactivereduction/packages).
- Have docker installed to build the container
- Construct the container by running:

```shell
docker build . -f ./container/rundetection.D -t ghcr.io/interactivereduction/rundetection
```

- Run the container by running:

```shell
docker run -it --rm --name rundetection ghcr.io/interactivereduction/rundetection
```

- To push containers you will need to setup the correct access for it, you can follow this [guide](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-to-the-container-registry).
- Upload the container by running (should be handled by CI, but this can be done manually if needed):

```shell
docker push ghcr.io/interactivereduction/rundetection
```

- To pull containers you will also need the permissions set above in the [guide](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-to-the-container-registry).
- Pull the container by running:

```shell
docker pull ghcr.io/interactivereduction/rundetection:latest
```
