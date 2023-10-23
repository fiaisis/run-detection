# run-detection

![License: GPL-3.0](https://img.shields.io/github/license/InteractiveReduction/run-detection)
![Build: passing](https://img.shields.io/github/actions/workflow/status/interactivereduction/run-detection/tests.yml?branch=main)
[![codecov](https://codecov.io/gh/interactivereduction/run-detection/branch/main/graph/badge.svg?token=9YZ619JJ0N)](https://codecov.io/gh/interactivereduction/run-detection)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![linting: pylint](https://img.shields.io/badge/linting-pylint-yellowgreen)](https://github.com/PyCQA/pylint)

## Running and Testing

To run:

- `pip install .`
- `run-detection`

To install when developing:

- `pip install .[dev]`  
  *Note:* you may need to escape the square brackets. This will also install pytest, pylint, mypy etc.

To demo and test.
The easiest way to test the whole run detection currently:

- Start the docker-compose setup in the test directory
- `sudo docker compose up -d`
- Visit the rabbitmq web ui
- Submit messages to the ingress station, verify run-detection logs, check the egress station

## Configuration

Run detection has 5 environment variables it will check

1. `QUEUE_HOST` - host name of the queue server
2. `QUEUE_USER` - Username of the application user run detection should use when connecting to queue
3. `QUEUE_PASSWORD` - Password of the above user
4. `INGRESS_QUEUE_NAME` - queue name that run detection will consume from
5. `EGRESS_QUEUE_NAME` - queue name that run detection will produce to

If these are not provided, run detection will choose default station names, "watched-files", "scheduled-jobs".
localhost will be used as the default host, and the default credentials, guest guest, will be used.

## How to container

- The containers are stored in
  the [container registry for the organisation on github](https://github.com/orgs/interactivereduction/packages).
- Have docker installed to build the container
- Construct the container by running:

```shell
docker build . -f ./container/rundetection.D -t ghcr.io/interactivereduction/rundetection
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

## Adding additional nexus extraction rules

In certain cases, specific instruments may include additional metadata that can be used as run inputs. As these metadata
are instrument-specific, they will not apply to all Nexus files. To accommodate these additional metadata, you can
create custom extraction functions for the ingestion process (e.g., mari_extract).

Adding Custom Nexus Extraction Rules

In certain cases, specific instruments may include additional metadata that can be used as run inputs. As these metadata
are instrument-specific, they will not apply to all Nexus files. To accommodate these additional metadata, you can
create custom extraction functions for the ingestion process (e.g., mari_extract).

To add a custom extraction function, follow these steps:

1.

```python
def my_instrument_extract(job_request: JobRequest, dataset: Any) -> JobRequest
    """
    Extracts additional metadata specific to my instrument from the given dataset and updates the JobRequest
    instance. If the metadata does not exist, the default values will be set instead.

    :param job_request: JobRequest instance for which to extract additional metadata
    :param dataset: The dataset from which to extract additional MARI-specific metadata.
    :return: JobRequest instance with updated additional metadata
    """
    job_request.additional_values["some_key"] = dataset.get("some_key")
    return job_request
```

Where the extraction function has the type `Callable[[JobRequest, Any] JobRequest]`
While Any is listed, it is actually a h5py group, but the library does not have any type stubs.

Next update the extraction factory function:

2.

```python
def get_extraction_function(instrument: str) -> Callable[[JobRequest, Any], JobRequest]:
    """
    Given an instrument name, return the additional metadata extraction function for the instrument
    :param instrument: str - instrument name
    :return: Callable[[JobRequest, Any], JobRequest]: The additional metadata extraction function for the instrument
    """
    match instrument.lower():
        case "mari":
            return mari_extract
        case "my_instrument":
            return my_instrument_extract
        case _:
            return skip_extract


```

After making these two changes, when a run for your instrument is detected, the new extraction function will be
automatically called, and the JobRequest.additional_values will be updated accordingly.

## Adding to Instrument Specifications

For a run to be sent downstream the metadata of the recieved file must meet the specification for that instrument.
The specifications for each instrument are found in `rundetection/specifications/<instrument>_specification.json`

An example specification file:

```json
{
  "enabled": true
}
```

Within the json file each field is considered to be a `Rule` and has a class associated with it. e.g. the `EnabledRule`
class.

### Example of Adding a new Rule

Below is an example of adding a new rule. The example is unrealistic, but it shows how much flexibility there is.

1. Update the specification file:
    ```json
    {
     "enabled": true,
     "skipTitlesIncluding": ["25581", "bar", "baz"] 
    }
    ```
2. Create the `Rule` implementation:
    ```python
    class SkipTitlesIncludingRule(Rule[List[str]]):
  
      def verify(self, job_request: JobRequest) -> None:
          job_request.will_reduce =  any(word in run.experiment_title for word in self._value)
    ```
3. Update the `RuleFactory`:
    ```python
    def rule_factory(key: str, value: T_co) -> Rule[T_co]:
        """
        Given the rule key, and rule value, return the rule implementation
        :param key: The key of the rule
        :param value: The value of the rule
        :return: The Rule implementation
        """
        match key.lower():
            case "enabled":
                if isinstance(value, bool):
                    return EnabledRule(value)
                else:
                    raise ValueError(f"Bad value: {value} in rule: {key}")
            case "skiptitlesincluding":
                if isinstance(value, list):
                    return SkipTitlesIncludingRule(value)
                else:
                    raise ValueError(f"Bad value: {value} in rule: {key}")
            case _:
                raise MissingRuleError(f"Implementation of Rule: {key} does not exist.")

    ```
