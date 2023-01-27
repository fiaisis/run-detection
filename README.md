# run-detection

![License: GPL-3.0](https://img.shields.io/github/license/InteractiveReduction/run-detection)
![Build: passing](https://img.shields.io/github/actions/workflow/status/interactivereduction/run-detection/tests.yml?branch=main)
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
     "skipTitlesIncluding": ["foo", "bar", "baz"] 
    }
    ```
2. Create the `Rule` implementation:
    ```python
    class SkipTitlesIncludingRule(Rule[List[str]]):
  
      def verify(self, metadata: NexusMetadata):
          return any(word in metadata.experiment_title for word in self._value)
    ```
3. Update the `RuleFactory`:
    ```python
    def rule_factory(key: str, value: T) -> Rule[T]:
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
