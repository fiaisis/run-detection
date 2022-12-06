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


